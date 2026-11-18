import argparse
import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from contextlib import closing
from dataclasses import asdict
from pathlib import Path

from aiodouyu import ApiError

from .config import Config
from .douyu import Douyu
from .http_worker import resolve_room_id
from .mail import message
from .service import run
from .smtp_worker import bounded_deliver as deliver
from .state import local
from .storage import connect, migrate, single_instance, validate_snapshot


def snapshot(source: Path, target: Path, restore: bool = False) -> None:
    if not source.is_file():
        raise ValueError("源数据库不存在")
    # Open exclusively, publish a complete snapshot only after verifying it.
    from tempfile import NamedTemporaryFile

    with NamedTemporaryFile(dir=target.parent, prefix=".snapshot-") as temp:
        with (
            closing(connect(source, readonly=True)) as src,
            closing(sqlite3.connect(temp.name)) as dst,
        ):
            src.backup(dst)
            validate_snapshot(dst)
            if dst.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("SQLite 完整性校验失败")
            dst.execute("PRAGMA journal_mode=DELETE")
            if restore:
                migrate(dst)
                with dst:
                    dst.execute("UPDATE sessions SET restored=1 WHERE ended IS NULL")
                    # Sessions starting before now may have been notified after the
                    # snapshot was taken; see MonitorState._open.
                    dst.execute("UPDATE monitor SET restored_at=?", (time.time(),))
                    dst.execute("""UPDATE outbox SET state='UNCERTAIN',reason='restored_snapshot'
                                WHERE state IN ('PENDING','PREPARING','SUBMITTING','RETRYING',
                                                'WAITING_FOR_LIVE_EMAIL')""")
        os.link(temp.name, target)


def room_status(db: sqlite3.Connection, c: Config) -> dict:
    row = db.execute("SELECT * FROM monitor WHERE room=?", (c.room_id,)).fetchone()
    if row is None:
        raise ValueError(f"找不到房间 {c.room_id} 的监控记录，服务是否已启动？")
    events = []
    # status may open an older database read-only before the service migrates it.
    session_columns = {col[1] for col in db.execute("PRAGMA table_info(sessions)")}
    for kind in ("LIVE", "OFFLINE"):
        start_column = (
            "s.first_started_at"
            if kind == "LIVE" and "first_started_at" in session_columns
            else "s.started_at"
        )
        event = db.execute(
            f"""SELECT q.kind,q.session_id,s.name,q.state,q.reason,q.subject,q.body,
                           q.attempts,q.created,q.sent_at,
                           {start_column} AS platform_started_at,
                           o.utc AS first_http_completed_at, o.utc-o.elapsed AS first_http_requested_at,
                           a.started AS smtp_started_at, a.data_at AS smtp_data_at,
                           q.sent_at-q.created AS confirmation_to_accept_seconds,
                           q.sent_at-o.utc AS first_http_to_accept_seconds,
                           CASE WHEN q.kind='LIVE' THEN q.sent_at-{start_column} END AS platform_start_to_accept_seconds
                           FROM outbox q JOIN sessions s
                           ON s.id=q.session_id
                           LEFT JOIN observations o ON o.id=CASE WHEN q.kind='LIVE' THEN s.first_live_id ELSE s.first_off_id END
                           LEFT JOIN delivery_attempts a ON a.id=(SELECT MAX(id) FROM delivery_attempts WHERE outbox_id=q.id)
                           WHERE s.room=? AND q.kind=? ORDER BY q.id DESC LIMIT 1""",
            (c.room_id, kind),
        ).fetchone()
        if event:
            events.append(dict(event))
    state = dict(row)
    for record in [state, *events]:
        for key in (
            "last_valid",
            "last_offline",
            "replay_at",
            "window_first",
            "heartbeat",
            "created",
            "sent_at",
            "platform_started_at",
            "first_http_completed_at",
            "first_http_requested_at",
            "smtp_started_at",
            "smtp_data_at",
        ):
            if record.get(key) is not None:
                record[key] = local(record[key])
    return {"monitor": state, "latest": events}


def healthy(db: sqlite3.Connection, c: Config) -> str:
    row = db.execute(
        "SELECT health,heartbeat FROM monitor WHERE room=?", (c.room_id,)
    ).fetchone()
    if row is None:
        return "NOT_STARTED"
    ok = (
        row["health"] != "STOPPED"
        and row["heartbeat"]
        and 0 <= time.time() - row["heartbeat"] < 45
    )
    return row["health"] if ok else "STALE_OR_STOPPED"


def read_status(rooms: list[Config], health: bool) -> int:
    with closing(connect(rooms[0].db, readonly=True)) as db:
        if health:
            states = {c.room_id: healthy(db, c) for c in rooms}
            for room_id, state in states.items():
                print(f"{room_id} {state}")
            return int(
                any(s in ("NOT_STARTED", "STALE_OR_STOPPED") for s in states.values())
            )
        report = {str(c.room_id): room_status(db, c) for c in rooms}
        print(json.dumps({"rooms": report}, ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="live-alert")
    subs = parser.add_subparsers(dest="command", required=True)
    for command in ("run", "test-email"):
        subs.add_parser(command)
    for command in ("status", "healthcheck", "check-once"):
        subs.add_parser(command).add_argument(
            "--room", type=int, help="只处理 DOUYU_ROOMS 中的这个房间"
        )
    subs.add_parser("resolve-room").add_argument("room", type=int)
    for command in ("backup", "restore"):
        subs.add_parser(command).add_argument("file", type=Path)
    args = parser.parse_args(argv)
    # The database and backups hold recipients and history: owner-only by default.
    os.umask(0o077)
    try:
        c = Config.load()
        logging.basicConfig(level=c.log_level, format="%(message)s")
        if args.command == "run":
            asyncio.run(run(c))
        elif args.command == "check-once":
            unknown = False
            for rc in c.room_configs(args.room):
                o = asyncio.run(Douyu(rc).sample())
                if o.state == "UNKNOWN":
                    unknown = True
                    print(
                        f"房间 {rc.room_id} 状态未知（{o.reason}）。若持续出现，"
                        "请先用 resolve-room 确认它不是靓号。",
                        file=sys.stderr,
                    )
                print(
                    json.dumps(
                        dict(
                            source="betard",
                            room=rc.room_id,
                            observed_at=local(o.utc),
                            **asdict(o),
                        ),
                        ensure_ascii=False,
                    )
                )
            return int(unknown)
        elif args.command == "resolve-room":
            real = asyncio.run(
                resolve_room_id(args.room, timeout=c.http_timeout_seconds)
            )
            print(real)
            if real != args.room:
                print(
                    f"{args.room} 是靓号，请在 DOUYU_ROOMS 中使用真实房间号 {real}",
                    file=sys.stderr,
                )
        elif args.command == "test-email":
            c.require_smtp()
            msg = message(
                c,
                c.smtp_to_address,
                "[TEST] 斗鱼提醒邮件通道测试",
                "这是一封由部署者主动发起的测试邮件。",
            )
            state, reason = deliver(c, msg, lambda: None)
            print(state, reason)
            return int(state != "SENT")
        elif args.command == "backup":
            snapshot(c.db, args.file)
        elif args.command == "restore":
            with single_instance(c.db):
                if any(c.db.parent.glob(c.db.name + "*")):
                    raise ValueError("请先将旧数据目录移到安全位置，再恢复到空目录")
                snapshot(args.file, c.db, restore=True)
        else:
            return read_status(c.room_configs(args.room), args.command == "healthcheck")
        return 0
    except (ValueError, OSError, RuntimeError, sqlite3.Error, ApiError) as exc:
        print(f"live-alert: {exc}", file=sys.stderr)
        return 2
