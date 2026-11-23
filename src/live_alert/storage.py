import fcntl
import json
import logging
import os
import sqlite3
import stat
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path

from .config import Config

SCHEMA = """
CREATE TABLE IF NOT EXISTS monitor (
    room INTEGER PRIMARY KEY, confirmed TEXT NOT NULL DEFAULT 'BASELINING',
    session_id INTEGER, phase TEXT NOT NULL DEFAULT 'BASELINING',
    last_valid REAL, heartbeat REAL, health TEXT DEFAULT 'STARTING',
    stale_restart INTEGER NOT NULL DEFAULT 0, window_first REAL, samples INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS observations (
    id TEXT PRIMARY KEY, room INTEGER NOT NULL, utc REAL NOT NULL, trigger TEXT NOT NULL,
    state TEXT NOT NULL, reason TEXT NOT NULL, fields TEXT NOT NULL, elapsed REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS observed_at ON observations(utc);
CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY, room INTEGER NOT NULL, name TEXT NOT NULL, title TEXT NOT NULL,
    live_at REAL NOT NULL, started_at INTEGER, silent INTEGER NOT NULL,
    first_off REAL, ended REAL, sample_count INTEGER, first_live_id TEXT, last_live_id TEXT,
    first_off_id TEXT, last_off_id TEXT, restored INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY, session_id INTEGER NOT NULL REFERENCES sessions(id),
    kind TEXT NOT NULL, recipient TEXT NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL,
    subject TEXT NOT NULL, body TEXT NOT NULL, created REAL NOT NULL, next_try REAL NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0, sent_at REAL,
    UNIQUE(session_id, kind, recipient)
);
CREATE INDEX IF NOT EXISTS ready ON outbox(state, next_try);
CREATE TABLE IF NOT EXISTS delivery_attempts (
    id INTEGER PRIMARY KEY, outbox_id INTEGER NOT NULL REFERENCES outbox(id),
    started REAL NOT NULL, data_at REAL, finished REAL, state TEXT, reason TEXT
);
CREATE INDEX IF NOT EXISTS attempt_event ON delivery_attempts(outbox_id);

"""


def connect(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    db = sqlite3.connect(
        path.resolve().as_uri() + ("?mode=ro" if readonly else "?mode=rwc"),
        uri=True,
        timeout=5,
    )
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    return db


@contextmanager
def single_instance(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("数据目录已有监控实例运行") from None
        yield


def secure_database_files(path: Path) -> None:
    """Run under the instance lock, before SQLite opens any files or creates a WAL."""
    for suffix in ("", "-wal", "-shm", "-journal"):
        filename = path.with_name(path.name + suffix)
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        try:
            fd = os.open(filename, flags if suffix else flags | os.O_CREAT, 0o600)
        except FileNotFoundError:
            if not suffix:
                raise
            continue
        try:
            mode = os.fstat(fd).st_mode
            if not stat.S_ISREG(mode):
                raise ValueError(f"数据库路径不是普通文件：{filename}")
            private = mode & 0o600
            if stat.S_IMODE(mode) != private:
                os.fchmod(fd, private)
        finally:
            os.close(fd)


def migrate(db: sqlite3.Connection) -> None:
    """Bring any earlier schema, including a restored snapshot, up to date."""
    db.executescript(SCHEMA)
    columns = {r[1] for r in db.execute("PRAGMA table_info(monitor)")}
    for name, ddl in (
        ("mail_ready", "INTEGER NOT NULL DEFAULT 0"),
        ("last_offline", "REAL"),
        ("replay_at", "REAL"),
        ("replay_start", "INTEGER"),
        ("offline_first", "REAL"),
        ("restored_at", "REAL"),
        ("gap_reference", "REAL"),
    ):
        if name not in columns:
            db.execute(f"ALTER TABLE monitor ADD COLUMN {name} {ddl}")
    # started_at follows a resumed broadcast; the original start dates the duration.
    # Legacy sqlite3 transaction control autocommits DDL. Open the transaction
    # explicitly so the column and its backfill commit together: a column left
    # without the backfill would be skipped on every later start.
    with db:
        db.execute("BEGIN IMMEDIATE")
        if "first_started_at" not in {
            r[1] for r in db.execute("PRAGMA table_info(sessions)")
        }:
            db.execute("ALTER TABLE sessions ADD COLUMN first_started_at INTEGER")
            db.execute("UPDATE sessions SET first_started_at=started_at")


def validate_snapshot(db: sqlite3.Connection) -> None:
    """Check the common schema before migration can create missing business tables."""
    tables = {
        r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    with closing(sqlite3.connect(":memory:")) as baseline:
        baseline.executescript(SCHEMA)
        for (name,) in baseline.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ):
            # SCHEMA contains the original columns. Later migrations only add columns.
            info = f"PRAGMA table_info({name})"
            required = {(r[1], r[2], r[5]) for r in baseline.execute(info)}
            actual = (
                {(r[1], r[2], r[5]) for r in db.execute(info)}
                if name in tables
                else set()
            )
            if not required <= actual:
                raise ValueError(f"无效的直播提醒快照：缺少或不兼容的业务表 {name}")
    if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
        raise ValueError("无效的直播提醒快照：存在失效的外键引用")


def initialize(db: sqlite3.Connection, c: Config) -> None:
    db.execute("PRAGMA journal_mode=WAL")
    migrate(db)
    with db:
        db.execute("""UPDATE delivery_attempts SET state='INTERRUPTED',reason='process_restart'
                      WHERE finished IS NULL AND state IS NULL""")
        db.execute("UPDATE monitor SET mail_ready=0 WHERE room=?", (c.room_id,))
        db.execute("INSERT OR IGNORE INTO monitor(room) VALUES (?)", (c.room_id,))
        db.execute(
            "UPDATE outbox SET state='UNCERTAIN', reason='restart_during_data' WHERE state='SUBMITTING'"
        )
        db.execute(
            "UPDATE outbox SET state='RETRYING', reason='restart_before_data' WHERE state='PREPARING'"
        )


def audit(
    c: Config, source: str, before: str, after: str, reason: str, **extra: object
) -> None:
    logging.getLogger(__name__).info(
        json.dumps(
            dict(
                utc=datetime.now(UTC).isoformat(),
                platform="douyu",
                room_id=c.room_id,
                source=source,
                state_before=before,
                state_after=after,
                reason=reason,
                **extra,
            ),
            ensure_ascii=False,
        )
    )
