import asyncio
import shlex
import shutil
import sqlite3
import stat
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

import pytest

from live_alert.cli import main, read_status, snapshot
from live_alert.config import Config
from live_alert.service import Service, run
from live_alert.storage import connect, single_instance

ROOT = Path(__file__).resolve().parents[1]


def test_compose_defaults_and_cli_example():
    env = dict(
        shlex.split(line)[0].split("=", 1)
        for line in (ROOT / ".env.example").read_text().splitlines()
        if line.strip() and not line.startswith("#")
    )
    c = Config.load(env)
    assert c.notification_mode == "dry-run" and not c.smtp_password
    # The template ships without any room or mailbox; run requires a room.
    assert not c.rooms and not c.smtp_from_address and not c.smtp_to_address
    with pytest.raises(ValueError, match="DOUYU_ROOMS"):
        c.room_configs()
    rooms = Config.load(
        {**env, "DOUYU_ROOMS": " 10001, 10002=测试主播 "}
    ).room_configs()
    assert [(r.room_id, r.room_name) for r in rooms] == [
        (10001, ""),
        (10002, "测试主播"),
    ]
    compose = (ROOT / "compose.yaml").read_text()
    assert "ports:" not in compose and "network_mode:" not in compose
    assert "USER 10001:10001" in (ROOT / "Dockerfile").read_text()


@pytest.mark.parametrize(
    "env",
    [
        {"NOTIFICATION_MODE": "send"},
        {"OFFLINE_CONFIRM_SAMPLES": "2"},
        {"NOTIFY_OFFLINE": "yes"},
        {"SMTP_PORT": "not-a-number"},
        {"SMTP_SECURITY": "ssl"},
        {"SMTP_TO_ADDRESS": "bad"},
        {"DOUYU_ROOMID": "1"},
        {"DOUYU_ROOM_ID": "10001"},
        {"DOUYU_ROOMS": "abc"},
        {"DOUYU_ROOMS": "1_0001"},
        {"DOUYU_ROOMS": "+10001"},
        {"DOUYU_ROOMS": "１０００１"},
        {"DOUYU_ROOMS": "0"},
        {"DOUYU_ROOMS": "10001,10001=重复"},
        {"DOUYU_ROOMS": ",".join(str(10001 + i) for i in range(21))},
        {"NOTIFICATION_MODE": "send", "SMTP_USERNAME": "u", "SMTP_PASSWORD": "p"},
        {"POLL_INTERVAL_SECONDS": "10"},
        {"SMTP_FROM_NAME": "x\nfoo"},
        {"INITIAL_BASELINE_SILENT": "false"},
    ],
)
def test_configuration_rejects_invalid(env):
    with pytest.raises(ValueError):
        Config.load(env)


def test_second_instance_and_database_deduplication(scenario):
    s = scenario
    with single_instance(s.c.db), pytest.raises(RuntimeError), single_instance(s.c.db):
        pytest.fail("second instance acquired lock")
    s.baseline()
    s.live()
    with (
        closing(connect(s.c.db)) as other,
        pytest.raises(sqlite3.IntegrityError),
        other,
    ):
        other.execute("""INSERT INTO outbox(session_id,kind,recipient,state,reason,subject,body,created,next_try)
                      SELECT session_id,kind,recipient,state,reason,subject,body,created,next_try FROM outbox""")
    assert len(s.events()) == 1


def test_startup_secures_existing_database_and_wal_files(
    scenario, tmp_path, monkeypatch
):
    s = scenario
    s.baseline()
    s.live()
    target = tmp_path / "upgrade"
    target.mkdir()
    # Copy the quiescent WAL database so the service is its only opener.
    files = [target / (s.c.db.name + suffix) for suffix in ("", "-wal", "-shm")]
    for path in files:
        shutil.copyfile(s.c.db.parent / path.name, path)
        path.chmod(0o644)
    c = Config.load({"DATA_DIR": str(target), "DOUYU_ROOMS": str(s.c.room_id)})

    async def inspect(service):
        assert [stat.S_IMODE(path.stat().st_mode) for path in files] == [0o600] * 3
        assert service.db.execute("SELECT count(*) FROM sessions").fetchone()[0] == 1

    monkeypatch.setattr(Service, "run", inspect)
    asyncio.run(run(c))
    assert stat.S_IMODE(c.db.stat().st_mode) == 0o600


def test_backup_and_restore_preserve_terminals_and_quarantine_pending(
    scenario, tmp_path
):
    s = scenario
    s.baseline()
    s.live()
    s.worker()
    s.offline()
    dest = tmp_path / "snapshot.sqlite"
    snapshot(s.c.db, dest)
    other = tmp_path / "restored"
    with patch.dict("os.environ", {"DATA_DIR": str(other)}, clear=True):
        assert main(["restore", str(dest)]) == 0
        assert main(["restore", str(dest)]) != 0
    with closing(connect(other / "live_alert.db")) as restored:
        assert [
            r[0] for r in restored.execute("SELECT state FROM outbox ORDER BY id")
        ] == ["SENT", "UNCERTAIN"]
    with pytest.raises(FileExistsError):
        snapshot(s.c.db, dest)


@pytest.mark.parametrize(
    "source_kind", ["zero_bytes", "unrelated", "missing_table", "missing_column"]
)
def test_restore_rejects_invalid_snapshot_before_publishing(
    scenario, tmp_path, monkeypatch, source_kind
):
    archive = tmp_path / "candidate.sqlite"
    target = tmp_path / "restored"
    monkeypatch.setenv("DATA_DIR", str(target))
    if source_kind == "zero_bytes":
        archive.touch()
    elif source_kind == "unrelated":
        with closing(sqlite3.connect(archive)) as db:
            db.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY)")
    else:
        snapshot(scenario.c.db, archive)
        with closing(sqlite3.connect(archive)) as db:
            if source_kind == "missing_table":
                db.execute("DROP TABLE delivery_attempts")
            else:
                db.execute("ALTER TABLE sessions DROP COLUMN title")
    original = archive.read_bytes()
    assert main(["restore", str(archive)]) != 0
    assert archive.read_bytes() == original
    assert not (target / "live_alert.db").exists()
    assert not list(target.glob(".snapshot-*"))
    # Retrying with a genuine snapshot in the same directory must still work.
    assert main(["restore", str(scenario.c.db)]) == 0


def test_cli_check_once_readonly_and_health(scenario, tmp_path):
    s = scenario
    with (
        patch.dict(
            "os.environ",
            {"DATA_DIR": str(tmp_path / "absent"), "DOUYU_ROOMS": "10001"},
            clear=True,
        ),
        patch(
            "aiodouyu.web._http_get_json",
            return_value={"room": {"room_id": 10001, "show_status": 2, "videoLoop": 0}},
        ),
    ):
        assert main(["check-once"]) == 0
        assert not (tmp_path / "absent").exists()
    service = Service(s.c, s.db, None)
    service.tick()
    assert read_status([s.c], True) == 0
    # A configured room the running service has not picked up yet is unhealthy.
    assert read_status([s.c, s.c.for_room(10002)], True) != 0
    with s.db:
        s.db.execute("UPDATE monitor SET heartbeat=0")
    assert read_status([s.c], True) != 0
    assert read_status([s.c], False) == 0


def test_snapshot_before_offline_mail_cannot_duplicate_after_restore(
    scenario, tmp_path
):
    from dataclasses import replace

    from live_alert.state import MonitorState

    s = scenario
    s.baseline()
    s.live()
    s.worker()
    backup = tmp_path / "active.sqlite"
    snapshot(s.c.db, backup)
    s.offline()
    assert s.worker()
    c = replace(s.c, data_dir=str(tmp_path / "restored"))
    c.db.parent.mkdir()
    snapshot(backup, c.db, restore=True)
    with closing(connect(c.db)) as db:
        s.c, s.db, s.state = c, db, MonitorState(db, c, s.t)
        s.offline()
        assert s.events()[-1] == ("OFFLINE", "SUPPRESSED_RESTORE")
        assert not s.worker()
        s.live()
        assert s.worker()
