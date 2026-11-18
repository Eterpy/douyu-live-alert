import asyncio
import sys
import time

import pytest
from conftest import config

from live_alert.config import Config
from live_alert.http_worker import bounded_fetch
from live_alert.mail import process_one
from live_alert.service import Service


@pytest.mark.parametrize("cancel", [False, True])
def test_stuck_http_process_is_killed_and_reaped(monkeypatch, cancel):
    original = asyncio.create_subprocess_exec
    children = []

    async def spawn(*args, **kwargs):
        child = await original(
            sys.executable, "-c", "import time; time.sleep(60)", **kwargs
        )
        children.append(child)
        return child

    async def exercise():
        monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
        start = time.monotonic()
        task = asyncio.create_task(bounded_fetch(1, source="betard", timeout=0.15))
        if cancel:
            while not children:
                await asyncio.sleep(0.01)
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
            await task
        assert time.monotonic() - start < 3
        assert children[0].returncode is not None

    asyncio.run(exercise())


def start_known(s):
    s.baseline()
    s.feed("LIVE", 30, started_at=1020)
    s.feed("LIVE", 10, started_at=1020)
    assert s.worker()


@pytest.mark.parametrize("restart", [False, True])
def test_new_session_and_final_offline_survive_interruption(scenario, restart):
    s = scenario
    start_known(s)
    if restart:
        s.restart(700)
    else:
        # Without offline evidence a newer start cannot be a resumed broadcast.
        s.feed("UNKNOWN", 5)
    new_start = s.t + 5
    s.feed("LIVE", 10, started_at=new_start)
    assert s.state.session == 1
    second = s.feed("LIVE", 10, started_at=new_start)
    assert s.state.session == 2
    for _ in range(100):
        s.state.observe(second, "duplicate", s.t)
    assert s.worker() and not s.worker()
    for _ in range(120):
        s.feed("LIVE", 60, started_at=new_start)
    s.offline()
    assert s.worker()
    assert s.events() == [
        ("LIVE", "SENT"),
        ("OFFLINE", "SUPPRESSED_SESSION_CHANGE"),
        ("LIVE", "SENT"),
        ("OFFLINE", "SENT"),
    ]


def test_single_changed_start_does_not_rotate(scenario):
    s = scenario
    start_known(s)
    s.feed("LIVE", 10, started_at=s.t + 5)
    s.feed("LIVE", 10, started_at=1020)
    assert s.state.session == 1 and not s.state.stale
    s.offline()
    assert s.worker()


@pytest.mark.parametrize("seconds", [900, 1200])
def test_conflicting_config_rejected(seconds):
    with pytest.raises(ValueError, match="MAX_NOTIFICATION"):
        Config.load({"OFFLINE_CONFIRM_SECONDS": str(seconds)})
    assert Config.load(
        {
            "OFFLINE_CONFIRM_SECONDS": str(seconds),
            "MAX_NOTIFICATION_AGE_SECONDS": str(seconds + 300),
        }
    )


def test_old_open_not_sent_after_close(scenario):
    s = scenario
    s.baseline()
    s.live()
    s.offline()
    assert not s.worker()
    assert s.events() == [("LIVE", "EXPIRED"), ("OFFLINE", "SUPPRESSED_NO_LIVE_NOTICE")]


def test_offline_waits_for_fresh_evidence(scenario):
    s = scenario
    start_known(s)
    s.offline()
    s.t += 31
    assert not s.worker()
    s.feed("OFFLINE")
    assert s.worker()


def test_offline_cancelled_when_live_resumes(scenario):
    s = scenario
    start_known(s)
    s.offline()
    s.feed("LIVE", 5)
    # One live-looking sample only defers the notice ...
    assert not s.worker()
    assert s.events()[-1] == ("OFFLINE", "PENDING")
    # ... a confirmed resumption makes it obsolete.
    s.feed("LIVE", 10)
    assert s.worker()  # The resumed broadcast is a new session with its own notice.
    assert s.events()[-2:] == [("OFFLINE", "EXPIRED"), ("LIVE", "SENT")]


def test_recheck_after_smtp_handshake(scenario):
    s = scenario
    start_known(s)
    s.offline()

    def transport(c, msg, before_data):
        s.feed("LIVE", 5)
        before_data()
        pytest.fail("obsolete mail submitted")

    assert process_one(s.c, transport, lambda: s.t)
    assert s.events()[-1] == ("OFFLINE", "PENDING")  # Deferred, nothing submitted.
    s.feed("LIVE", 10)
    assert s.worker()  # The resumed broadcast is a new session with its own notice.
    assert s.events()[-2:] == [("OFFLINE", "EXPIRED"), ("LIVE", "SENT")]


def test_restart_blocks_queue_until_fresh_http(scenario):
    s = scenario
    start_known(s)
    s.offline()
    s.restart()
    assert not s.worker()
    s.feed("UNKNOWN")
    assert not s.worker()
    s.feed("OFFLINE", 1)
    assert s.worker()


def test_flapping_rss_cannot_starve_http_confirmation(scenario):
    s = scenario
    start_known(s)
    service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
    s.state = service.state
    for _ in range(3):
        service.event("disconnected")
        service.event("connected")
        s.feed("OFFLINE", 60)
    assert s.state.confirmed == "OFFLINE"
    assert s.worker()


def test_sampler_watchdog_raises_to_restart_process(scenario):
    s = scenario
    service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
    s.t += 400
    with pytest.raises(RuntimeError, match="progress"):
        asyncio.run(service.heartbeat())


def test_restart_during_new_candidate_requires_two_new_samples(scenario):
    s = scenario
    start_known(s)
    new_start = s.t + 5
    s.feed("LIVE", 10, started_at=new_start)
    s.restart()
    s.feed("LIVE", 10, started_at=new_start)
    assert s.state.session == 1
    s.feed("LIVE", 10, started_at=new_start)
    assert s.state.session == 2


@pytest.mark.parametrize(
    "kind", ["LIVE", "REPLAY", "http_429", "JSONDecodeError", "ResponseTooLarge"]
)
def test_real_worker_protocol_and_adapter(monkeypatch, kind):
    import os
    from pathlib import Path

    from live_alert.douyu import Douyu

    original = asyncio.create_subprocess_exec
    script = (
        """
import json
from email.message import Message
from io import BytesIO
from unittest.mock import patch
from urllib.error import HTTPError
from live_alert import http_worker
kind = """
        + repr(kind)
        + """
class Reply(BytesIO):
    headers = Message()
    def read(self, n=-1):
        assert 0 < n <= http_worker.MAX_RESPONSE_BYTES + 1
        return super().read(n)

def open_response(*args, **kwargs):
    if kind == 'http_429':
        raise HTTPError('https://example.invalid', 429, 'limited', Message(), None)
    if kind == 'JSONDecodeError':
        return Reply(b'<html>error page</html>')
    if kind == 'ResponseTooLarge':
        http_worker.MAX_RESPONSE_BYTES = 32
        return Reply(b' ' * 33)
    return Reply(json.dumps({'room': {'room_id': 10001, 'show_status': 1,
                         'videoLoop': int(kind == 'REPLAY'), 'show_time': 100}}).encode())

with patch('urllib.request.OpenerDirector.open', open_response):
    http_worker.main()
"""
    )

    async def spawn(*args, **kwargs):
        return await original(
            sys.executable,
            "-c",
            script,
            *args[args.index("live_alert.http_worker") + 1 :],
            env={
                **os.environ,
                "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
            },
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr("live_alert.douyu.fetch_room", bounded_fetch)
    observation = asyncio.run(Douyu(config()).sample())
    if kind in ("LIVE", "REPLAY"):
        assert observation.state == kind
    else:
        # The worker's own failure kind survives the process boundary.
        assert (observation.state, observation.reason) == ("UNKNOWN", kind)


def test_watchdog_exits_service_cleanly(scenario):
    s = scenario

    class Stuck:
        async def events(self):
            await asyncio.Event().wait()
            yield "connected"

        async def sample(self):
            s.t += 400  # The request itself hangs past the sampler limit.
            await asyncio.Event().wait()

    async def exercise():
        service = Service(s.c, s.db, Stuck(), lambda: s.t, lambda: s.t)
        with pytest.raises(RuntimeError, match="progress"):
            await asyncio.wait_for(service.run(), 3)
        assert s.db.execute("SELECT health FROM monitor").fetchone()[0] == "STOPPED"

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "phase,expected", [("PREPARING", "RETRYING"), ("SUBMITTING", "UNCERTAIN")]
)
def test_sigkill_then_restart_recovers_committed_delivery(scenario, phase, expected):
    import subprocess

    from live_alert.storage import initialize

    s = scenario
    s.baseline()
    s.live()
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import os, signal, sqlite3, sys
with sqlite3.connect(sys.argv[1]) as db:
    db.execute('UPDATE outbox SET state=?', (sys.argv[2],))
os.kill(os.getpid(), signal.SIGKILL)
""",
            str(s.c.db),
            phase,
        ],
        check=False,
    )
    assert child.returncode == -9
    initialize(s.db, s.c)
    assert s.events() == [("LIVE", expected)]
    assert not s.worker()  # Restart gate is closed even for PREPARING recovery.
    s.feed("LIVE", 1)
    if expected == "RETRYING":
        assert s.worker()
    else:
        assert not s.worker()


def test_duplicate_same_direction_rss_does_not_block_mail(scenario):
    s = scenario
    s.baseline()
    s.live()
    service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
    for _ in range(100):
        service.event("possible_live")
    assert s.worker() and not s.worker()


def test_contradictory_rss_blocks_mail_until_http(scenario):
    s = scenario
    s.baseline()
    s.live()
    service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
    service.event("possible_offline")
    assert not s.worker()
    s.feed("LIVE", 1)
    assert s.worker()


def test_old_pending_live_waits_for_restart_identity_reconciliation(scenario):
    s = scenario
    s.baseline()
    s.feed("LIVE", 30, started_at=1020)
    s.feed("LIVE", 10, started_at=1020)
    s.restart(601)
    assert not s.worker()
    assert s.events() == [("LIVE", "PENDING")]
    s.feed("LIVE", 1, started_at=1020)
    assert s.worker()
