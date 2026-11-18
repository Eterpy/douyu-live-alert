import asyncio
import json
import os
import statistics
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest
from conftest import config

from live_alert.config import Config
from live_alert.mail import Expired, message, process_one
from live_alert.service import Service
from live_alert.smtp_worker import WarmSMTP


@pytest.fixture
def smtp_process(monkeypatch, tmp_path):
    """Real isolated worker, fake SMTP in that child; no network or email."""
    original = subprocess.Popen
    children = []
    marker = tmp_path / "data.txt"

    def install(stage="ok", handshake=0):
        script = f"""
import os, time, smtplib
from pathlib import Path
from live_alert.smtp_worker import main
stage = {stage!r}
class SMTP:
    def __init__(self, *args, **kwargs):
        if stage == 'connect': time.sleep(60)
    def ehlo(self): pass
    def starttls(self, **kwargs):
        if stage == 'tls': time.sleep(60)
    def login(self, *args):
        if stage == 'auth': time.sleep(60)
        time.sleep({handshake})
    def mail(self, *args): return (250, b'ok')
    def rcpt(self, *args): return (250, b'ok')
    def docmd(self, command):
        if stage == 'data_command': time.sleep(60)
        return (354, b'go')
    def send(self, payload):
        Path({str(marker)!r}).write_bytes(payload)
    def getreply(self):
        if stage == 'slow_data': time.sleep(.5)
        if stage == 'data': time.sleep(60)
        if stage == 'crash': os._exit(1)
        if stage == 'reject': raise smtplib.SMTPDataError(451, b'temporary')
        return (250, b'accepted')
    def close(self):
        if stage == 'close_error': raise OSError('cleanup failed')
smtplib.SMTP = SMTP
main()
"""

        def spawn(*args, **kwargs):
            child = original(
                [sys.executable, "-c", script],
                env={
                    **os.environ,
                    "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
                },
                **kwargs,
            )
            children.append(child)
            return child

        monkeypatch.setattr("live_alert.smtp_worker.subprocess.Popen", spawn)
        return marker, children

    yield install
    for child in children:
        if child.poll() is None:
            child.kill()
        child.wait()


@pytest.mark.parametrize(
    "stage,expected",
    [
        ("connect", "RETRYING"),
        ("tls", "RETRYING"),
        ("auth", "RETRYING"),
        ("data_command", "RETRYING"),
        ("data", "UNCERTAIN"),
        ("crash", "UNCERTAIN"),
        ("reject", "RETRYING"),
        ("ok", "SENT"),
        ("close_error", "SENT"),
    ],
)
def test_real_smtp_process_deadline_and_data_boundary(
    smtp_process, scenario, stage, expected
):
    _marker, children = smtp_process(stage)
    s = scenario
    # The budget covers interpreter start-up of the SMTP worker; leave room for it
    # on a busy machine while hanging stages still finish well within the bound.
    s.c = s.state.c = replace(s.c, smtp_total_timeout_seconds=1.0)
    s.baseline()
    s.live()
    started = time.monotonic()
    assert process_one(s.c, WarmSMTP(s.c), lambda: s.t)
    elapsed = time.monotonic() - started
    assert elapsed < 4
    assert s.events() == [("LIVE", expected)]
    attempt = s.db.execute("SELECT * FROM delivery_attempts").fetchone()
    assert (attempt["data_at"] is not None) == (
        stage not in ("connect", "tls", "auth", "data_command")
    )
    assert attempt["state"] == expected and attempt["finished"] is not None
    assert all(child.poll() is not None for child in children)
    if expected == "UNCERTAIN":
        assert not s.worker()  # No retry after possible DATA acceptance.
    print(f"smtp stage={stage}: {elapsed:.3f}s -> {expected}")


def test_preconnect_cannot_submit_and_cancel_reaps(smtp_process):
    marker, children = smtp_process()
    c = config()
    warm = WarmSMTP(c)
    warm.start()
    time.sleep(0.15)
    assert not marker.exists()
    warm.close()
    assert children[0].poll() is not None and not marker.exists()


def test_before_data_rejection_never_sends(smtp_process):
    marker, _ = smtp_process()
    c = config()

    def refuse():
        raise Expired()

    assert (
        WarmSMTP(c)(c, message(c, c.smtp_to_address, "test", "body"), refuse)[0]
        == "EXPIRED"
    )
    assert not marker.exists()


def test_preconnect_reduces_submission_latency(smtp_process):
    marker, _ = smtp_process(handshake=0.3)
    c = config()
    msg = message(c, c.smtp_to_address, "test", "body")
    cold, warm_times = [], []
    for _ in range(3):
        t = time.monotonic()
        assert WarmSMTP(c)(c, msg, lambda: None)[0] == "SENT"
        cold.append(time.monotonic() - t)
        marker.unlink()
        warm = WarmSMTP(c)
        warm.start()
        time.sleep(0.5)  # Simulated confirmation work overlaps SMTP authentication.
        assert not marker.exists()
        t = time.monotonic()
        assert warm(c, msg, lambda: None)[0] == "SENT"
        warm_times.append(time.monotonic() - t)
    assert statistics.median(warm_times) < statistics.median(cold) / 2
    print(
        f"fake SMTP 300ms auth: cold median={statistics.median(cold):.3f}s; preconnected median={statistics.median(warm_times):.3f}s"
    )


def test_queue_notification_wakes_consumer_without_one_second_delay(scenario):
    s = scenario
    s.c = replace(s.c, notification_mode="dry-run")

    async def exercise():
        service = Service(s.c, s.db, None)
        called = asyncio.Event()
        loop = asyncio.get_running_loop()
        count = 0

        def worker(*args):
            nonlocal count
            count += 1
            loop.call_soon_threadsafe(called.set)
            return False

        with patch("live_alert.service.process_one", worker):
            task = asyncio.create_task(service.mail_loop())
            try:
                await asyncio.wait_for(called.wait(), 1)
                called.clear()
                await asyncio.sleep(0.02)
                t = time.monotonic()
                service.mail_wake.set()
                await asyncio.wait_for(called.wait(), 0.5)
                print(f"queue wake latency={time.monotonic() - t:.4f}s")
                assert count >= 2
            finally:
                service.stop.set()
                service.mail_wake.set()
                await task

    asyncio.run(exercise())


def test_status_exposes_latency(scenario, capsys):
    from live_alert.cli import read_status

    s = scenario
    s.baseline()
    s.live()
    s.t += 2
    assert s.worker()
    assert read_status([s.c], False) == 0
    room = json.loads(capsys.readouterr().out)["rooms"][str(s.c.room_id)]
    event = room["latest"][0]
    assert event["confirmation_to_accept_seconds"] == 2
    assert event["first_http_to_accept_seconds"] == 12
    assert event["smtp_data_at"] is not None


@pytest.mark.parametrize(
    "env",
    [
        {"LIVE_CONFIRM_GAP_SECONDS": "9"},
        {"LIVE_CONFIRM_SAMPLES": "1"},
        {"SMTP_TOTAL_TIMEOUT_SECONDS": "10"},
        {"SMTP_TOTAL_TIMEOUT_SECONDS": "91"},
    ],
)
def test_latency_configuration_keeps_safety_bounds(env):
    with pytest.raises(ValueError):
        Config.load(env)


@pytest.mark.parametrize("interval,expected_delay", [(60, 69), (30, 39)])
def test_poll_fallback_latency_keeps_ten_second_confirmation(
    scenario, interval, expected_delay
):
    from live_alert.douyu import Observation

    s = scenario
    s.c = s.state.c = replace(s.c, poll_interval_seconds=interval)
    s.baseline()
    start = s.t
    requests = []

    class Adapter:
        async def sample(self):
            elapsed = s.t - start
            requests.append(elapsed)
            if elapsed >= interval + 10:
                service.stop.set()
            return Observation(
                "LIVE" if elapsed >= 1 else "OFFLINE",
                "fixture",
                s.t,
                s.t,
                s.t,
                started_at=start + 1 if elapsed >= 1 else None,
            )

    service = Service(s.c, s.db, Adapter(), lambda: s.t, lambda: s.t)

    async def wait(delay):
        s.t += delay

    with (
        patch.object(service, "wait_for_sample", wait),
        patch("random.uniform", return_value=1),
    ):
        asyncio.run(service.sample_loop())
    assert requests == [0, interval, interval + 10]
    assert s.worker()
    event = s.db.execute("SELECT sent_at FROM outbox WHERE kind='LIVE'").fetchone()
    assert event[0] - (start + 1) == expected_delay
    print(
        f"virtual poll={interval}s: start-to-accept={expected_delay}s, confirmation gap=10s"
    )


@pytest.mark.parametrize("cancel", [False, True])
def test_service_preconnect_lifecycle(smtp_process, scenario, cancel):
    marker, children = smtp_process()
    s = scenario
    s.baseline()
    service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
    s.state = service.state
    s.feed("OFFLINE")
    s.feed("LIVE", 1)

    async def exercise():
        with patch(
            "live_alert.service.process_one",
            lambda c, transport, *_: process_one(c, transport, lambda: s.t),
        ):
            task = asyncio.create_task(service.mail_loop())
            try:
                for _ in range(100):
                    if children:
                        break
                    await asyncio.sleep(0.01)
                assert children and not marker.exists()
                s.feed("REPLAY" if cancel else "LIVE", 10)
                service.mail_wake.set()
                for _ in range(100):
                    if children[0].poll() is not None:
                        break
                    await asyncio.sleep(0.01)
                assert children[0].poll() is not None
                assert marker.exists() != cancel
                if not cancel:
                    for _ in range(100):
                        if s.events() == [("LIVE", "SENT")]:
                            break
                        await asyncio.sleep(0.01)
                    assert s.events() == [("LIVE", "SENT")]
            finally:
                service.stop.set()
                service.mail_wake.set()
                await task

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "changes",
    [
        {"notification_mode": "dry-run"},
        {"smtp_preconnect": False},
        {"notify_live": False},
    ],
)
def test_disabled_preconnect_never_starts_worker(scenario, changes):
    s = scenario
    s.c = s.state.c = replace(s.c, **changes)
    s.baseline()
    service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
    s.state = service.state
    s.feed("OFFLINE")
    s.feed("LIVE", 1)

    async def exercise():
        with patch.object(
            WarmSMTP, "start", side_effect=AssertionError("unexpected SMTP")
        ):
            task = asyncio.create_task(service.mail_loop())
            await asyncio.sleep(0.05)
            service.stop.set()
            service.mail_wake.set()
            await task

    asyncio.run(exercise())


def test_orphan_smtp_process_has_own_deadline(smtp_process):
    _, children = smtp_process("connect")
    c = replace(config(), smtp_total_timeout_seconds=0.25)
    warm = WarmSMTP(c)
    warm.start()
    try:
        # Deliberately do not run the parent's timeout controller.
        assert children[0].wait(timeout=2) == 124
    finally:
        warm.close()


def test_preconnect_idle_time_does_not_consume_send_budget(smtp_process):
    _, children = smtp_process("slow_data")
    c = replace(config(), smtp_total_timeout_seconds=0.8)
    warm = WarmSMTP(c)
    warm.start()
    time.sleep(0.45)
    assert (
        warm(c, message(c, c.smtp_to_address, "test", "body"), lambda: None)[0]
        == "SENT"
    )
    assert len(children) == 1  # Reuse the authenticated process, no cold reconnect.
    assert children[0].poll() is not None


def test_evidence_deferral_crosses_worker_protocol_without_data(smtp_process):
    from live_alert.mail import Deferred

    marker, children = smtp_process()
    c = config()

    def defer():
        raise Deferred()

    assert (
        WarmSMTP(c)(c, message(c, c.smtp_to_address, "test", "body"), defer)[0]
        == "DEFERRED"
    )
    assert not marker.exists() and children[0].poll() is not None


def test_idle_preconnects_are_capped_and_permits_returned(smtp_process):
    import threading

    _marker, children = smtp_process()
    c = config()
    permits = threading.BoundedSemaphore(1)
    first, second = WarmSMTP(c, permits), WarmSMTP(c, permits)
    first.prepare()
    second.prepare()
    assert first.process is not None and second.process is None
    first.close()
    second.prepare()  # The released permit is reusable.
    assert second.process is not None
    second.close()
    assert permits.acquire(blocking=False)  # Nothing leaked.
    assert all(child.poll() is not None for child in children)


def test_silent_confirmations_do_not_preconnect(scenario):
    s = scenario
    s.baseline()
    s.restart(30)  # Reconciling: a live start found now may predate the restart.
    s.feed("LIVE", 5)
    assert s.state.phase == "LIVE_CANDIDATE" and not s.state.expects_notice()
    s.restart(0)
    s.feed("OFFLINE", 5)
    s.feed("LIVE", 30)
    assert s.state.expects_notice()


def test_smtp_worker_keeps_working_directory_off_sys_path(monkeypatch):
    commands = []

    def spawn(command, **kwargs):
        commands.append(command)
        raise OSError("not started")

    monkeypatch.setattr("live_alert.smtp_worker.subprocess.Popen", spawn)
    WarmSMTP(config()).start()
    assert commands == [[sys.executable, "-P", "-m", "live_alert.smtp_worker"]]
