import pytest
from conftest import config

from live_alert.mail import process_one
from live_alert.service import Service


@pytest.mark.parametrize("during_handshake", [False, True])
def test_old_offline_mail_expires_after_a_later_session_ends(
    scenario, during_handshake
):
    s = scenario
    s.baseline()
    s.live()
    assert s.worker()
    s.offline()
    first = s.db.execute("SELECT id FROM outbox WHERE kind='OFFLINE'").fetchone()[0]
    writes = []

    def later_session():
        s.live()
        s.offline()

    def transport(c, msg, before_data):
        if during_handshake:
            later_session()
        before_data()
        writes.append(msg)
        return "SENT", "accepted"

    if not during_handshake:
        later_session()
    process_one(s.c, transport, lambda: s.t)
    assert (
        s.db.execute("SELECT state FROM outbox WHERE id=?", (first,)).fetchone()[0]
        == "EXPIRED"
    )
    assert not writes


def known_live(s):
    s.baseline()
    s.feed("LIVE", 30, started_at=1020)
    s.feed("LIVE", 10, started_at=1020)


@pytest.mark.parametrize("change", ["unknown", "age", "hint"])
def test_temporary_evidence_loss_during_handshake_can_recover(scenario, change):
    s = scenario
    known_live(s)
    assert s.worker()
    s.offline()

    def transport(c, msg, before_data):
        if change == "unknown":
            s.feed("UNKNOWN", 1)
        elif change == "age":
            s.t += 31
        else:
            service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
            service.event("possible_live")
        before_data()
        pytest.fail("must not submit without evidence")

    assert process_one(s.c, transport, lambda: s.t)
    assert s.events()[-1] == ("OFFLINE", "PENDING")
    assert (
        s.db.execute("SELECT attempts FROM outbox WHERE kind='OFFLINE'").fetchone()[0]
        == 0
    )
    s.feed("OFFLINE", 10)
    assert s.worker()
    assert not s.worker()
    assert s.events()[-1] == ("OFFLINE", "SENT")


def test_unconfirmed_session_change_does_not_discard_pending_live(scenario):
    s = scenario
    known_live(s)
    s.feed("LIVE", 10, started_at=s.t + 5)
    assert not s.worker()
    assert s.events() == [("LIVE", "PENDING")]
    s.feed("LIVE", 10, started_at=1020)
    assert s.worker() and not s.worker()


def test_confirmed_session_change_still_expires_old_notice(scenario):
    s = scenario
    known_live(s)
    new_start = s.t + 5
    s.feed("LIVE", 10, started_at=new_start)
    s.feed("LIVE", 10, started_at=new_start)
    assert s.worker() and not s.worker()
    assert s.events() == [
        ("LIVE", "EXPIRED"),
        ("OFFLINE", "SUPPRESSED_SESSION_CHANGE"),
        ("LIVE", "SENT"),
    ]


@pytest.mark.parametrize("action", ["offline", "new_session", "unknown"])
def test_state_change_during_data_invitation_never_sends_stale_body(scenario, action):
    from unittest.mock import MagicMock, patch

    from live_alert.mail import deliver

    s = scenario
    known_live(s)
    client = MagicMock()
    client.mail.return_value = client.rcpt.return_value = (250, b"ok")

    def invite(command):
        assert command == "DATA"
        if action == "offline":
            s.offline()
        elif action == "new_session":
            start = s.t + 5
            s.feed("LIVE", 10, started_at=start)
            s.feed("LIVE", 10, started_at=start)
        else:
            s.feed("UNKNOWN", 1)
        return 354, b"go"

    client.docmd.side_effect = invite
    with patch("smtplib.SMTP", return_value=client):
        assert process_one(s.c, deliver, lambda: s.t)
    client.send.assert_not_called()
    assert s.events()[0][1] == ("PENDING" if action == "unknown" else "EXPIRED")


def test_smtp_dot_stuffing_and_data_authorization_order():
    from unittest.mock import MagicMock, patch

    from live_alert.mail import deliver, message

    c = config()
    client = MagicMock()
    client.mail.return_value = client.rcpt.return_value = (250, b"ok")
    client.docmd.return_value = (354, b"go")
    client.getreply.return_value = (250, b"accepted")
    calls = []
    client.docmd.side_effect = lambda *_: calls.append("354") or (354, b"go")
    client.send.side_effect = lambda _: calls.append("body")
    with patch("smtplib.SMTP", return_value=client):
        assert (
            deliver(
                c,
                message(c, c.smtp_to_address, "test", ".\n..line\nend"),
                lambda: calls.append("authorized"),
            )[0]
            == "SENT"
        )
    assert calls == ["354", "authorized", "body"]
    payload = client.send.call_args.args[0]
    assert b"\r\n..\r\n...line\r\nend\r\n" in payload
    assert payload.endswith(b"\r\n.\r\n")


@pytest.mark.parametrize("phase", ["PREPARING", "SUBMITTING"])
def test_real_parent_sigkill_does_not_duplicate_smtp(scenario, tmp_path, phase):
    import json
    import os
    import subprocess
    import sys
    import time
    from dataclasses import asdict, replace
    from pathlib import Path

    from live_alert.storage import initialize

    s = scenario
    known_live(s)
    c = replace(s.c, smtp_total_timeout_seconds=3)
    gate, release, bodies = (tmp_path / n for n in ("gate", "release", "bodies"))
    hooks = tmp_path / "hooks"
    hooks.mkdir()
    # Inject fake SMTP into both real processes, never connect to a mail server.
    (hooks / "sitecustomize.py").write_text(f"""
import smtplib, time
from pathlib import Path
class FakeSMTP:
    def __init__(self, *a, **k): pass
    def ehlo(self): pass
    def starttls(self, **k): pass
    def login(self, *a): pass
    def mail(self, *a): return 250, b'ok'
    def rcpt(self, *a): return 250, b'ok'
    def pause(self):
        Path({str(gate)!r}).touch()
        while not Path({str(release)!r}).exists(): time.sleep(.01)
    def docmd(self, *a):
        if {phase!r} == 'PREPARING': self.pause()
        return 354, b'go'
    def send(self, payload):
        with open({str(bodies)!r}, 'a') as f: f.write('DATA\\n')
        if {phase!r} == 'SUBMITTING': self.pause()
    def getreply(self): return 250, b'accepted'
    def close(self): pass
smtplib.SMTP = FakeSMTP
""")
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            [str(hooks), str(Path(__file__).resolve().parents[1] / "src")]
        ),
    }
    script = "import json,sys; from live_alert.config import Config; from live_alert.mail import process_one; process_one(Config(**json.loads(sys.argv[1])), clock=lambda:float(sys.argv[2]))"
    parent = subprocess.Popen(
        [sys.executable, "-c", script, json.dumps(asdict(c)), str(s.t)],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 5
        while (
            not gate.exists() and parent.poll() is None and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        assert gate.exists()
        assert s.events() == [("LIVE", phase)]
        parent.kill()
        parent.wait(timeout=3)
        initialize(s.db, c)
        assert s.events() == [
            ("LIVE", "RETRYING" if phase == "PREPARING" else "UNCERTAIN")
        ]
        release.touch()
        s.feed("LIVE", 10)
        restarted = subprocess.run(
            [sys.executable, "-c", script, json.dumps(asdict(c)), str(s.t)],
            env=env,
            capture_output=True,
            timeout=5,
            check=False,
        )
        assert restarted.returncode == 0, restarted.stderr.decode()
        assert s.events() == [("LIVE", "SENT" if phase == "PREPARING" else "UNCERTAIN")]
        assert bodies.read_text().splitlines() == ["DATA"]
    finally:
        release.touch()
        if parent.poll() is None:
            parent.kill()
        parent.wait(timeout=3)
        parent.stderr.close()


def test_mail_task_cancellation_drains_thread_before_closing_transport(scenario):
    import asyncio
    from threading import Event
    from unittest.mock import patch

    s = scenario
    entered, release = Event(), Event()
    closed = []

    def worker(*args):
        entered.set()
        assert release.wait(3)
        return False

    async def exercise():
        service = Service(s.c, s.db, None)
        with (
            patch("live_alert.service.process_one", worker),
            patch("live_alert.service.WarmSMTP.close", lambda _: closed.append(True)),
        ):
            task = asyncio.create_task(service.mail_loop())
            try:
                for _ in range(100):
                    if entered.is_set():
                        break
                    await asyncio.sleep(0.01)
                assert entered.is_set()
                task.cancel()
                await asyncio.sleep(0.02)
                assert not closed and not task.done()
            finally:
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await task
            assert closed == [True]

    asyncio.run(exercise())
