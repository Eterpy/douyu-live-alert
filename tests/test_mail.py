import smtplib
import ssl
from dataclasses import replace
from unittest.mock import MagicMock, patch

import pytest
from conftest import config

from live_alert.cli import main
from live_alert.mail import deliver, message


def test_waiting_dependency_and_retry_order(scenario):
    s = scenario
    s.baseline()
    s.live()
    assert s.worker("RETRYING")
    assert not s.worker()
    s.feed("LIVE", 10)
    assert s.worker()
    s.offline()
    assert s.worker()
    assert [m["Subject"] for m in s.sent] == [
        "🎙️ 斗鱼房间 10001 开播啦～",
        "🎙️ 斗鱼房间 10001 开播啦～",
        "🌙 斗鱼房间 10001 下播休息啦～",
    ]


@pytest.mark.parametrize("result", ["FAILED", "UNCERTAIN", "EXPIRED"])
def test_dependency_terminal_failure(scenario, result):
    s = scenario
    s.baseline()
    s.live()
    s.offline()
    s.worker(result)
    assert not s.worker()
    assert s.events()[-1] == ("OFFLINE", "SUPPRESSED_NO_LIVE_NOTICE")


def test_expire_and_limit_retries(scenario):
    s = scenario
    s.baseline()
    s.live()
    for _ in range(s.c.smtp_max_attempts):
        assert s.worker("RETRYING")
        s.feed("LIVE", 80)
    assert s.events()[-1] == ("LIVE", "FAILED")
    s.offline()
    s.live()
    s.t += 901
    assert not s.worker()
    assert s.events()[-1] == ("LIVE", "EXPIRED")


@pytest.mark.parametrize(
    "phase,outcome", [("PREPARING", "RETRYING"), ("SUBMITTING", "UNCERTAIN")]
)
def test_recover_delivery_phase(scenario, phase, outcome):
    s = scenario
    s.baseline()
    s.live()
    with s.db:
        s.db.execute("UPDATE outbox SET state=?", (phase,))
    s.restart()
    assert s.events() == [("LIVE", outcome)]


def test_dry_run_does_not_connect_and_never_promotes(scenario):
    s = scenario
    s.restart(notification_mode="dry-run")
    s.baseline()
    s.live()
    s.offline()
    assert not s.worker() and not s.sent
    s.restart(notification_mode="send")
    assert not s.worker() and not s.sent
    assert s.events() == [("LIVE", "SIMULATED"), ("OFFLINE", "SIMULATED")]


def test_mid_session_mode_switch(scenario):
    s = scenario
    s.restart(notification_mode="dry-run")
    s.baseline()
    s.live()
    s.restart(notification_mode="send")
    s.feed("LIVE", 1)
    s.offline()
    assert not s.worker()
    assert s.events()[-1] == ("OFFLINE", "SUPPRESSED_NO_LIVE_NOTICE")


@pytest.mark.parametrize(
    "setting,value,expected",
    [
        ("offline_requires_live_email", False, "PENDING"),
        ("notify_live", False, "SUPPRESSED_NO_LIVE_NOTICE"),
    ],
)
def test_optional_pairing(scenario, setting, value, expected):
    s = scenario
    s.c = s.state.c = replace(s.c, **{setting: value})
    s.baseline()
    s.live()
    s.offline()
    assert s.events()[-1] == ("OFFLINE", expected)
    if setting == "offline_requires_live_email":
        assert s.worker() and not s.worker()
        assert [m["Subject"] for m in s.sent] == ["🌙 斗鱼房间 10001 下播休息啦～"]


@pytest.mark.parametrize(
    "stage,error,result",
    [
        ("connect", ConnectionRefusedError(), "RETRYING"),
        ("starttls", ssl.SSLCertVerificationError(), "FAILED"),
        ("login", smtplib.SMTPAuthenticationError(535, b"no"), "FAILED"),
        ("getreply", smtplib.SMTPServerDisconnected(), "UNCERTAIN"),
        ("getreply", smtplib.SMTPDataError(451, b"try later"), "RETRYING"),
        ("getreply", None, "SENT"),
    ],
)
def test_smtp_submission_boundary(stage, error, result):
    c = config()
    client = MagicMock()
    client.mail.return_value = (250, b"ok")
    client.rcpt.return_value = (250, b"ok")
    client.docmd.return_value = (354, b"go")
    client.getreply.return_value = (250, b"queued")
    if stage != "connect":
        getattr(client, stage).side_effect = error
    mark = MagicMock()
    msg = message(c, "fixture@example.org", "live\nBcc: hostile", "body")
    assert "Bcc" not in msg
    with patch(
        "smtplib.SMTP",
        return_value=client,
        side_effect=error if stage == "connect" else None,
    ):
        assert deliver(c, msg, mark)[0] == result
    if stage == "getreply":
        mark.assert_called_once()
        ctx = client.starttls.call_args.kwargs["context"]
        assert ctx.check_hostname and ctx.verify_mode == ssl.CERT_REQUIRED
    else:
        mark.assert_not_called()


def test_test_email_separate_from_database(tmp_path, monkeypatch):
    env = {
        "SMTP_USERNAME": "test-login",
        "SMTP_PASSWORD": "fixture",
        "SMTP_TO_ADDRESS": "fixture@example.org",
        "SMTP_HOST": "smtp.example.org",
        "SMTP_FROM_ADDRESS": "alerts@example.org",
        "DATA_DIR": str(tmp_path),
    }
    with (
        patch.dict("os.environ", env, clear=True),
        patch("live_alert.cli.deliver", return_value=("SENT", "smtp_accepted")) as fake,
    ):
        assert main(["test-email"]) == 0
        assert fake.call_args.args[1]["Subject"].startswith("[TEST]")
    assert not list(tmp_path.iterdir())


def test_claim_transaction_excludes_concurrent_sender(scenario):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    from live_alert.mail import process_one

    s = scenario
    s.baseline()
    s.live()
    submitting = Event()
    release = Event()

    def transport(c, msg, before_data):
        before_data()
        submitting.set()
        assert release.wait(5)
        return "SENT", "accepted"

    with ThreadPoolExecutor() as pool:
        first = pool.submit(process_one, s.c, transport, lambda: s.t)
        try:
            assert submitting.wait(5)
            assert not s.worker()
        finally:
            release.set()
        assert first.result(5)
    assert s.events() == [("LIVE", "SENT")] and not s.sent


def test_disabled_notification_suppresses_preexisting_queue(scenario):
    s = scenario
    s.baseline()
    s.live()
    s.offline()
    s.restart(notify_live=False)
    assert not s.worker()
    assert s.events() == [
        ("LIVE", "SUPPRESSED_DISABLED"),
        ("OFFLINE", "SUPPRESSED_NO_LIVE_NOTICE"),
    ]
