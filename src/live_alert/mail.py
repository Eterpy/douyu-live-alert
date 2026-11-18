import re
import smtplib
import sqlite3
import ssl
import time
from collections.abc import Callable
from contextlib import closing, suppress
from datetime import datetime
from email.headerregistry import Address
from email.message import EmailMessage
from email.policy import SMTP
from email.utils import format_datetime, make_msgid
from zoneinfo import ZoneInfo

from .config import Config
from .storage import audit, connect


def message(
    c: Config, recipient: str, subject: str, body: str, message_id: str | None = None
) -> EmailMessage:
    msg = EmailMessage(policy=SMTP)
    sender = Address(c.smtp_from_name, addr_spec=c.smtp_from_address)
    msg["From"] = sender
    msg["To"] = Address(addr_spec=recipient)
    msg["Subject"] = " ".join(subject.splitlines())
    # RFC 5322 requires Date; some providers reject mail without a Message-ID.
    msg["Date"] = format_datetime(datetime.now(ZoneInfo("Asia/Shanghai")))
    msg["Message-ID"] = message_id or make_msgid(domain=sender.domain)
    msg.set_content(body, cte="quoted-printable")
    return msg


class Expired(Exception):
    pass


class Deferred(Exception):
    """No DATA authorized; wait for fresh evidence without spending an SMTP retry."""


def connect_smtp(c: Config) -> smtplib.SMTP:
    client: smtplib.SMTP | None = None
    try:
        context = ssl.create_default_context()
        if c.smtp_security == "ssl":
            client = smtplib.SMTP_SSL(
                c.smtp_host,
                c.smtp_port,
                timeout=c.smtp_timeout_seconds,
                context=context,
            )
        else:
            client = smtplib.SMTP(
                c.smtp_host, c.smtp_port, timeout=c.smtp_timeout_seconds
            )
            client.ehlo()
            client.starttls(context=context)
        client.ehlo()
        client.login(c.smtp_username, c.smtp_password)
        return client
    except BaseException:
        if client is not None:
            with suppress(OSError, smtplib.SMTPException):
                client.close()
        raise


def deliver(
    c: Config,
    msg: EmailMessage,
    before_data: Callable[[], None],
    client: smtplib.SMTP | None = None,
) -> tuple[str, str]:
    submitting = False
    try:
        if client is None:
            client = connect_smtp(c)
        for command, address in (
            (client.mail, c.smtp_from_address),
            (client.rcpt, str(msg["To"])),
        ):
            code, response = command(address)
            if code // 100 != 2:
                raise smtplib.SMTPResponseException(code, response)
        payload = msg.as_bytes()
        code, response = client.docmd("DATA")
        if code != 354:
            raise smtplib.SMTPDataError(code, response)
        # Authorize after the server's potentially slow DATA invitation, not before.
        before_data()
        submitting = True
        payload = re.sub(rb"(?m)^\.", b"..", payload)
        if not payload.endswith(b"\r\n"):
            payload += b"\r\n"
        client.send(payload + b".\r\n")
        code, response = client.getreply()
        if code != 250:
            raise smtplib.SMTPResponseException(code, response)
        return "SENT", "smtp_accepted"
    except Expired:
        return "EXPIRED", "obsolete_or_stale_before_submission"
    except smtplib.SMTPResponseException as exc:
        return (
            "RETRYING" if exc.smtp_code // 100 == 4 else "FAILED"
        ), f"smtp_{exc.smtp_code}"
    except (ssl.SSLCertVerificationError, smtplib.SMTPNotSupportedError) as exc:
        return "FAILED", type(exc).__name__
    except (OSError, smtplib.SMTPException) as exc:
        return ("UNCERTAIN" if submitting else "RETRYING"), type(exc).__name__
    finally:
        if client is not None:
            with suppress(OSError, smtplib.SMTPException):
                client.close()


def eligibility(db: sqlite3.Connection, c: Config, row: sqlite3.Row, now: float) -> str:
    session = db.execute(
        "SELECT * FROM sessions WHERE id=?", (row["session_id"],)
    ).fetchone()
    monitor = db.execute("SELECT * FROM monitor WHERE room=?", (c.room_id,)).fetchone()
    latest = db.execute(
        "SELECT state,utc FROM observations WHERE room=? ORDER BY rowid DESC LIMIT 1",
        (c.room_id,),
    ).fetchone()
    if not 0 <= now - row["created"] <= c.max_notification_age_seconds:
        return "EXPIRED"
    if not monitor["mail_ready"] or latest is None:
        return "WAIT"
    if row["kind"] == "LIVE":
        if session["ended"] is not None or monitor["session_id"] != row["session_id"]:
            return "EXPIRED"
        if monitor["stale_restart"]:
            return "WAIT"
    # One live-looking sample only defers an offline notice (the expected-state
    # check below); a confirmed new session makes it obsolete.
    if row["kind"] == "OFFLINE":
        latest_session = db.execute(
            "SELECT MAX(id) FROM sessions WHERE room=?", (c.room_id,)
        ).fetchone()[0]
        if monitor["session_id"] is not None or row["session_id"] != latest_session:
            return "EXPIRED"
    limit = (
        c.offline_final_evidence_max_age_seconds
        if row["kind"] == "OFFLINE"
        else c.poll_interval_seconds + c.http_timeout_seconds + 5
    )
    expected = ("LIVE",) if row["kind"] == "LIVE" else ("OFFLINE", "REPLAY")
    if latest["state"] not in expected or not 0 <= now - latest["utc"] <= limit:
        return "WAIT"
    return "READY"


def process_one(
    c: Config,
    transport=None,
    clock: Callable[[], float] = time.time,
    stopping: Callable[[], bool] = lambda: False,
) -> bool:
    # A call queued before shutdown must not start a new SMTP attempt.
    if c.notification_mode != "send" or stopping():
        return False
    if transport is None:
        from .smtp_worker import bounded_deliver

        transport = bounded_deliver
    now = clock()
    with closing(connect(c.db)) as db:
        # Each room polls about once a second; take the write lock only with work queued.
        queued = db.execute(
            """SELECT 1 FROM outbox q JOIN sessions s ON s.id=q.session_id
               WHERE s.room=? AND q.state IN ('PENDING','RETRYING','WAITING_FOR_LIVE_EMAIL')
               LIMIT 1""",
            (c.room_id,),
        ).fetchone()
        if queued is None:
            return False
        with db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute(
                """SELECT q.*, s.room FROM outbox q JOIN sessions s ON s.id=q.session_id
                              WHERE s.room=? AND q.state IN ('PENDING','RETRYING','WAITING_FOR_LIVE_EMAIL')
                              ORDER BY q.id""",
                (c.room_id,),
            ).fetchall()
            chosen = None
            for row in rows:
                state = row["state"]
                enabled = c.notify_live if row["kind"] == "LIVE" else c.notify_offline
                if not enabled:
                    _update(
                        db, c, row, "SUPPRESSED_DISABLED", "disabled_before_delivery"
                    )
                    continue
                if now - row["created"] > c.max_notification_age_seconds:
                    _update(db, c, row, "EXPIRED", "notification_age")
                    continue
                if row["recipient"] != c.smtp_to_address:
                    # A replaced recipient must not receive queued notices.
                    _update(
                        db, c, row, "SUPPRESSED_RECIPIENT_CHANGED", "recipient_changed"
                    )
                    continue
                if row["kind"] == "OFFLINE":
                    prior = db.execute(
                        """SELECT state FROM outbox WHERE session_id=? AND recipient=?
                                       AND kind='LIVE'""",
                        (row["session_id"], row["recipient"]),
                    ).fetchone()
                    prior_state = prior[0] if prior else None
                    if prior_state in (
                        "PENDING",
                        "RETRYING",
                        "PREPARING",
                        "SUBMITTING",
                    ):
                        continue
                    if (
                        state == "WAITING_FOR_LIVE_EMAIL"
                        or c.offline_requires_live_email
                    ) and prior_state != "SENT":
                        _update(
                            db,
                            c,
                            row,
                            "SUPPRESSED_NO_LIVE_NOTICE",
                            f"paired_live_{prior_state}",
                        )
                        continue
                eligible = eligibility(db, c, row, now)
                if eligible == "EXPIRED":
                    _update(db, c, row, "EXPIRED", "obsolete_state")
                    continue
                if eligible == "WAIT":
                    continue
                if row["attempts"] >= c.smtp_max_attempts:
                    _update(db, c, row, "FAILED", "attempt_limit")
                    continue
                if chosen is None and row["next_try"] <= now:
                    chosen = row
            if chosen is None:
                return False
            db.execute(
                "UPDATE outbox SET state='PREPARING', attempts=attempts+1 WHERE id=?",
                (chosen["id"],),
            )

            attempt_id = db.execute(
                "INSERT INTO delivery_attempts(outbox_id,started) VALUES (?,?)",
                (chosen["id"], clock()),
            ).lastrowid

        def before_data() -> None:
            with db:
                db.execute("BEGIN IMMEDIATE")
                eligible = eligibility(db, c, chosen, clock())
                if eligible == "WAIT":
                    raise Deferred()
                if eligible != "READY":
                    raise Expired()
                db.execute(
                    "UPDATE outbox SET state='SUBMITTING' WHERE id=?", (chosen["id"],)
                )
                db.execute(
                    "UPDATE delivery_attempts SET data_at=? WHERE id=?",
                    (clock(), attempt_id),
                )

        # Stable across retries of one notice, so receivers can de-duplicate.
        domain = Address(addr_spec=c.smtp_from_address).domain
        msg = message(
            c,
            chosen["recipient"],
            chosen["subject"],
            chosen["body"],
            f"<{chosen['kind'].lower()}.{chosen['session_id']}.{chosen['id']}"
            f".{int(chosen['created'] * 1000)}@{domain}>",
        )
        try:
            state, reason = transport(c, msg, before_data)
        except Deferred:
            state, reason = "DEFERRED", "waiting_for_fresh_evidence"
        except Expired:
            state, reason = "EXPIRED", "obsolete_before_submission"
        deferred = state == "DEFERRED"
        if deferred:
            state = "PENDING"
        attempts = chosen["attempts"] + 1
        if state == "RETRYING" and attempts >= c.smtp_max_attempts:
            state = "FAILED"
        finished = clock()
        delay = min(
            c.backoff_initial_seconds * 2 ** (attempts - 1), c.backoff_max_seconds
        )
        with db:
            db.execute(
                "UPDATE delivery_attempts SET finished=?,state=?,reason=? WHERE id=?",
                (finished, state, reason, attempt_id),
            )
            audit(
                c,
                "latency",
                "SMTP",
                state,
                reason,
                event=chosen["kind"],
                session=chosen["session_id"],
                confirmed_at=chosen["created"],
                finished_at=finished,
                confirmation_to_result_seconds=finished - chosen["created"],
            )
            _update(db, c, chosen, state, reason)
            if deferred:
                db.execute(
                    "UPDATE outbox SET attempts=attempts-1 WHERE id=?", (chosen["id"],)
                )
            db.execute(
                "UPDATE outbox SET next_try=?, sent_at=? WHERE id=?",
                (finished + delay, finished if state == "SENT" else None, chosen["id"]),
            )
        return True


def _update(
    db: sqlite3.Connection, c: Config, row: sqlite3.Row, state: str, reason: str
) -> None:
    db.execute(
        "UPDATE outbox SET state=?, reason=? WHERE id=?", (state, reason, row["id"])
    )
    audit(
        c,
        "smtp",
        row["state"],
        state,
        reason,
        session=row["session_id"],
        event=row["kind"],
    )
