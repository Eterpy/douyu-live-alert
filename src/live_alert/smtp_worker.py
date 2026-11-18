"""Bound SMTP DNS/TLS/DATA in a killable process with a durable DATA gate."""

import base64
import json
import os
import selectors
import signal
import smtplib
import ssl
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import asdict
from email import message_from_bytes
from email.message import EmailMessage
from email.policy import SMTP
from typing import cast

from .config import Config
from .mail import Deferred, Expired, connect_smtp, deliver
from .storage import audit


class WarmSMTP:
    """Owned by the single mail consumer; never shared across sender threads."""

    def __init__(self, c: Config, preconnects: threading.Semaphore | None = None):
        self.c = c
        # Shared by all rooms: caps idle preconnected workers, not real sends.
        self.preconnects = preconnects
        self.permit = False
        self.process: subprocess.Popen | None = None
        self.deadline = 0.0
        self.buffer = b""

    @property
    def expired(self) -> bool:
        return self.process is not None and time.monotonic() >= self.deadline

    def prepare(self) -> None:
        """Preconnect if a shared permit is free; otherwise connect when sending."""
        if self.process is not None:
            return
        if self.preconnects is not None:
            if not self.preconnects.acquire(blocking=False):
                return
            self.permit = True
        self.start()
        if self.process is None:
            self._release()

    def _release(self) -> None:
        if self.permit and self.preconnects is not None:
            self.permit = False
            self.preconnects.release()

    def start(self) -> None:
        if self.process is not None:
            return
        self.deadline = time.monotonic() + self.c.smtp_total_timeout_seconds
        self.buffer = b""
        try:
            self.process = subprocess.Popen(
                # -P keeps the working directory off sys.path: no module shadowing.
                [sys.executable, "-P", "-m", "live_alert.smtp_worker"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                bufsize=0,
            )
            assert self.process.stdin is not None
            os.set_blocking(self.process.stdin.fileno(), False)
            self._send(asdict(self.c))
        except (OSError, TimeoutError):
            self.close()

    def _send(self, value: object) -> None:
        assert self.process is not None and self.process.stdin is not None
        data = (json.dumps(value) + "\n").encode()
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdin, selectors.EVENT_WRITE)
            while data:
                remaining = self.deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise TimeoutError()
                try:
                    n = os.write(self.process.stdin.fileno(), data)
                    data = data[n:]
                except BlockingIOError:
                    continue

    def _read(self) -> dict:
        assert self.process is not None and self.process.stdout is not None
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            while b"\n" not in self.buffer:
                remaining = self.deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise TimeoutError()
                chunk = os.read(self.process.stdout.fileno(), 65536)
                if not chunk:
                    raise EOFError()
                self.buffer += chunk
                if len(self.buffer) > 65536:
                    raise ValueError("SMTP worker protocol too large")
        line, self.buffer = self.buffer.split(b"\n", 1)
        return json.loads(line)

    def __call__(
        self, c: Config, msg: EmailMessage, before_data: Callable[[], None]
    ) -> tuple[str, str]:
        submitting = False
        try:
            # Leave enough time to hand over an idle child's alarm safely.
            if self.process is not None and self.deadline - time.monotonic() < min(
                1, c.smtp_total_timeout_seconds / 4
            ):
                self.close()
            reused = self.process is not None
            self.start()
            if reused:
                self.deadline = time.monotonic() + c.smtp_total_timeout_seconds
            if self.process is None:
                return "RETRYING", "worker_start_failed"
            response = self._read()
            if response.get("event") == "READY":
                audit(
                    c,
                    "smtp_timing",
                    "CONNECTING",
                    "READY",
                    "authenticated",
                    authenticated_at=response["at"],
                )
                self._send(
                    {
                        "message": base64.b64encode(msg.as_bytes()).decode(),
                        "timeout": max(0.001, self.deadline - time.monotonic()),
                    }
                )
                response = self._read()
            if response.get("event") == "CHECK":
                # Commit SUBMITTING before authorizing any DATA bytes.
                before_data()
                submitting = True
                self._send({"go": True})
                response = self._read()
            if response.get("state") not in (
                "SENT",
                "FAILED",
                "RETRYING",
                "UNCERTAIN",
                "EXPIRED",
            ):
                raise ValueError("Unexpected SMTP worker result")
            if response["state"] == "SENT" and not submitting:
                raise ValueError("SMTP success before DATA authorization")
            return response["state"], response["reason"]
        except Deferred:
            return "DEFERRED", "waiting_for_fresh_evidence"
        except Expired:
            return "EXPIRED", "obsolete_before_submission"
        except (OSError, EOFError, ValueError) as exc:
            return ("UNCERTAIN" if submitting else "RETRYING"), type(exc).__name__
        finally:
            self.close()

    def close(self) -> None:
        process, self.process = self.process, None
        if process is not None:
            if process.poll() is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            process.wait()
            for pipe in (process.stdin, process.stdout):
                if pipe is not None:
                    pipe.close()
        self.buffer = b""
        self._release()


def bounded_deliver(
    c: Config, msg: EmailMessage, before_data: Callable[[], None]
) -> tuple[str, str]:
    return WarmSMTP(c)(c, msg, before_data)


def emit(value: dict) -> None:
    print(json.dumps(value), flush=True)


def main() -> None:
    c = Config(**json.loads(sys.stdin.readline()))
    # Also bound an orphan child if the parent is SIGKILLed while DNS/DATA blocks.
    signal.signal(signal.SIGALRM, lambda *_: os._exit(124))
    signal.setitimer(signal.ITIMER_REAL, c.smtp_total_timeout_seconds)
    client = None
    authorized = False
    try:
        client = connect_smtp(c)
        emit({"event": "READY", "at": time.time()})
        command = json.loads(sys.stdin.readline())
        # The idle preconnect lifetime must not eat into the delivery budget.
        budget = min(c.smtp_total_timeout_seconds, float(command["timeout"]))
        if budget <= 0:
            raise TimeoutError()
        signal.setitimer(signal.ITIMER_REAL, budget)
        msg = cast(
            EmailMessage,
            message_from_bytes(base64.b64decode(command["message"]), policy=SMTP),
        )

        def before_data() -> None:
            nonlocal authorized
            emit({"event": "CHECK"})
            if not json.loads(sys.stdin.readline()).get("go"):
                raise Expired()
            authorized = True

        state, reason = deliver(c, msg, before_data, client=client)
        client = None  # deliver owns and closes it, including all failure paths.
    except smtplib.SMTPResponseException as exc:
        state, reason = (
            (
                "UNCERTAIN"
                if authorized
                else ("RETRYING" if exc.smtp_code // 100 == 4 else "FAILED")
            ),
            f"smtp_{exc.smtp_code}",
        )
    except (ssl.SSLCertVerificationError, smtplib.SMTPNotSupportedError) as exc:
        state, reason = ("UNCERTAIN" if authorized else "FAILED"), type(exc).__name__
    except (OSError, smtplib.SMTPException, ValueError) as exc:
        state, reason = ("UNCERTAIN" if authorized else "RETRYING"), type(exc).__name__
    finally:
        if client is not None:
            with suppress(OSError, smtplib.SMTPException):
                client.close()
    emit({"state": state, "reason": reason})


if __name__ == "__main__":
    main()
