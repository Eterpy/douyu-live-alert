"""Read Douyu HTTP responses in a size- and time-bounded subprocess."""

import asyncio
import json
import re
import sys
from dataclasses import asdict, replace
from email.message import Message
from http.client import HTTPException, IncompleteRead
from io import BytesIO
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from aiodouyu import ApiError, web
from aiodouyu.web import RoomInfo

# The worker emits a few short fields; anything larger is a broken or hostile reply.
MAX_OUTPUT_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 1024 * 1024
RAW_KEYS = ("room_id", "show_status", "status", "videoLoop", "show_time")
# Match a whole numeric field. A digit prefix inside a string is not a room ID.
ROOM_ID_FIELD = re.compile(
    r'"(?:room_id|rid)"\s*:\s*(?:"([0-9]+)"|([0-9]+))\s*(?=[,}])'
)


class WorkerError(ApiError):
    """A failure reported by the worker; `kind` keeps its original exception name."""

    def __init__(self, kind: str):
        super().__init__(kind)
        self.kind = kind


async def _call_worker(room_id: int, timeout: float, operation: str) -> dict:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-P",  # Keep the working directory off sys.path: no module shadowing.
        "-m",
        "live_alert.http_worker",
        str(room_id),
        str(timeout),
        operation,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        assert process.stdout is not None
        async with asyncio.timeout(timeout):
            output = b""
            while chunk := await process.stdout.read(MAX_OUTPUT_BYTES):
                output += chunk
                if len(output) > MAX_OUTPUT_BYTES:
                    raise WorkerError("WorkerProtocolError")
            await process.wait()
        if process.returncode != 0:
            raise WorkerError("WorkerExited")
        try:
            result = json.loads(output)
        except ValueError:
            # Distinct from a betard error page, which the worker reports itself.
            raise WorkerError("WorkerProtocolError") from None
        if not isinstance(result, dict):
            raise WorkerError("WorkerProtocolError")
        if "error" in result:
            kind = str(result["error"])[:64]
            if result.get("code"):
                raise WorkerError(kind) from HTTPError(
                    "https://www.douyu.com",
                    result["code"],
                    "HTTP failure",
                    Message(),
                    None,
                )
            raise WorkerError(kind)
        return result
    finally:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        await process.wait()


async def bounded_fetch(room_id: int, *, source: str, timeout: float) -> RoomInfo:
    if source != "betard":
        raise ValueError("Only betard evidence is supported")
    result = await _call_worker(room_id, timeout, source)
    try:
        return RoomInfo(**result)
    except TypeError:
        raise WorkerError("WorkerProtocolError") from None


async def resolve_room_id(room_id: int, *, timeout: float) -> int:
    if room_id <= 0:
        raise ValueError("room_id 必须为正整数")
    result = await _call_worker(room_id, timeout, "resolve")
    if result == {"room_id": None}:
        raise ApiError("未能从斗鱼页面确认真实房间号")
    real = result.get("room_id")
    if type(real) is not int or real <= 0:
        raise WorkerError("WorkerProtocolError")
    return real


class ResponseTooLarge(ValueError):
    """The upstream HTTP body exceeds the input limit."""


class InsecureRedirect(ValueError):
    """A redirect would leave HTTPS; its response must not become evidence."""


class _SkipRedirectBody(HTTPRedirectHandler):
    def http_error_302(self, req, fp, code, msg, headers):
        # urllib normally drains the entire redirect body before following Location.
        # Connections are short-lived here, so close it and keep urllib's redirect rules.
        fp.close()
        return super().http_error_302(req, BytesIO(), code, msg, headers)

    http_error_301 = http_error_303 = http_error_307 = http_error_308 = http_error_302

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # urllib also follows http:// and ftp://; status is only verified over HTTPS.
        # newurl is already resolved against the request URL here.
        if urlsplit(newurl).scheme.lower() != "https":
            raise InsecureRedirect()
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _read_text(url: str, timeout: float) -> str:
    request = Request(url, headers=web._BETARD_HEADERS)
    with build_opener(_SkipRedirectBody).open(request, timeout=timeout) as response:
        length = response.headers.get("Content-Length")
        expected = None
        if (
            length
            and length.isdecimal()
            and not response.headers.get("Transfer-Encoding")
        ):
            expected = int(length)
            if expected > MAX_RESPONSE_BYTES:
                raise ResponseTooLarge()
        body = response.read(MAX_RESPONSE_BYTES + 1)
        if len(body) > MAX_RESPONSE_BYTES:
            raise ResponseTooLarge()
        if expected is not None and len(body) < expected:
            raise IncompleteRead(body, expected - len(body))
        return body.decode(
            response.headers.get_content_charset() or "utf-8", errors="replace"
        )


def main() -> None:
    result: dict[str, object]
    try:
        room_id, timeout, operation = int(sys.argv[1]), float(sys.argv[2]), sys.argv[3]
        if room_id <= 0:
            raise ValueError("room_id 必须为正整数")
        if operation == "resolve":
            html = _read_text(web._MOBILE_URL.format(room_id=room_id), timeout)
            match = ROOM_ID_FIELD.search(html)
            resolved = int(match[1] or match[2]) if match else 0
            result = {"room_id": resolved if 1 <= resolved <= 2**63 - 1 else None}
        elif operation == "betard":
            payload = json.loads(_read_text(web._betard_url(room_id), timeout))
            room = web._parse_betard(room_id, payload)
            raw = {
                k: v
                for k, v in room.raw.items()
                if k in RAW_KEYS
                and type(v) in (int, str, bool, type(None))
                and len(str(v)) <= 64
            }
            result = asdict(
                replace(
                    room,
                    raw=raw,
                    title=room.title[:300],
                    owner=room.owner[:100],
                    category=None,
                    avatar_url=None,
                    cover_url=None,
                )
            )
        else:
            raise ValueError("Unknown HTTP worker operation")
    except (
        ApiError,
        HTTPException,
        OSError,
        ValueError,
        LookupError,
        RecursionError,
    ) as exc:
        result = {
            "error": type(exc).__name__,
            "code": exc.code if isinstance(exc, HTTPError) else None,
        }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
