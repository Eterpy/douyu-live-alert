import time
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from urllib.error import HTTPError
from uuid import uuid4

from aiodouyu import (
    EVENT_CONNECTED,
    EVENT_DISCONNECTED,
    ApiError,
    DanmakuClient,
)
from aiodouyu.web import RoomInfo

from .config import Config
from .http_worker import WorkerError
from .http_worker import bounded_fetch as fetch_room

# Platform show_time may lead the host clock slightly; tolerate it as trusted.
CLOCK_SKEW_SECONDS = 120


def clean(text: object, limit: int) -> str:
    """Upstream text, minus control characters and lone surrogates SQLite rejects."""
    return "".join(ch for ch in str(text) if ch.isprintable())[:limit]


@dataclass(frozen=True)
class Observation:
    state: str
    reason: str
    start: float  # monotonic request start
    end: float  # monotonic response completion
    utc: float  # completion epoch, for persistence
    raw: dict = field(default_factory=dict)
    title: str = ""
    started_at: int | None = None
    owner: str = ""
    id: str = field(default_factory=lambda: uuid4().hex)


def normalize(room: RoomInfo, expected: int) -> tuple[str, str]:
    raw = room.raw

    def matches(key: str, *allowed: int) -> bool:
        value = raw.get(key)
        return type(value) in (int, str) and str(value) in {str(n) for n in allowed}

    if room.source != "betard" or room.is_loop is None:
        return "UNKNOWN", "unverified_source_or_loop"
    if not matches("room_id", expected) or room.room_id != expected:
        return "UNKNOWN", "room_mismatch"
    if not matches("show_status", 1, 2) or not matches("videoLoop", 0, 1):
        return "UNKNOWN", "invalid_status_fields"
    if room.is_live != (
        matches("show_status", 1) and matches("videoLoop", 0)
    ) or room.is_loop != (matches("show_status", 1) and matches("videoLoop", 1)):
        return "UNKNOWN", "conflicting_normalization"
    if matches("show_status", 2):
        if matches("videoLoop", 1):
            return "UNKNOWN", "conflicting_fields"
        return "OFFLINE", "show_status_2"
    if matches("videoLoop", 1):
        return "REPLAY", "video_loop"
    return "LIVE", "live_without_loop"


class Douyu:
    def __init__(self, config: Config):
        self.c = config

    async def sample(self) -> Observation:
        start = time.monotonic()
        try:
            room = await fetch_room(
                self.c.room_id, source="betard", timeout=self.c.http_timeout_seconds
            )
            end, utc = time.monotonic(), time.time()
            state, reason = normalize(room, self.c.room_id)
            # urllib's timeout applies per socket operation, not to the whole read.
            if end - start > self.c.http_timeout_seconds:
                state, reason = "UNKNOWN", "request_exceeded_timeout"
            raw = {
                k: room.raw[k]
                for k in ("room_id", "show_status", "status", "videoLoop")
                if k in room.raw and type(room.raw[k]) in (str, int, bool, type(None))
            }
            started = room.started_at
            raw_start = room.raw.get("show_time")
            # Replay show_time is kept too: it identifies stale live-looking snapshots.
            if not (
                state in ("LIVE", "REPLAY")
                and type(started) is int
                and 0 < started <= utc + CLOCK_SKEW_SECONDS
                and type(raw_start) in (int, str)
                and str(raw_start) == str(started)
            ):
                started = None
            return Observation(
                state,
                reason,
                start,
                end,
                utc,
                raw,
                clean(room.title, 300),
                started,
                clean(room.owner, 100),
            )
        except (ApiError, OSError, ValueError) as exc:
            cause = exc.__cause__
            reason = (
                f"http_{cause.code}"
                if isinstance(cause, HTTPError)
                else exc.kind
                if isinstance(exc, WorkerError)
                else type(cause or exc).__name__
            )
            return Observation("UNKNOWN", reason, start, time.monotonic(), time.time())

    async def events(self) -> AsyncGenerator[str, None]:
        async with DanmakuClient(
            self.c.room_id,
            transport=self.c.transport,
            types={"rss"},
            emit_connection_events=True,
        ) as client:
            async for msg in client:
                kind = msg.get("type")
                if kind == EVENT_CONNECTED:
                    yield "connected"
                elif kind == EVENT_DISCONNECTED:
                    yield "disconnected"
                elif kind == "rss" and str(msg.get("rid", self.c.room_id)) == str(
                    self.c.room_id
                ):
                    if msg.get("ss") == "0":
                        yield "possible_offline"
                    elif msg.get("ss") == "1":
                        loop = msg.get("ivl")
                        if loop == "1":
                            yield "replay"
                        elif loop == "0":
                            yield "possible_live"
                        else:
                            yield "status_hint"
