import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock, patch
from urllib.error import HTTPError

import pytest
from aiodouyu import DanmakuClient, fetch_room, web
from conftest import config

from live_alert.douyu import Douyu, normalize

ROOM = 10001


def parsed(raw):
    with patch.object(web, "_http_get_json", return_value={"room": raw}):
        return asyncio.run(fetch_room(ROOM, source="betard"))


@pytest.mark.parametrize(
    "fields, upstream, strict",
    [
        ({"show_status": 1, "videoLoop": 0}, (True, False), "LIVE"),
        ({"show_status": "1", "videoLoop": "0"}, (True, False), "LIVE"),
        ({"show_status": 1, "videoLoop": 1}, (False, True), "REPLAY"),
        ({"show_status": 2, "status": "1", "videoLoop": 0}, (False, False), "OFFLINE"),
        ({"show_status": 1}, (True, False), "UNKNOWN"),
        ({"show_status": 1, "videoLoop": None}, (True, False), "UNKNOWN"),
        ({"show_status": 1, "videoLoop": False}, (True, False), "UNKNOWN"),
        ({"show_status": 1, "videoLoop": 0.5}, (True, False), "UNKNOWN"),
        ({"show_status": 1, "videoLoop": ""}, (True, False), "UNKNOWN"),
        ({"show_status": 2, "videoLoop": 1}, (False, False), "UNKNOWN"),
        ({"status": "1", "videoLoop": 0}, (False, False), "UNKNOWN"),
    ],
)
def test_real_published_parser(fields, upstream, strict):
    room = parsed({"room_id": ROOM, **fields})
    assert (room.is_live, room.is_loop) == upstream
    assert normalize(room, ROOM)[0] == strict


@pytest.mark.parametrize("raw", [{}, {"room_id": 42}, {"room_id": True}])
def test_original_room_id_required(raw):
    room = parsed(dict(show_status=1, videoLoop=0, **raw))
    assert normalize(room, ROOM)[0] == "UNKNOWN"


def test_open_cannot_confirm():
    room = parsed({"room_id": ROOM, "show_status": 1, "videoLoop": 0})
    for bad in (replace(room, source="open"), replace(room, is_loop=None)):
        assert normalize(bad, ROOM)[0] == "UNKNOWN"


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError(),
        ValueError("invalid JSON"),
        *[
            HTTPError("https://example.invalid", n, "error", {}, None)
            for n in (403, 429, 502)
        ],
    ],
)
def test_http_exceptions(error):
    with patch.object(web, "_http_get_json", side_effect=error) as request:
        o = asyncio.run(Douyu(config()).sample())
    assert o.state == "UNKNOWN"
    assert request.call_count == 1
    assert "/betard/" in request.call_args.args[0]
    if isinstance(error, HTTPError):
        assert o.reason == f"http_{error.code}"


def test_client_filters_rss_and_reconnects():
    # Feed decoded packets at the published TCP client's read boundary.
    async def exercise():
        messages = AsyncMock(
            side_effect=[
                {"type": "chatmsg"},
                {"type": "rss", "ss": "1", "ivl": "1"},
                {"type": "rss", "ss": "0"},
                OSError(),
                {"type": "rss", "ss": "1"},
                {"type": "rss", "ss": "1", "ivl": "0"},
            ]
        )
        with (
            patch.object(DanmakuClient, "_connect", new_callable=AsyncMock) as connects,
            patch.object(DanmakuClient, "_read_message", messages),
            patch.object(DanmakuClient, "_backoff_wait", AsyncMock(return_value=False)),
        ):
            gen = Douyu(config()).events()
            try:
                assert [await anext(gen) for _ in range(7)] == [
                    "connected",
                    "replay",
                    "possible_offline",
                    "disconnected",
                    "connected",
                    "status_hint",
                    "possible_live",
                ]
                assert connects.await_count == 2
            finally:
                await gen.aclose()

    asyncio.run(exercise())


@pytest.mark.parametrize("missing", ["videoLoop", "room_id", "show_status"])
def test_missing_raw_field_breaks_both_confirmation_windows(scenario, missing):
    # Replace only the network read. Exercise the installed wheel, adapter,
    # SQLite state machine and delivery worker in their normal order.
    s = scenario
    adapter = Douyu(s.c)

    def probe(show, loop, delta, *, incomplete=False):
        s.t += delta
        raw = {"room_id": ROOM, "show_status": show, "videoLoop": loop}
        if incomplete:
            del raw[missing]
        with (
            patch.object(web, "_http_get_json", return_value={"room": raw}),
            patch("live_alert.douyu.time") as clock,
        ):
            clock.monotonic.return_value = clock.time.return_value = s.t
            observation = asyncio.run(adapter.sample())
        s.state.observe(observation, "timer", s.t)
        if incomplete:
            assert observation.state == "UNKNOWN"
            assert s.state.window is None

    s.baseline()
    probe(1, 0, 10)
    probe(1, 0, 10, incomplete=True)
    probe(1, 0, 10)
    assert not s.worker()
    probe(1, 0, 10)
    assert s.worker()
    probe(2, 0, 10)
    probe(1, 1, 60)
    probe(2, 0, 60, incomplete=True)
    probe(2, 0, 5)
    probe(1, 1, 60)
    assert not s.worker() and s.state.session == 1
    probe(2, 0, 60)
    assert s.worker() and s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT")]


@pytest.mark.parametrize("show,loop", [(1, 0), (2, 0), (1, 1)])
def test_slow_http_data_is_ineligible_even_if_fields_are_valid(show, loop):
    with (
        patch.object(
            web,
            "_http_get_json",
            return_value={
                "room": {"room_id": ROOM, "show_status": show, "videoLoop": loop}
            },
        ),
        patch("live_alert.douyu.time") as clock,
    ):
        clock.monotonic.side_effect = [1, 151]
        clock.time.return_value = 1000
        o = asyncio.run(Douyu(config()).sample())
    assert (o.state, o.reason) == ("UNKNOWN", "request_exceeded_timeout")


@pytest.mark.parametrize(
    "raw_start,expected", [(10**30, None), (False, None), (100.5, None), ("100", 100)]
)
def test_optional_start_time_cannot_poison_session_identity(raw_start, expected):
    with patch.object(
        web,
        "_http_get_json",
        return_value={
            "room": {
                "room_id": ROOM,
                "show_status": 1,
                "videoLoop": 0,
                "show_time": raw_start,
            }
        },
    ):
        o = asyncio.run(Douyu(config()).sample())
    assert o.state == "LIVE" and o.started_at == expected
