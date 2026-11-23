import asyncio
from contextlib import suppress
from dataclasses import replace
from unittest.mock import patch

import pytest

from live_alert.douyu import Observation
from live_alert.service import Service


@pytest.mark.parametrize("use_rss", [True, False])
def test_rss_and_poll_through_sampler_sqlite_and_smtp(scenario, use_rss):
    s = scenario
    s.baseline()
    start = s.t
    requests = []
    kinds = asyncio.Queue()

    class FakeDouyu:
        async def events(self):
            while True:
                yield await kinds.get()
                kinds.task_done()

        async def sample(self):
            t = s.t - start
            requests.append(t)
            first = 3 if use_rss else 60
            at_offline = 24 if use_rss else 130
            value = "LIVE" if first <= t < at_offline else "OFFLINE"
            if t >= at_offline + 120:
                service.stop.set()
            return Observation(value, "fixture", s.t, s.t, s.t)

    trigger = [(3, "possible_live"), (24, "possible_offline")] if use_rss else []
    service = Service(s.c, s.db, FakeDouyu(), lambda: s.t, lambda: s.t)

    async def wait(delay):
        while s.worker():
            pass
        if trigger and start + trigger[0][0] <= s.t + delay:
            t, kind = trigger.pop(0)
            s.t = start + t
            for _ in range(100):
                await kinds.put(kind)
            await kinds.join()
        else:
            s.t += delay

    async def exercise():
        watcher = asyncio.create_task(service.watch())
        try:
            with (
                patch.object(service, "wait_for_sample", wait),
                patch("random.uniform", return_value=1),
            ):
                await service.sample_loop()
        finally:
            watcher.cancel()
            with suppress(asyncio.CancelledError):
                await watcher
        while s.worker():
            pass

    asyncio.run(exercise())
    assert requests == (
        [0, 3, 13, 24, 54, 84, 114, 144]
        if use_rss
        else [0, 30, 60, 70, 100, 130, 160, 190, 220, 250]
    )
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT")]


def test_health_and_backoff_despite_rss(scenario):
    s = scenario
    start = s.t
    times = []

    class Broken:
        async def sample(self):
            times.append(s.t - start)
            # Notify during the HTTP request, like a concurrent RSS burst.
            service.event("connected")
            if len(times) == 5:
                service.stop.set()
            return Observation("UNKNOWN", "http_429", s.t, s.t, s.t)

    service = Service(s.c, s.db, Broken(), lambda: s.t, lambda: s.t)

    async def wait(delay):
        s.t += delay

    with patch.object(service, "wait_for_sample", wait):
        asyncio.run(service.sample_loop())
    assert times == [0, 60, 120, 180, 240]
    s.t += 601
    service.tick()
    assert service.health == "DEGRADED"
    assert not s.events()


@pytest.mark.parametrize("recent_replay_hint", [False, True])
def test_sampler_recovers_from_persisted_replay_without_a_new_start(
    scenario, recent_replay_hint
):
    s = scenario
    s.baseline()
    s.feed("REPLAY", 5, started_at=950)
    s.t += 660  # Old deployment stopped accepting evidence more than 10 minutes ago.
    start = s.t
    states = []

    class ReplayRoom:
        async def sample(self):
            if s.t - start >= 90:
                service.stop.set()
            return Observation(
                "LIVE", "live_without_loop", s.t, s.t, s.t, started_at=950
            )

    service = Service(s.c, s.db, ReplayRoom(), lambda: s.t, lambda: s.t)
    service.event("connected")
    service.tick()
    assert service.health == "DEGRADED"
    if recent_replay_hint:
        service.event("replay")

    async def wait(delay):
        row = s.db.execute("SELECT health,mail_ready FROM monitor").fetchone()
        states.append(tuple(row))
        s.t += delay

    with (
        patch.object(service, "wait_for_sample", wait),
        patch("random.uniform", return_value=1),
    ):
        asyncio.run(service.sample_loop())
    assert states[0] == (("DEGRADED", 0) if recent_replay_hint else ("OK", 1))
    assert states[-1] == ("OK", 1)
    assert service.state.confirmed == "LIVE_CONFIRMED"
    assert s.events() == [("LIVE", "SUPPRESSED_BASELINE")] and not s.worker()


def test_sampler_prioritizes_rechecking_live_during_pending_end(scenario):
    s = scenario
    s.baseline()
    s.live()
    assert s.worker()
    start = s.t
    reads = []

    class Room:
        async def sample(self):
            elapsed = s.t - start
            reads.append(elapsed)
            state = {0: "REPLAY", 30: "OFFLINE"}.get(elapsed, "LIVE")
            if elapsed >= 70:
                service.stop.set()
            return Observation(state, "fixture", s.t, s.t, s.t, started_at=900)

    service = Service(s.c, s.db, Room(), lambda: s.t, lambda: s.t)

    async def wait(delay):
        if s.t == start + 60:
            assert s.db.execute("SELECT mail_ready FROM monitor").fetchone()[0] == 0
        s.t += delay
        if s.t == start + 70:
            assert service.rank("timer") == (0, start + 78)

    with (
        patch.object(service, "wait_for_sample", wait),
        patch("random.uniform", return_value=1),
    ):
        asyncio.run(service.sample_loop())
    assert reads == [0, 30, 60, 70]
    assert service.state.phase == "LIVE_CONFIRMED"
    assert s.db.execute("SELECT mail_ready FROM monitor").fetchone()[0] == 1
    assert s.events() == [("LIVE", "SENT")]


def test_replay_burst_preserves_nonlive_baseline_and_cancels_live_candidate(scenario):
    s = scenario
    service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
    s.state = service.state
    s.feed("REPLAY")
    for _ in range(100):
        service.event("replay")
    s.feed("REPLAY", 10)
    assert s.state.phase == "OFFLINE"
    s.feed("LIVE", 5)
    service.event("replay")
    s.feed("LIVE", 10)
    assert not s.events()


@pytest.mark.parametrize("closing", [False, True])
def test_opposite_rss_during_decisive_http_cannot_queue_email(scenario, closing):
    s = scenario
    s.baseline()
    if closing:
        s.live()
        s.worker()

    class Conflict:
        async def sample(self):
            service.event("possible_live" if closing else "possible_offline")
            service.stop.set()
            return Observation(
                "OFFLINE" if closing else "LIVE", "fixture", s.t, s.t, s.t
            )

    service = Service(s.c, s.db, Conflict(), lambda: s.t, lambda: s.t)
    s.state = service.state
    if closing:
        s.feed("OFFLINE", 10)
        s.feed("REPLAY", 60)
        s.t += 60
    else:
        s.feed("OFFLINE", 1)
        s.feed("LIVE", 30)
        s.t += 10
    before = s.events()
    asyncio.run(service.sample_loop())
    assert s.events() == before


@pytest.mark.parametrize("backoff", [5, 30])
@pytest.mark.parametrize("pattern", ["flip_flop", "strike_reset"])
def test_injected_rss_hints_cannot_block_live_confirmation(scenario, pattern, backoff):
    s = scenario
    s.baseline()
    s.t += 10  # The broadcast starts after the last offline evidence.
    c = replace(s.c, backoff_initial_seconds=backoff)
    start, stop_at = s.t, s.t + 600

    class Room:
        async def sample(self):
            # An on-path attacker injects hints while each HTTP request is in flight.
            if pattern == "flip_flop":
                service.event("possible_offline")
                service.event("possible_live")
            else:  # One agreeing hint whenever trust is lost, then contradict again.
                service.event(
                    "possible_live" if service.rss_strikes >= 2 else "possible_offline"
                )
            if s.t >= stop_at:
                service.stop.set()
            return Observation("LIVE", "fixture", s.t, s.t, s.t, started_at=start)

    service = Service(c, s.db, Room(), lambda: s.t, lambda: s.t)

    async def wait(delay):
        s.t += delay

    with (
        patch.object(service, "wait_for_sample", wait),
        patch("random.uniform", return_value=1),
    ):
        asyncio.run(service.sample_loop())
    assert s.events() == [("LIVE", "PENDING")]


def test_failed_mail_loop_still_marks_room_stopped(scenario):
    s = scenario
    service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)

    async def broken():
        raise RuntimeError("mail failed")

    async def idle():
        await asyncio.Event().wait()

    with (
        patch.object(service, "mail_loop", broken),
        patch.object(service, "watch", idle),
        patch.object(service, "sample_loop", idle),
        patch.object(service, "heartbeat", idle),
        pytest.raises(RuntimeError, match="mail failed"),
    ):
        asyncio.run(service._supervise())
    assert s.db.execute("SELECT health FROM monitor").fetchone()[0] == "STOPPED"
