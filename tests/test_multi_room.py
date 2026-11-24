import asyncio
import os
import signal
import sqlite3
import time
from contextlib import closing
from unittest.mock import AsyncMock, patch

import pytest
from conftest import ROOM, Scenario, config, text

from live_alert import service as service_module
from live_alert.cli import main
from live_alert.douyu import Observation
from live_alert.storage import connect, initialize

OTHER = ROOM + 1


def second_room(s, name=""):
    c = s.c.for_room(OTHER, name)
    db = connect(c.db)
    initialize(db, c)
    return Scenario(c, db)


def room_events(s, room):
    return [
        tuple(r)
        for r in s.db.execute(
            """SELECT q.kind,q.state FROM outbox q JOIN sessions s ON s.id=q.session_id
               WHERE s.room=? ORDER BY q.id""",
            (room,),
        )
    ]


def test_rooms_share_database_without_sharing_state_or_mail(scenario):
    a = scenario
    b = second_room(a)
    with closing(b.db):
        a.baseline()
        b.baseline()
        b.t = a.t
        a.live()
        b.feed("OFFLINE", 70)
        assert a.state.session == 1 and b.state.session is None
        # Room B's fresher OFFLINE evidence must not block room A's live mail.
        assert not b.worker()
        assert a.worker()
        assert room_events(a, ROOM) == [("LIVE", "SENT")]
        assert room_events(a, OTHER) == []
        b.live()
        assert b.worker()
        assert room_events(a, OTHER) == [("LIVE", "SENT")]
        a.offline()
        assert a.worker() and not b.worker()
        assert b.state.confirmed == "LIVE_CONFIRMED"
        assert room_events(a, ROOM) == [("LIVE", "SENT"), ("OFFLINE", "SENT")]


@pytest.mark.parametrize(
    "configured,owner,expected",
    [
        ("自定义名", "平台昵称", "自定义名"),
        ("", "平台昵称", "平台昵称"),
        ("", "", "斗鱼房间 10001"),
    ],
)
def test_mail_name_prefers_configured_then_platform(
    scenario, configured, owner, expected
):
    s = scenario
    s.c = s.c.for_room(ROOM, configured)
    s.state.c = s.c
    s.baseline()
    s.feed("LIVE", 60, owner=owner)
    s.feed("LIVE", 10, owner=owner)
    assert s.worker()
    assert s.sent[-1]["Subject"] == f"🎙️ {expected} 开播啦～"
    assert "标题: 未获取" in text(s.sent[-1]).splitlines()
    s.offline()
    assert s.worker()
    assert s.sent[-1]["Subject"] == f"🌙 {expected} 下播休息啦～"
    assert "标题: 未获取" in text(s.sent[-1]).splitlines()


class FakeDouyu:
    failing: frozenset[int] = frozenset()

    def __init__(self, c, slots=None):
        self.c = c

    async def events(self):
        await asyncio.Event().wait()
        yield "connected"

    async def sample(self):
        if self.c.room_id in self.failing:
            raise RuntimeError("room adapter broke")
        now = time.monotonic()
        return Observation("OFFLINE", "fixture", now, now, time.time())


def health(c):
    with closing(sqlite3.connect(c.db)) as db:
        return dict(db.execute("SELECT room, health FROM monitor"))


def test_one_failing_room_stops_every_room(tmp_path, monkeypatch):
    c = config(data_dir=str(tmp_path), rooms=f"{ROOM},{OTHER}")
    monkeypatch.setattr(service_module, "Douyu", FakeDouyu)
    monkeypatch.setattr(FakeDouyu, "failing", frozenset({OTHER}))
    with pytest.raises(RuntimeError, match="room adapter broke"):
        asyncio.run(asyncio.wait_for(service_module.run(c), 10))
    assert health(c) == {ROOM: "STOPPED", OTHER: "STOPPED"}


def test_sigterm_stops_all_rooms_gracefully(tmp_path, monkeypatch):
    c = config(data_dir=str(tmp_path), rooms=f"{ROOM},{OTHER}")
    monkeypatch.setattr(service_module, "Douyu", FakeDouyu)

    async def exercise():
        task = asyncio.create_task(service_module.run(c))
        for _ in range(100):
            await asyncio.sleep(0.05)
            if c.db.exists() and len(health(c)) == 2:
                break
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(task, 10)

    asyncio.run(exercise())
    assert health(c) == {ROOM: "STOPPED", OTHER: "STOPPED"}


def test_slots_admit_urgent_requests_first_and_survive_cancellation():
    from live_alert.service import Slots

    async def exercise():
        slots = Slots(1)
        await slots.acquire((2, 0))  # Held by a routine poll.
        order = []

        async def wait(name, rank):
            await slots.acquire(rank)
            order.append(name)
            slots.release()

        routine = [asyncio.create_task(wait(f"poll{i}", (2, i))) for i in range(3)]
        hint = asyncio.create_task(wait("hint", (1, 0)))
        await asyncio.sleep(0)
        cancelled = asyncio.create_task(wait("cancelled", (0, 1)))
        late = asyncio.create_task(wait("late_deadline", (0, 9)))
        confirm = asyncio.create_task(wait("confirm", (0, 5)))
        await asyncio.sleep(0)
        cancelled.cancel()
        slots.release()
        await asyncio.gather(*routine, hint, late, confirm, return_exceptions=True)
        await asyncio.gather(cancelled, return_exceptions=True)
        await slots.acquire((2, 0))  # The slot was returned, not leaked.
        return order

    assert asyncio.run(exercise()) == [
        "confirm",
        "late_deadline",
        "hint",
        "poll0",
        "poll1",
        "poll2",
    ]


class Saturating(FakeDouyu):
    """Always LIVE; every request holds its slot for 0.6 s."""

    async def sample(self):
        start = time.monotonic()
        await asyncio.sleep(0.6)
        return Observation("LIVE", "fixture", start, time.monotonic(), time.time())


def test_saturated_rooms_still_complete_confirmation(tmp_path, monkeypatch):
    # A reported production case scaled 1/10 in every duration: 20 rooms, 4 slots,
    # 0.6 s requests (6 s), 1 s confirmation gap (10 s), 0.8 s timeout (8 s), 3 s
    # polls (30 s), 0.2 s burst gap (2 s). Demand equals capacity. With a plain FIFO
    # the first round of samples takes 5 x 0.6 s, so each second sample starts
    # 2.4 s after the first ended, beyond the 1.8 s continuity limit, and every
    # room stays BASELINING forever.
    monkeypatch.setattr(service_module, "Douyu", Saturating)
    monkeypatch.setattr(service_module, "BURST_GAP_SECONDS", 0.2)
    c = config(
        data_dir=str(tmp_path),
        rooms=",".join(str(ROOM + i) for i in range(20)),
        poll_interval_seconds=3,
        live_confirm_gap_seconds=1,
        http_timeout_seconds=0.8,
    )

    async def exercise():
        task = asyncio.create_task(service_module.run(c))
        for _ in range(150):
            await asyncio.sleep(0.1)
            confirmed = [
                s for s in health_rows(c, "confirmed") if s == "LIVE_CONFIRMED"
            ]
            if len(confirmed) == 20:
                break
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(task, 10)
        return health_rows(c, "confirmed")

    assert asyncio.run(exercise()) == ["LIVE_CONFIRMED"] * 20


def health_rows(c, column):
    if not c.db.exists():
        return []
    with closing(sqlite3.connect(c.db)) as db:
        return [r[0] for r in db.execute(f"SELECT {column} FROM monitor ORDER BY room")]


@pytest.mark.parametrize("real,hint", [(ROOM, False), (OTHER, True)])
def test_resolve_room_reports_real_id(capsys, real, hint):
    with (
        patch.dict("os.environ", {}, clear=True),
        patch("live_alert.cli.resolve_room_id", AsyncMock(return_value=real)),
    ):
        assert main(["resolve-room", str(ROOM)]) == 0
    out, err = capsys.readouterr()
    assert out.strip() == str(real)
    assert ("靓号" in err) is hint


def test_cli_room_filter_rejects_unknown_room(tmp_path, capsys):
    env = {"DATA_DIR": str(tmp_path), "DOUYU_ROOMS": str(ROOM)}
    with patch.dict("os.environ", env, clear=True):
        assert main(["status", "--room", str(OTHER)]) == 2
    assert "不在 DOUYU_ROOMS" in capsys.readouterr().err


class Flooding(FakeDouyu):
    """Rooms below `flooded` get a status hint every 50 ms and are always LIVE.
    The other rooms are quiet: OFFLINE until `switch_at`, then LIVE, so only a
    routine timer poll can discover the change."""

    flooded = ROOM + 4
    switch_at = 0.0

    def quiet(self):
        return self.c.room_id >= self.flooded

    async def events(self):
        if not self.quiet():
            while True:
                await asyncio.sleep(0.05)
                yield "status_hint"
        await asyncio.Event().wait()
        yield "connected"

    async def sample(self):
        start = time.monotonic()
        await asyncio.sleep(0.3)
        live = not self.quiet() or start >= self.switch_at
        state = "LIVE" if live else "OFFLINE"
        return Observation(state, "fixture", start, time.monotonic(), time.time())


def test_hint_flood_in_some_rooms_cannot_starve_quiet_rooms(tmp_path, monkeypatch):
    # Scaled 1/10 (poll 3 s, gap 1 s, timeout 0.8 s, burst gap 0.2 s, 0.3 s
    # requests) with 2 shared slots. Four rooms are flooded with direction-less
    # hints and keep both slots busy. Four quiet rooms finish their OFFLINE baseline,
    # go live after 4 s, and can only notice through timer polls (no open window,
    # no hint). When every hint outranked timer polls, those polls never ran and the
    # quiet rooms stayed OFFLINE.
    monkeypatch.setattr(service_module, "Douyu", Flooding)
    monkeypatch.setattr(service_module, "BURST_GAP_SECONDS", 0.2)
    monkeypatch.setattr(service_module, "HTTP_WORKER_SLOTS", 2)
    monkeypatch.setattr(Flooding, "switch_at", time.monotonic() + 4)
    c = config(
        data_dir=str(tmp_path),
        rooms=",".join(str(ROOM + i) for i in range(8)),
        poll_interval_seconds=3,
        live_confirm_gap_seconds=1,
        http_timeout_seconds=0.8,
    )

    async def exercise():
        task = asyncio.create_task(service_module.run(c))
        await asyncio.sleep(3.5)
        assert health_rows(c, "confirmed")[4:] == ["OFFLINE"] * 4  # Baselines done.
        for _ in range(90):
            await asyncio.sleep(0.1)
            if health_rows(c, "confirmed").count("LIVE_CONFIRMED") == 8:
                break
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(task, 10)
        return health_rows(c, "confirmed")

    assert asyncio.run(exercise()) == ["LIVE_CONFIRMED"] * 8


def test_hint_priority_is_rate_limited_per_room(scenario):
    s = scenario
    service = service_module.Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
    assert service.rank("status_hint")[0] == 1
    assert service.rank("status_hint")[0] == 2  # Same poll interval: routine.
    s.t += s.c.poll_interval_seconds
    assert service.rank("possible_live")[0] == 1
    assert service.rank("timer")[0] == 2


def test_mail_runs_on_dedicated_pool_and_stops_claiming(scenario):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from live_alert.mail import process_one

    s = scenario
    with ThreadPoolExecutor(1, thread_name_prefix="mail") as pool:
        service = service_module.Service(
            s.c, s.db, None, lambda: s.t, lambda: s.t, mail_executor=pool
        )
        name = asyncio.run(service.mail_io(lambda: threading.current_thread().name))
    assert name.startswith("mail")
    s.baseline()
    s.live()
    # Queued before shutdown, run after it: no new SMTP attempt may start.
    assert not process_one(
        s.c, lambda *a: pytest.fail("sent"), lambda: s.t, lambda: True
    )
    assert s.events() == [("LIVE", "PENDING")]
