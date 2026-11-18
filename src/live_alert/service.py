import asyncio
import functools
import heapq
import itertools
import logging
import os
import random
import signal
import sqlite3
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, aclosing, closing

from .config import Config
from .douyu import Douyu, Observation
from .mail import process_one
from .smtp_worker import WarmSMTP
from .state import MonitorState
from .storage import audit, connect, initialize, secure_database_files, single_instance

# Compose does not restart unhealthy containers, so a stuck loop must exit itself.
LOOP_STALL_SECONDS = 120
# After this many RSS hints in a row are disproved by HTTP, RSS only wakes sampling.
RSS_STRIKES = 2
# Minimum spacing between request starts of one room during RSS event bursts.
BURST_GAP_SECONDS = 2


class Slots:
    """Bounded HTTP concurrency shared by all rooms, granted lowest rank first.

    A sample that continues an open confirmation window ranks by the deadline after
    which that window would restart, ahead of realtime/startup checks and routine
    polls. Its wait is then at most one request, so saturation cannot keep a room
    from ever confirming."""

    def __init__(self, size: int):
        self.free = size
        self.waiters: list[tuple[tuple[int, float], int, asyncio.Future]] = []
        self.order = itertools.count()

    async def acquire(self, rank: tuple[int, float]) -> None:
        if self.free and not self.waiters:
            self.free -= 1
            return
        future = asyncio.get_running_loop().create_future()
        heapq.heappush(self.waiters, (rank, next(self.order), future))
        try:
            await future
        except asyncio.CancelledError:
            if future.done() and not future.cancelled():
                self.release()  # Granted just as we were cancelled.
            raise

    def release(self) -> None:
        while self.waiters:
            _, _, future = heapq.heappop(self.waiters)
            if not future.done():
                future.set_result(None)
                return
        self.free += 1


def loop_watchdog(
    c: Config,
    alive: Callable[[], float],
    halt: threading.Event,
    clock: Callable[[], float] = time.monotonic,
    exit: Callable[[int], object] = os._exit,
    interval: float = 10,
) -> None:
    """Runs in a thread: a coroutine watchdog cannot fire while the loop is stuck."""
    while not halt.wait(interval):
        stalled = clock() - alive()
        if stalled > LOOP_STALL_SECONDS:
            audit(
                c, "watchdog", "RUNNING", "EXIT", "event_loop_stalled", seconds=stalled
            )
            # Same recovery path as SIGKILL: PREPARING/SUBMITTING resolve on restart.
            exit(1)
            return


class Service:
    def __init__(
        self,
        c: Config,
        db: sqlite3.Connection,
        adapter: Douyu,
        monotonic: Callable[[], float] = time.monotonic,
        utc: Callable[[], float] = time.time,
        slots: Slots | None = None,
        mail_executor: ThreadPoolExecutor | None = None,
        preconnects: threading.Semaphore | None = None,
    ):
        self.c, self.db, self.adapter, self.slots = c, db, adapter, slots
        # Mail runs on its own small pool: SMTP attempts last up to a minute and
        # must not hold the default executor that asyncio uses for DNS lookups.
        self.mail_executor, self.preconnects = mail_executor, preconnects
        self.halting = threading.Event()  # read by mail threads
        self.monotonic, self.utc = monotonic, utc
        self.state = MonitorState(db, c, utc(), monotonic)
        self.wake = asyncio.Event()
        self.stop = asyncio.Event()
        self.mail_wake = asyncio.Event()
        self.trigger = "startup"
        self.live_evidence_epoch = 0
        self.nonlive_evidence_epoch = 0
        self.disconnected_since: float | None = self.monotonic()
        self.last_progress = self.monotonic()
        self.queued = False  # Waiting for an HTTP slot is not a sampler stall.
        # "live"/"nonlive" hints awaiting an HTTP verdict. All of them, not just the
        # latest: a disproved hint followed by an agreeing one must still count.
        self.rss_claims: set[str] = set()
        self.rss_strikes = 0
        self.last_hint_slot = float("-inf")
        self.last_http = self.monotonic()
        if self.state.last_valid is not None:
            self.last_http -= max(0, utc() - self.state.last_valid)
        self.health = "STARTING"
        self.last_prune = float("-inf")
        # Real clock on purpose: read by the watchdog thread, not the injected one.
        self.loop_alive = time.monotonic()

    def event(self, kind: str) -> None:
        self.mail_wake.set()
        if kind == "disconnected":
            self.disconnected_since = self.monotonic()
            # RSS availability is independent of valid HTTP evidence.
            return
        if kind == "connected":
            self.disconnected_since = None
        if kind in ("replay", "possible_offline", "possible_live"):
            self.rss_claims.add("live" if kind == "possible_live" else "nonlive")
            if self.rss_strikes >= RSS_STRIKES:
                # Repeatedly wrong (or injected) hints may only trigger a check.
                self.wake_for(kind)
                return
        if (
            kind in ("replay", "possible_offline")
            and self.state.confirmed == "LIVE_CONFIRMED"
        ) or (kind == "possible_live" and self.state.confirmed != "LIVE_CONFIRMED"):
            with self.db:
                self.db.execute(
                    "UPDATE monitor SET mail_ready=0 WHERE room=?", (self.c.room_id,)
                )
        if kind in ("replay", "possible_offline"):
            self.live_evidence_epoch += 1
            if kind == "replay":
                self.state.note_replay(self.utc())
            if self.state.window and self.state.window.kind == "LIVE":
                self.state.interrupt(kind)
            elif self.state.phase in ("OFFLINE_CANDIDATE", "OFFLINE"):
                return
            if kind == "replay" and self.state.confirmed != "LIVE_CONFIRMED":
                return
        elif kind == "possible_live":
            self.nonlive_evidence_epoch += 1
            if self.state.window and self.state.window.kind == "NONLIVE":
                self.state.interrupt(kind)
            elif self.state.phase in ("LIVE_CANDIDATE", "LIVE_CONFIRMED"):
                return
        self.wake_for(kind)

    def wake_for(self, kind: str) -> None:
        if self.wake.is_set() and self.trigger == kind:
            return
        audit(
            self.c,
            "rss",
            self.state.phase,
            self.state.phase,
            kind,
            received_at=self.utc(),
        )
        self.trigger = kind
        self.wake.set()

    async def watch(self) -> None:
        async with aclosing(self.adapter.events()) as events:
            async for kind in events:
                self.event(kind)
        if not self.stop.is_set():
            raise RuntimeError("斗鱼事件流意外结束")

    async def wait_for_sample(self, delay: float) -> None:
        if self.wake.is_set():
            await asyncio.sleep(delay)
            return
        try:
            await asyncio.wait_for(self.wake.wait(), delay)
        except TimeoutError:
            pass

    async def sample_loop(self) -> None:
        c = self.c
        due = gate = self.monotonic()
        failures = 0
        while not self.stop.is_set():
            now = self.monotonic()
            if self.wake.is_set():
                due = min(due, now)
            delay = max(due, gate) - now
            if delay > 0:
                await self.wait_for_sample(delay)
                continue
            trigger = (
                self.trigger
                if self.wake.is_set() or self.trigger == "startup"
                else "timer"
            )
            self.trigger = "timer"
            self.wake.clear()
            if self.slots is not None:
                self.queued = True
                try:
                    await self.slots.acquire(self.rank(trigger))
                finally:
                    self.queued = False
            try:
                self.last_progress = self.monotonic()
                live_epoch = self.live_evidence_epoch
                nonlive_epoch = self.nonlive_evidence_epoch
                o = await self.adapter.sample()
            finally:
                if self.slots is not None:
                    self.slots.release()
            now = self.monotonic()
            self.last_progress = now
            superseded = (
                o.state == "LIVE" and live_epoch != self.live_evidence_epoch
            ) or (
                o.state in ("OFFLINE", "REPLAY")
                and nonlive_epoch != self.nonlive_evidence_epoch
            )
            self.judge_rss(o, superseded)
            if superseded and self.rss_strikes < RSS_STRIKES:
                audit(
                    c,
                    trigger,
                    self.state.phase,
                    self.state.phase,
                    "superseded_http",
                    observation=o.id,
                )
                due = now
                gate = now + c.backoff_initial_seconds
                continue
            audit(
                c,
                "http_timing",
                self.state.phase,
                o.state,
                o.reason,
                observation=o.id,
                trigger=trigger,
                requested_at=o.utc - (o.end - o.start),
                completed_at=o.utc,
                elapsed_seconds=o.end - o.start,
            )
            usable = self.state.observe(o, trigger)
            self.mail_wake.set()
            now = self.monotonic()
            if o.state == "UNKNOWN":
                failures += 1
                pause = min(
                    c.backoff_initial_seconds * 2 ** min(failures - 1, 20),
                    c.backoff_max_seconds,
                )
                if o.reason == "http_429":
                    pause = max(pause, c.poll_interval_seconds, 60)
                gate = due = now + pause
                audit(
                    c,
                    "http",
                    self.state.phase,
                    self.state.phase,
                    "backoff",
                    seconds=pause,
                )
            else:
                failures = 0
                if usable:
                    self.last_http = o.end
                gate = o.start + BURST_GAP_SECONDS
                due = now + c.poll_interval_seconds * random.uniform(0.97, 1.03)
                candidate = self.state.next_check()
                if candidate is not None:
                    due = min(due, candidate)
                    if (
                        self.state.phase == "OFFLINE_CANDIDATE"
                        and now - o.end > c.offline_final_evidence_max_age_seconds
                    ):
                        due = now
                # Merge hints received during the same request. A contradictory RSS
                # still schedules a fresh check (including a missed state transition).
                if self.trigger == trigger or self.trigger == "connected":
                    self.wake.clear()
            self.tick()

    def judge_rss(self, o: Observation, superseded: bool) -> None:
        """Count RSS hints contradicted by HTTP, including the responses they
        superseded; otherwise a hint per request would never be counted."""
        if not self.rss_claims or o.state not in ("LIVE", "OFFLINE", "REPLAY"):
            return
        verdict = "live" if o.state == "LIVE" else "nonlive"
        if self.rss_claims == {verdict}:
            self.rss_strikes = 0
            self.rss_claims.clear()
            return
        self.rss_strikes += 1
        if self.rss_strikes == RSS_STRIKES:
            audit(self.c, "rss", self.state.phase, self.state.phase, "rss_untrusted")
        if not superseded:
            # A superseded response predates the hint; the next one may still agree.
            self.rss_claims.clear()

    def rank(self, trigger: str) -> tuple[int, float]:
        now = self.monotonic()
        w, due = self.state.window, self.state.next_check()
        # Only the sample that will count toward an open window outranks others;
        # earlier hint-driven samples would be ignored by the window anyway.
        if w is not None and due is not None and now >= due:
            cadence = (
                self.c.offline_confirm_seconds
                if self.state.phase == "OFFLINE_CANDIDATE"
                else self.c.live_confirm_gap_seconds
            )
            # The continuity limit in MonitorState._advance.
            return 0, w.last.end + cadence + self.c.http_timeout_seconds
        # At most one hint-priority request per room per poll interval: a room
        # flooded with hints must not starve other rooms' routine polls.
        if (
            trigger != "timer"
            and now - self.last_hint_slot >= self.c.poll_interval_seconds
        ):
            self.last_hint_slot = now
            return 1, now
        return 2, now

    def tick(self) -> None:
        now, utc = self.monotonic(), self.utc()
        degraded = now - self.last_http > 600 or (
            self.disconnected_since is not None and now - self.disconnected_since > 600
        )
        health = "DEGRADED" if degraded else "OK"
        if health != self.health:
            audit(self.c, "health", self.health, health, "upstream_health")
            self.health = health
        with self.db:
            self.db.execute(
                "UPDATE monitor SET heartbeat=?, health=? WHERE room=?",
                (utc, health, self.c.room_id),
            )
            if now - self.last_prune >= 86400:
                self.db.execute(
                    "DELETE FROM observations WHERE utc < ?",
                    (utc - self.c.observation_retention_days * 86400,),
                )
                self.last_prune = now

    async def heartbeat(self) -> None:
        while not self.stop.is_set():
            limit = (
                max(self.c.backoff_max_seconds, self.c.poll_interval_seconds)
                + self.c.http_timeout_seconds
                + 30
            )
            if not self.queued and self.monotonic() - self.last_progress > limit:
                raise RuntimeError("HTTP sampler stopped making progress")
            self.loop_alive = time.monotonic()
            self.tick()
            await asyncio.sleep(10)

    async def mail_io(self, fn, *args):
        # Cancelling to_thread does not stop its thread. Drain the bounded operation
        # before closing/reusing its process or releasing the instance lock.
        loop = asyncio.get_running_loop()
        call = functools.partial(fn, *args)
        task = asyncio.ensure_future(loop.run_in_executor(self.mail_executor, call))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def mail_loop(self) -> None:
        warm = WarmSMTP(self.c, self.preconnects)
        prepared_key = None
        try:
            while not self.stop.is_set():
                self.mail_wake.clear()
                w = self.state.window
                candidate = (
                    self.c.notification_mode == "send"
                    and self.c.notify_live
                    and self.c.smtp_preconnect
                    and w is not None
                    and self.state.phase == "LIVE_CANDIDATE"
                    # Silent confirmations (restart, evidence gap) need no SMTP.
                    and self.state.expects_notice()
                )
                if candidate and w is not None and w.first.id != prepared_key:
                    await self.mail_io(warm.close)
                    await self.mail_io(warm.prepare)
                    prepared_key = w.first.id
                worked = await self.mail_io(
                    process_one, self.c, warm, time.time, self.halting.is_set
                )
                if not candidate or warm.expired:
                    await self.mail_io(warm.close)
                if not worked:
                    try:
                        await asyncio.wait_for(self.mail_wake.wait(), 1)
                    except TimeoutError:
                        pass
        finally:
            await self.mail_io(warm.close)

    async def run(self) -> None:
        halt = threading.Event()
        threading.Thread(
            target=loop_watchdog,
            args=(self.c, lambda: self.loop_alive, halt),
            name="loop-watchdog",
            daemon=True,
        ).start()
        try:
            await self._supervise()
        finally:
            halt.set()

    async def _supervise(self) -> None:
        # On SIGTERM finish the active SMTP attempt before releasing the instance lock.
        mail = asyncio.create_task(self.mail_loop())
        tasks = [
            asyncio.create_task(fn())
            for fn in (self.watch, self.sample_loop, self.heartbeat)
        ]
        stopped = asyncio.create_task(self.stop.wait())
        try:
            done, _ = await asyncio.wait(
                [*tasks, mail, stopped], return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()
        finally:
            self.stop.set()
            self.halting.set()
            for task in [*tasks, stopped]:
                task.cancel()
            await asyncio.gather(*tasks, stopped, return_exceptions=True)
            try:
                await mail
            finally:
                # Also when the mail loop failed: its error would skip this update.
                with self.db:
                    self.db.execute(
                        "UPDATE monitor SET health='STOPPED' WHERE room=?",
                        (self.c.room_id,),
                    )


# Each room samples through a short-lived worker process; cap how many run at once.
HTTP_WORKER_SLOTS = 4
# SMTP attempts in flight and idle preconnected SMTP workers, across all rooms.
MAIL_THREADS = 2
PRECONNECT_WORKERS = 2


async def run(c: Config) -> None:
    rooms = c.room_configs()
    with single_instance(c.db), ExitStack() as stack:
        secure_database_files(c.db)
        slots = Slots(HTTP_WORKER_SLOTS)
        mail_executor = stack.enter_context(
            ThreadPoolExecutor(MAIL_THREADS, thread_name_prefix="mail")
        )
        preconnects = threading.BoundedSemaphore(PRECONNECT_WORKERS)
        services = []
        for rc in rooms:
            # Own connection per room; every transaction completes without awaiting.
            db = stack.enter_context(closing(connect(c.db)))
            initialize(db, rc)
            services.append(
                Service(
                    rc,
                    db,
                    Douyu(rc),
                    slots=slots,
                    mail_executor=mail_executor,
                    preconnects=preconnects,
                )
            )
        loop = asyncio.get_running_loop()

        def stop_all() -> None:
            for service in services:
                service.stop.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, stop_all)
        if not c.notify_live and c.offline_requires_live_email:
            logging.getLogger(__name__).warning(
                "开播通知已关闭，同场下播邮件将按配对规则抑制"
            )
        for service in services:
            audit(
                service.c,
                "service",
                "STARTING",
                service.state.phase,
                c.notification_mode,
                environment=c.app_env,
                rooms=len(services),
            )
        tasks = [asyncio.create_task(service.run()) for service in services]
        try:
            # One failing room restarts the whole process; recovery is per room.
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stop_all()
            results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result
