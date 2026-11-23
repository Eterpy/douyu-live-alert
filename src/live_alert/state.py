import json
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from zoneinfo import ZoneInfo

from .config import Config
from .douyu import CLOCK_SKEW_SECONDS, Observation, clean
from .storage import audit

# aiodouyu's LiveStatusMonitor defers conflicting live snapshots this long after a replay.
REPLAY_GUARD_SECONDS = 60
SHANGHAI = ZoneInfo("Asia/Shanghai")


def local(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, SHANGHAI).isoformat(timespec="seconds")


def mail_time(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")


def duration(seconds: float) -> str:
    hours, minutes = divmod(max(0, round(seconds / 60)), 60)
    return f"{hours} 小时 {minutes} 分钟" if hours else f"{minutes} 分钟"


def sample_gap(previous: Observation, current: Observation) -> float:
    # During host suspend UTC can advance while the monotonic clock stands still.
    return max(
        current.start - previous.end,
        current.utc - (current.end - current.start) - previous.utc,
    )


@dataclass
class Window:
    kind: str
    first: Observation
    last: Observation
    started_at: int | None
    count: int = 1


class MonitorState:
    """One sampler owns this state. Each accepted observation commits with its effects."""

    def __init__(
        self,
        db: sqlite3.Connection,
        config: Config,
        now: float,
        monotonic: Callable[[], float] = time.monotonic,
    ):
        self.db, self.c = db, config
        self.monotonic = monotonic
        row = db.execute(
            """SELECT m.*, s.started_at, s.live_at FROM monitor m LEFT JOIN sessions s
               ON s.id=m.session_id WHERE m.room=?""",
            (config.room_id,),
        ).fetchone()
        self.confirmed: str = row["confirmed"]
        self.phase = self.confirmed
        self.session: int | None = row["session_id"]
        self.session_started_at: int | None = row["started_at"]
        self.session_live_at: float | None = row["live_at"]
        self.last_valid: float | None = row["last_valid"]
        # Last successful HTTP read, including replay artifacts (not persisted).
        self.last_seen = self.last_valid
        self.last_offline: float | None = row["last_offline"]
        self.replay_at: float | None = row["replay_at"]
        self.replay_start: int | None = row["replay_start"]
        self.replay_deadline: float | None = None
        if self.replay_at is not None:
            age = max(0.0, now - self.replay_at)
            self.replay_deadline = monotonic() + max(0.0, REPLAY_GUARD_SECONDS - age)
        self.resume_check: Observation | None = None
        self.correction_check: Observation | None = None
        # First non-live evidence since the session's last live sample.
        self.offline_first: float | None = row["offline_first"]
        # Set by restore: sessions starting before it may already have been notified.
        self.restored_at: float | None = row["restored_at"]
        self.restore_quiet_since: float | None = None
        self.restore_quiet_samples = 0
        self.evidence_gap_limit = (
            2 * config.poll_interval_seconds + config.http_timeout_seconds
        )
        # Keep the pre-outage reference across interrupted candidates and restarts.
        self.gap_reference: float | None = row["gap_reference"]
        self.first_live_after_gap: float | None = None
        self.reconciling = self.confirmed == "OFFLINE"
        # Even a short restart can hide a change of session. Use the last
        # offline evidence for an offline room, or the last valid read for a live room.
        if self.gap_reference is None and self.confirmed != "BASELINING":
            self.gap_reference = (
                self.last_offline
                if self.reconciling and self.last_offline is not None
                else self.last_valid
            )
            if self.confirmed == "LIVE_CONFIRMED" and row["phase"] == "LIVE_CANDIDATE":
                # The last read already saw the possible new session. Compare its
                # start with evidence before that candidate, as for an offline room.
                previous = db.execute(
                    """SELECT utc FROM observations WHERE room=? AND utc < ?
                       AND state IN ('LIVE','OFFLINE','REPLAY') ORDER BY utc DESC LIMIT 1""",
                    (config.room_id, row["window_first"]),
                ).fetchone()
                if previous is not None:
                    self.gap_reference = previous["utc"]
        self.window: Window | None = None
        self.stale = bool(row["stale_restart"]) or (
            self.session is not None
            and (
                self.last_valid is None
                or not 0
                <= now - self.last_valid
                <= config.offline_restart_grace_seconds
            )
        )
        with db:
            self._persist()

    def _persist(self) -> None:
        w = self.window
        self.db.execute(
            """UPDATE monitor SET confirmed=?, session_id=?, phase=?, last_valid=?,
                        stale_restart=?, window_first=?, samples=?, last_offline=?,
                        replay_at=?, replay_start=?, offline_first=?, restored_at=?, gap_reference=?
                        WHERE room=?""",
            (
                self.confirmed,
                self.session,
                self.phase,
                self.last_valid,
                self.stale,
                w.first.utc if w else None,
                w.count if w else 0,
                self.last_offline,
                self.replay_at,
                self.replay_start,
                self.offline_first,
                self.restored_at,
                self.gap_reference,
                self.c.room_id,
            ),
        )

    def interrupt_live_checks(self) -> None:
        self.resume_check = None
        self.correction_check = None

    def _record_replay(self, utc: float, end: float, start: int | None) -> None:
        self.replay_at = utc
        self.replay_start = start
        self.replay_deadline = end + REPLAY_GUARD_SECONDS
        self.interrupt_live_checks()

    def note_replay(self, utc: float) -> None:
        """A realtime replay event carries no platform start time."""
        self._record_replay(utc, self.monotonic(), None)
        with self.db:
            self._persist()

    def _live_evidence(self, o: Observation, now: float) -> Observation:
        spacing = self.c.live_confirm_gap_seconds
        max_gap = spacing + self.c.http_timeout_seconds
        if now - o.end > max_gap:
            return replace(o, state="UNKNOWN", reason="stale_live_evidence")

        start = o.started_at
        boundary = (
            self.replay_start if self.replay_start is not None else self.replay_at
        )
        replay_time = start is not None and boundary is not None and start <= boundary
        if self.replay_deadline is not None and o.end < self.replay_deadline:
            guarded = (
                start is not None and start == self.replay_start
                if self.confirmed == "LIVE_CONFIRMED"
                else replay_time or start is None
            )
            if guarded:
                self.resume_check = None
                return replace(o, state="UNKNOWN", reason="replay_guard")

        if self.phase == "OFFLINE_CANDIDATE" and replay_time:
            # One live-looking replay must neither cancel nor confirm a pending end.
            previous = self.resume_check
            if previous is None or sample_gap(previous, o) > max_gap:
                self.resume_check = o
                return replace(o, state="UNKNOWN", reason="replay_resume_check")
            if o.start < previous.end or o.start - previous.start < spacing:
                return replace(o, state="UNKNOWN", reason="replay_resume_check")
        # A valid LIVE verdict and a trustworthy session start are separate facts.
        return replace(o, started_at=None) if replay_time else o

    def interrupt(self, reason: str) -> None:
        before = self.phase
        self.interrupt_live_checks()
        self.window, self.phase = None, self.confirmed
        with self.db:
            self._persist()
        audit(self.c, "rss", before, self.phase, reason, session=self.session)

    def next_check(self) -> float | None:
        if self.resume_check is not None:
            return self.resume_check.start + self.c.live_confirm_gap_seconds
        w = self.window
        if not w:
            return None
        if self.phase == "OFFLINE_CANDIDATE":
            deadline = w.first.end + self.c.offline_confirm_seconds
            spacing = self.c.offline_confirm_seconds / (
                self.c.offline_confirm_samples - 1
            )
            return (
                min(deadline, w.last.end + spacing)
                if deadline > w.last.end
                else w.last.end + spacing
            )
        return w.last.start + self.c.live_confirm_gap_seconds

    def observe(
        self, o: Observation, trigger: str, mono_now: float | None = None
    ) -> bool:
        """Commit the observation and report whether it supplied usable evidence."""
        before = self.phase
        with self.db:
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO observations VALUES (?,?,?,?,?,?,?,?)",
                (
                    o.id,
                    self.c.room_id,
                    o.utc,
                    trigger,
                    o.state,
                    o.reason,
                    json.dumps(o.raw),
                    o.end - o.start,
                ),
            )
            if not inserted.rowcount:
                return False
            now = self.monotonic() if mono_now is None else mono_now
            if o.state == "LIVE":
                o = self._live_evidence(o, now)
                if o.state == "UNKNOWN":
                    self.db.execute(
                        "UPDATE observations SET state=?,reason=? WHERE id=?",
                        (o.state, o.reason, o.id),
                    )
            if (
                self.last_seen is not None
                and o.utc - self.last_seen > self.evidence_gap_limit
            ):
                self.first_live_after_gap = None
                if self.gap_reference is None:
                    self.gap_reference = self.last_seen
            usable = False
            if o.reason in ("replay_guard", "replay_resume_check"):
                # Neither counts toward nor resets a confirmation window.
                self.last_seen = o.utc
            elif o.state not in ("LIVE", "OFFLINE", "REPLAY"):
                self.resume_check = None
                self.window, self.phase = None, self.confirmed
            else:
                # The write lock can delay processing after the sample has returned.
                self.resume_check = None
                self._advance(o, now, trigger)
                usable = True
                self._settle_restore(o)
                self.last_valid = self.last_seen = o.utc
                if o.state != "LIVE":
                    self.last_offline = o.utc
                if o.state == "REPLAY":
                    self._record_replay(o.utc, o.end, o.started_at)
            if not usable or o.state != "LIVE":
                self.correction_check = None
            if not usable:
                self.restore_quiet_since = None
            self.db.execute(
                "UPDATE monitor SET mail_ready=? WHERE room=?",
                (int(usable), self.c.room_id),
            )
            self._persist()
        audit(
            self.c,
            trigger,
            before,
            self.phase,
            o.reason,
            observation=o.id,
            session=self.session,
            samples=self.window.count if self.window else 0,
        )
        return usable

    def _advance(self, o: Observation, now: float, trigger: str) -> None:
        live = o.state == "LIVE"
        if (
            self.session is not None
            and self.last_valid is not None
            and o.utc - self.last_valid > self.c.max_notification_age_seconds
        ):
            self.stale = True
        switch_after = self.session_started_at or self.session_live_at or o.utc
        resuming = self._resumed(o)
        undated_after_replay = (
            self.session_started_at is None
            and self.replay_at is None
            and self.replay_start is not None
        )
        # When replay made the start uncertain, a correction can lead local
        # confirmation by the platform's clock skew and still describe this session.
        if undated_after_replay and not resuming:
            switch_after += CLOCK_SKEW_SECONDS
        switching = (
            self.confirmed == "LIVE_CONFIRMED"
            and live
            and o.started_at is not None
            and o.started_at > switch_after
        )
        if switching and resuming:
            # Back within the offline confirmation period: same broadcast, new start.
            audit(
                self.c,
                trigger,
                self.phase,
                self.confirmed,
                "session_resumed",
                session=self.session,
                started_at=o.started_at,
            )
            self._set_session_start(o.started_at)
            self.gap_reference = None
            self.offline_first = None
            self.window, self.phase = None, self.confirmed
            return
        if self.confirmed == "LIVE_CONFIRMED" and live and not switching:
            if undated_after_replay and self.gap_reference is None and not self.stale:
                self._check_corrected_start(o)
            # Even without a show_time, this read proves monitoring has resumed.
            if self.gap_reference is not None and self.first_live_after_gap is None:
                self.first_live_after_gap = o.utc
            if (
                self.session_started_at is not None
                and o.started_at == self.session_started_at
            ):
                self.stale = False
                self.gap_reference = None
            self.offline_first = None
            self.window, self.phase = None, self.confirmed
            return
        if switching:
            self.correction_check = None
            self.stale = True
        if self.confirmed == "OFFLINE" and not live:
            self.reconciling = False
            self.gap_reference = None
            self.window, self.phase = None, self.confirmed
            return

        closing = self.confirmed == "LIVE_CONFIRMED" and not switching
        if closing and self.offline_first is None:
            self.offline_first = o.utc
        kind = "LIVE" if live else "NONLIVE"
        w = self.window
        if w is not None and w.kind == kind:
            # Across a sampling pause the intervening stream state is unknown.
            cadence = (
                self.c.offline_confirm_seconds
                if closing
                else self.c.live_confirm_gap_seconds
            )
            gap = sample_gap(w.last, o)
            different_start = (
                live
                and w.started_at is not None
                and o.started_at is not None
                and w.started_at != o.started_at
            )
            if gap > cadence + self.c.http_timeout_seconds or different_start:
                audit(
                    self.c,
                    trigger,
                    self.phase,
                    self.phase,
                    "session_start_changed" if different_start else "sampling_gap",
                    gap=gap,
                )
                w = None
        if w is None or w.kind != kind:
            self.window = Window(kind, o, o, o.started_at)
            if self.confirmed != "BASELINING":
                self.phase = "OFFLINE_CANDIDATE" if closing else "LIVE_CANDIDATE"
            return
        if o.start < w.last.end or o.start <= w.last.start:
            return
        if not closing and o.start - w.last.start < self.c.live_confirm_gap_seconds:
            return
        w.last, w.count = o, w.count + 1
        if w.started_at is None:
            w.started_at = o.started_at
        if closing:
            if (
                w.count >= self.c.offline_confirm_samples
                and o.end - w.first.end >= self.c.offline_confirm_seconds
                and 0 <= now - o.end <= self.c.offline_final_evidence_max_age_seconds
            ):
                self._close(w)
            return
        if w.count < self.c.live_confirm_samples:
            return
        if live:
            reference = self.gap_reference
            if switching:
                # New start proves a different session, but not the old end time.
                self.db.execute(
                    "UPDATE sessions SET ended=? WHERE id=?", (o.utc, self.session)
                )
                self._enqueue(
                    "OFFLINE",
                    "SUPPRESSED_SESSION_CHANGE",
                    "[斗鱼场次变更] 旧场次已归档",
                    "发现新的开播时间，旧场次实际结束时间未知。",
                    o.utc,
                )
                self.reconciling = False
                # A start definitely later than a post-gap live read happened
                # under observation, even if that earlier read lacked show_time.
                if (
                    self.first_live_after_gap is not None
                    and o.started_at is not None
                    and o.started_at > self.first_live_after_gap + CLOCK_SKEW_SECONDS
                ):
                    reference = None
            # After a restart or any evidence gap the start may long predate us.
            unwatched = self.reconciling or reference is not None
            missed = (
                unwatched
                and self.c.startup_missed_event_window_seconds > 0
                and (
                    o.started_at is not None
                    and w.first.started_at == o.started_at
                    and reference is not None
                    # Second-precision starts equal to the offline second are ambiguous.
                    and o.started_at > int(reference)
                    and -CLOCK_SKEW_SECONDS
                    <= o.utc - o.started_at
                    <= self.c.startup_missed_event_window_seconds
                )
            )
            silent = self.confirmed == "BASELINING" or (unwatched and not missed)
            self._open(w, silent)
        else:
            self.confirmed = self.phase = "OFFLINE"
        self.reconciling, self.window = False, None
        self.gap_reference = None

    def _set_session_start(self, start: int | None) -> None:
        self.correction_check = None
        self.session_started_at = start
        self.db.execute(
            "UPDATE sessions SET started_at=? WHERE id=?", (start, self.session)
        )

    def _check_corrected_start(self, o: Observation) -> None:
        # Two consistent corrections establish identity for future switches.
        # The start recorded for the original notice/duration stays unknown.
        first = self.correction_check
        if o.started_at is None:
            self.correction_check = None
        elif (
            first is None
            or first.started_at != o.started_at
            or sample_gap(first, o) > self.evidence_gap_limit
        ):
            self.correction_check = o
        elif (
            o.start >= first.end
            and o.start - first.start >= self.c.live_confirm_gap_seconds
        ):
            self._set_session_start(o.started_at)

    def _settle_restore(self, o: Observation) -> None:
        """Forget the restore point once the room has stayed off for a full offline
        window: any later broadcast is new. A single off sample is not enough."""
        if self.restored_at is None:
            return
        if o.state == "LIVE":
            self.restore_quiet_since = None
            return
        if (
            self.restore_quiet_since is None
            or self.last_valid is None
            or not 0 < o.utc - self.last_valid <= self.evidence_gap_limit
        ):
            self.restore_quiet_since = o.end
            self.restore_quiet_samples = 1
            return
        self.restore_quiet_samples += 1
        if (
            o.end - self.restore_quiet_since >= self.c.offline_confirm_seconds
            and self.restore_quiet_samples >= self.c.offline_confirm_samples
        ):
            self.restored_at = self.restore_quiet_since = None

    def expects_notice(self) -> bool:
        """Best effort: would confirming the current live window send an email?"""
        w = self.window
        return (
            w is not None
            and w.kind == "LIVE"
            and self.confirmed != "BASELINING"
            and not self.reconciling
            and self.gap_reference is None
        )

    def _resumed(self, o: Observation) -> bool:
        # Longer spans imply evidence gaps; the old end is then unknown.
        c = self.c
        limit = (
            c.offline_confirm_seconds + c.poll_interval_seconds + c.http_timeout_seconds
        )
        return (
            self.offline_first is not None and 0 <= o.utc - self.offline_first <= limit
        )

    def _open(self, w: Window, silent: bool) -> None:
        o = w.last
        c = self.c
        # Whatever reaches SQLite and mail headers must be printable text.
        name = clean(c.room_name or o.owner, 100) or f"斗鱼房间 {c.room_id}"
        title = clean(o.title, 300)
        self.session_started_at = (
            o.started_at if w.first.started_at == o.started_at else None
        )
        # A fast platform clock can date an already-notified session after restore.
        restored = self.restored_at is not None and (
            self.session_started_at is None
            or self.session_started_at <= self.restored_at + CLOCK_SKEW_SECONDS
        )
        cur = self.db.execute(
            """INSERT INTO sessions(room,name,title,live_at,started_at,first_started_at,
                              silent,first_live_id,last_live_id,restored)
                              VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                c.room_id,
                name,
                title,
                o.utc,
                self.session_started_at,
                self.session_started_at,
                silent,
                w.first.id,
                o.id,
                restored,
            ),
        )
        self.session = cur.lastrowid
        self.session_live_at = o.utc
        self.confirmed = self.phase = "LIVE_CONFIRMED"
        self.stale = False
        self.offline_first = None
        # Retire the timer. For an undated session keep only the replay boundary,
        # so a restart can still recognize a reused time or a later correction.
        if self.session_started_at is not None:
            self.replay_start = None
        elif self.replay_start is None and self.replay_at is not None:
            self.replay_start = int(self.replay_at)
        self.replay_at = self.replay_deadline = None
        self.correction_check = None
        subject = f"🎙️ {name} 开播啦～"
        body = (
            "📣 直播间有新动静啦～\n"
            f"直播间：https://www.douyu.com/{c.room_id}\n"
            f"标题: {title or '未获取'}\n"
            f"本场确认开播时间：{mail_time(o.utc)}"
        )
        state = (
            "SUPPRESSED_BASELINE"
            if silent
            else "SUPPRESSED_RESTORE"
            if restored
            else self._initial_delivery(c.notify_live)
        )
        self._enqueue("LIVE", state, subject, body, o.utc)

    def _initial_delivery(self, enabled: bool) -> str:
        if not enabled:
            return "SUPPRESSED_DISABLED"
        return "SIMULATED" if self.c.notification_mode == "dry-run" else "PENDING"

    def _close(self, w: Window) -> None:
        c, o = self.c, w.last
        row = self.db.execute(
            "SELECT * FROM sessions WHERE id=?", (self.session,)
        ).fetchone()
        self.db.execute(
            """UPDATE sessions SET first_off=?,ended=?,sample_count=?,first_off_id=?,
                        last_off_id=? WHERE id=?""",
            (w.first.utc, o.utc, w.count, w.first.id, o.id, self.session),
        )
        state = self._initial_delivery(c.notify_offline)
        if row["restored"]:
            state = "SUPPRESSED_RESTORE"
        elif row["silent"] and not c.notify_offline_for_baseline_live:
            state = "SUPPRESSED_BASELINE"
        elif self.stale or o.utc - w.first.utc > c.max_notification_age_seconds:
            state = "EXPIRED"
        elif c.notify_offline and c.offline_requires_live_email:
            prior = self.db.execute(
                """SELECT state FROM outbox WHERE session_id=? AND kind='LIVE'
                                    AND recipient=?""",
                (self.session, c.smtp_to_address),
            ).fetchone()
            prior_state = prior[0] if prior else None
            if (
                prior_state in ("PENDING", "RETRYING", "PREPARING", "SUBMITTING")
                and state == "PENDING"
            ):
                state = "WAITING_FOR_LIVE_EMAIL"
            elif prior_state != "SENT" and not (
                prior_state == "SIMULATED" and state == "SIMULATED"
            ):
                state = "SUPPRESSED_NO_LIVE_NOTICE"
        subject = f"🌙 {row['name']} 下播休息啦～"
        first_start = row["first_started_at"]
        # The stream ended before the first offline sample; the span is approximate.
        length = (
            f"约 {duration(w.first.utc - first_start)}"
            if first_start is not None
            else "未知"
        )
        body = (
            "🌙 直播间安静下来啦，记录放在这里～\n"
            f"直播间：https://www.douyu.com/{c.room_id}\n"
            f"标题: {row['title'] or '未获取'}\n"
            f"确认下播时间：{mail_time(o.utc)}\n"
            f"直播时长：{length}"
        )
        self._enqueue("OFFLINE", state, subject, body, o.utc)
        self.session, self.window, self.stale = None, None, False
        self.gap_reference = None
        self.offline_first = None
        self.confirmed = self.phase = "OFFLINE"

    def _enqueue(
        self, kind: str, state: str, subject: str, body: str, now: float
    ) -> None:
        self.db.execute(
            """INSERT INTO outbox(session_id,kind,recipient,state,reason,subject,body,
                        created,next_try) VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                self.session,
                kind,
                self.c.smtp_to_address,
                state,
                state,
                subject,
                body,
                now,
                now,
            ),
        )
        audit(self.c, "outbox", "NEW", state, kind, session=self.session)
