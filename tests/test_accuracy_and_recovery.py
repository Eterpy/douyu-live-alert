import asyncio
import threading
import time
from contextlib import suppress
from dataclasses import replace
from unittest.mock import patch

import pytest
from aiodouyu import web
from conftest import config, text

from live_alert.douyu import Douyu, Observation
from live_alert.service import Service, loop_watchdog
from live_alert.state import REPLAY_GUARD_SECONDS, duration
from live_alert.storage import initialize

ROOM = 10001
REPLAY_START = 900


def replay_baseline(s):
    s.feed("REPLAY", started_at=REPLAY_START)
    s.feed("REPLAY", 10, started_at=REPLAY_START)
    assert s.state.confirmed == "OFFLINE"


def live_session(s, start=1020):
    s.baseline()
    s.feed("LIVE", 30, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.worker()


# Replay snapshots that look live


@pytest.mark.parametrize("start", [REPLAY_START, REPLAY_START - 100])
def test_reused_or_older_replay_start_needs_two_live_reads_after_guard(scenario, start):
    s = scenario
    replay_baseline(s)
    s.feed("LIVE", 30, started_at=start)
    s.feed("LIVE", REPLAY_GUARD_SECONDS - 31, started_at=start)
    assert s.state.window is None and not s.events()
    stored = s.db.execute(
        "SELECT state,reason FROM observations ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    assert tuple(stored) == ("UNKNOWN", "replay_guard")
    s.feed("LIVE", 1, started_at=start)  # Exactly at expiry: first usable sample.
    assert s.state.phase == "LIVE_CANDIDATE" and s.state.window.count == 1
    s.feed("LIVE", 9, started_at=start)
    assert not s.events()
    s.feed("LIVE", 1, started_at=start)
    assert s.worker()
    s.feed("LIVE", 30, started_at=start)
    s.offline()
    assert s.worker() and not s.worker()
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT")]


def test_new_replay_evidence_rearms_guard_and_cancels_a_single_live_read(scenario):
    s = scenario
    replay_baseline(s)
    for _ in range(3):
        s.feed("LIVE", 50, started_at=REPLAY_START)
        assert s.state.window is None
        s.feed("LIVE", 10, started_at=REPLAY_START)
        assert s.state.phase == "LIVE_CANDIDATE"
        s.feed("REPLAY", 5, started_at=REPLAY_START)
        assert s.state.phase == "OFFLINE" and not s.events()


def test_unknown_between_live_reads_after_replay_restarts_confirmation(scenario):
    s = scenario
    replay_baseline(s)
    s.feed("LIVE", REPLAY_GUARD_SECONDS, started_at=REPLAY_START)
    s.feed("UNKNOWN", 5)
    assert s.state.window is None
    s.feed("LIVE", 10, started_at=REPLAY_START)
    assert not s.events()
    s.feed("LIVE", 10, started_at=REPLAY_START)
    assert s.worker() and s.events() == [("LIVE", "SENT")]


def test_restart_keeps_replay_deadline_but_rebuilds_live_confirmation(scenario):
    s = scenario
    replay_baseline(s)
    s.restart(REPLAY_GUARD_SECONDS - 2)
    s.feed("LIVE", 1, started_at=REPLAY_START)
    assert s.state.window is None
    s.feed("LIVE", 1, started_at=REPLAY_START)
    assert s.state.phase == "LIVE_CANDIDATE"
    s.restart(5)
    s.feed("LIVE", 5, started_at=REPLAY_START)
    assert s.state.window.count == 1 and not s.events()
    s.feed("LIVE", 10, started_at=REPLAY_START)
    assert s.state.confirmed == "LIVE_CONFIRMED"
    # The reused start predates the last offline evidence. No restart catch-up mail.
    assert s.events() == [("LIVE", "SUPPRESSED_BASELINE")] and not s.worker()


def test_live_without_start_waits_out_replay_guard(scenario):
    s = scenario
    replay_baseline(s)
    s.feed("LIVE", 20)
    s.feed("LIVE", 10)
    s.feed("LIVE", 10)
    assert not s.events()
    s.feed("LIVE", REPLAY_GUARD_SECONDS)
    s.feed("LIVE", 10)
    assert s.events() == [("LIVE", "PENDING")]


def test_real_start_after_replay_confirms_without_delay(scenario):
    s = scenario
    replay_baseline(s)
    start = s.t + 3
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events() == [("LIVE", "PENDING")]
    assert s.state.replay_at is None and s.state.replay_start is None


def test_realtime_replay_event_arms_guard_across_restart(scenario):
    s = scenario
    s.baseline()
    service = Service(s.c, s.db, Douyu(s.c), lambda: s.t, lambda: s.t)
    service.event("replay")
    s.restart(5)
    assert s.state.replay_at == s.t - 5
    s.feed("LIVE", 5)
    s.feed("LIVE", 10)
    s.feed("LIVE", 10, started_at=s.t - 30)
    s.feed("LIVE", 10, started_at=s.t - 40)
    assert not s.events()
    start = s.t + 2
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events() == [("LIVE", "PENDING")]


def test_replay_snapshot_neither_counts_nor_resets_offline_window(scenario):
    s = scenario
    live_session(s)
    s.feed("REPLAY", 30, started_at=REPLAY_START + 1000)
    s.feed("LIVE", 30, started_at=REPLAY_START + 1000)
    assert s.state.phase == "OFFLINE_CANDIDATE" and s.state.window.count == 1
    s.feed("REPLAY", 30, started_at=REPLAY_START + 1000)
    s.feed("REPLAY", 60, started_at=REPLAY_START + 1000)
    assert s.state.session is None
    assert s.worker() and s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT")]


@pytest.mark.parametrize("correction", [None, -65, 30])
@pytest.mark.parametrize("restart", [False, True])
def test_room_63136_reused_start_never_dates_or_duplicates_the_session(
    scenario, correction, restart
):
    s = scenario
    replay_start = 1795246265
    s.t = replay_start + 86400
    s.restart(room_id=63136)
    s.feed("REPLAY", started_at=replay_start)
    s.feed("REPLAY", 10, started_at=replay_start)
    s.feed("LIVE", 30, started_at=replay_start)
    s.feed("LIVE", 30, started_at=replay_start)
    s.feed("LIVE", 10, started_at=replay_start)
    assert s.worker()
    confirmed = s.t
    if restart:
        s.restart(2)
    start = replay_start if correction is None else confirmed + correction
    s.feed("LIVE", 30, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events() == [("LIVE", "SENT")]
    assert s.db.execute("SELECT first_started_at FROM sessions").fetchone()[0] is None
    s.offline()
    assert s.worker() and not s.worker()
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT")]
    assert "直播时长：未知" in text(s.sent[-1])


def test_distinct_later_broadcast_after_reused_start_still_rotates(scenario):
    s = scenario
    replay_baseline(s)
    s.feed("LIVE", 60, started_at=REPLAY_START)
    s.feed("LIVE", 10, started_at=REPLAY_START)
    assert s.worker()
    for _ in range(5):
        s.feed("LIVE", 30, started_at=REPLAY_START)
    new_start = s.t + 1
    s.feed("LIVE", 5, started_at=new_start)
    s.feed("LIVE", 10, started_at=new_start)
    assert s.events() == [
        ("LIVE", "SENT"),
        ("OFFLINE", "SUPPRESSED_SESSION_CHANGE"),
        ("LIVE", "PENDING"),
    ]


def test_corroborated_correction_allows_subsequent_real_switch(scenario):
    s = scenario
    replay_baseline(s)
    corrected = s.t + 3
    s.feed("LIVE", 60, started_at=REPLAY_START)
    s.feed("LIVE", 10, started_at=REPLAY_START)
    assert s.worker()
    s.feed("LIVE", 10, started_at=corrected)
    assert s.state.session_started_at is None
    for _ in range(5):
        s.feed("LIVE", 2, started_at=corrected)
    assert s.state.session_started_at == corrected
    assert s.events() == [("LIVE", "SENT")]
    next_start = s.t + 1
    s.feed("LIVE", 5, started_at=next_start)
    s.feed("LIVE", 10, started_at=next_start)
    assert s.state.session == 2 and s.worker()
    assert s.events() == [
        ("LIVE", "SENT"),
        ("OFFLINE", "SUPPRESSED_SESSION_CHANGE"),
        ("LIVE", "SENT"),
    ]


def test_reused_replay_time_stays_untrusted_after_confirmed_offline(scenario):
    s = scenario
    replay_baseline(s)
    s.feed("LIVE", 60, started_at=REPLAY_START)
    s.feed("LIVE", 10, started_at=REPLAY_START)
    assert s.worker()
    for _ in range(3):
        s.feed("OFFLINE", 60)
    assert s.worker()
    corrected = s.t + 5
    s.feed("LIVE", 30, started_at=REPLAY_START)
    s.feed("LIVE", 10, started_at=REPLAY_START)
    assert s.worker()
    s.feed("LIVE", 30, started_at=corrected)
    s.feed("LIVE", 10, started_at=corrected)
    assert s.state.session == 2 and s.state.session_started_at == corrected
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT"), ("LIVE", "SENT")]


def test_later_replay_hint_keeps_normal_switch_for_preexisting_undated_session(
    scenario,
):
    s = scenario
    service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
    s.state = service.state
    s.baseline()
    s.live()
    assert s.worker()
    s.t += 5
    service.event("replay")
    next_start = s.t + 2
    s.feed("LIVE", 5, started_at=next_start)
    s.feed("LIVE", 10, started_at=next_start)
    assert s.state.session == 2 and s.worker()


@pytest.mark.parametrize("old_first", [True, False])
def test_alternating_replay_and_real_starts_confirm_live(scenario, old_first):
    s = scenario
    replay_baseline(s)
    new_start = s.t + 5
    starts = [REPLAY_START, new_start] if old_first else [new_start, REPLAY_START]
    s.feed("LIVE", 60, started_at=starts[0])
    s.feed("LIVE", 10, started_at=starts[1])
    assert s.worker()
    for start in starts * 3:
        s.feed("LIVE", 10, started_at=start)
    assert s.events() == [("LIVE", "SENT")]


@pytest.mark.parametrize("source", ["HTTP", "RSS"])
def test_latest_undated_replay_cannot_borrow_an_older_show_time(scenario, source):
    s = scenario
    replay_baseline(s)
    suspect = s.t + 5
    if source == "HTTP":
        s.feed("REPLAY", 30)
    else:
        s.t += 30
        service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
        service.event("replay")
    s.restart(1)
    s.feed("LIVE", 5, started_at=suspect)
    s.feed("LIVE", 10, started_at=suspect)
    assert s.state.window is None and not s.events()
    s.feed("LIVE", 44, started_at=suspect)
    s.feed("LIVE", 10, started_at=suspect)
    assert s.events() == [("LIVE", "SUPPRESSED_BASELINE")]


@pytest.mark.parametrize("clock_step", [-300, 300])
def test_replay_window_uses_elapsed_time_across_wall_clock_steps(scenario, clock_step):
    s = scenario
    replay_baseline(s)
    replay_end = s.t
    for elapsed in [5, 15, 59, 60, 70]:
        s.t = replay_end + elapsed
        o = Observation(
            "LIVE", "fixture", s.t, s.t, s.t + clock_step, started_at=REPLAY_START
        )
        usable = s.state.observe(o, "timer", s.t)
        assert usable == (elapsed >= 60)
        if elapsed < 60:
            assert s.state.window is None
    assert s.state.confirmed == "LIVE_CONFIRMED"


def test_restart_with_future_replay_time_waits_one_bounded_window(scenario):
    s = scenario
    replay_baseline(s)
    s.restart(-300)
    s.feed("LIVE", 49, started_at=REPLAY_START)
    s.feed("LIVE", 10, started_at=REPLAY_START)
    assert s.state.window is None and not s.events()
    s.feed("LIVE", 1, started_at=REPLAY_START)
    s.feed("LIVE", 10, started_at=REPLAY_START)
    assert s.events() == [("LIVE", "SUPPRESSED_BASELINE")]


def test_isolated_live_at_replay_expiry_cannot_keep_room_open(scenario):
    s = scenario
    live_session(s)
    for _ in range(6):
        s.feed("REPLAY", 30, started_at=REPLAY_START)
        s.worker()
        s.feed("OFFLINE", 30)
        s.worker()
        s.feed("LIVE", 30, started_at=REPLAY_START)
    assert s.state.confirmed == "OFFLINE"
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT")]
    assert s.db.execute("SELECT sample_count FROM sessions").fetchone()[0] == 4


def test_ambiguous_resume_needs_fresh_independent_recheck(scenario):
    s = scenario
    live_session(s)
    s.feed("REPLAY", 30, started_at=REPLAY_START)
    s.feed("OFFLINE", 30)
    first = s.feed("LIVE", 30, started_at=REPLAY_START)
    assert s.state.phase == "OFFLINE_CANDIDATE" and s.state.window.count == 2
    assert s.state.next_check() == first.start + 10
    assert s.db.execute("SELECT mail_ready FROM monitor").fetchone()[0] == 0
    s.state.observe(first, "duplicate", first.end + 10)
    s.feed("LIVE", 9, started_at=REPLAY_START)
    assert s.state.phase == "OFFLINE_CANDIDATE"
    s.feed("LIVE", 1, started_at=REPLAY_START)
    assert s.state.phase == "LIVE_CONFIRMED" and s.state.window is None
    assert s.events() == [("LIVE", "SENT")]


@pytest.mark.parametrize("interruption", ["UNKNOWN", "restart", "gap", "RSS"])
def test_resume_recheck_cannot_bridge_interrupted_evidence(scenario, interruption):
    s = scenario
    live_session(s)
    service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
    s.state = service.state
    s.feed("REPLAY", 30, started_at=REPLAY_START)
    s.feed("OFFLINE", 30)
    s.feed("LIVE", 30, started_at=REPLAY_START)
    if interruption == "RSS":
        service.event("possible_offline")
    elif interruption == "gap":
        s.t += 20
    else:
        if interruption == "UNKNOWN":
            s.feed("UNKNOWN", 1)
        else:
            s.restart(1)
        assert s.state.window is None
        s.feed("OFFLINE", 1)
        assert s.state.window.count == 1
    s.feed("LIVE", 10, started_at=REPLAY_START)
    assert s.state.phase == "OFFLINE_CANDIDATE"
    s.feed("LIVE", 10, started_at=REPLAY_START)
    assert s.state.phase == "LIVE_CONFIRMED"
    assert s.events() == [("LIVE", "SENT")]


def replay_during_session(s, source, start):
    """Replay evidence whose boundary is not earlier than the session's start."""
    if source == "RSS":
        s.t += 5
        service = Service(s.c, s.db, None, lambda: s.t, lambda: s.t)
        s.state = service.state
        service.event("replay")
    elif source in ("HTTP_same", "HTTP_later"):
        replay_start = start if source == "HTTP_same" else int(s.t)
        s.feed("REPLAY", 30, started_at=replay_start)
        s.feed("LIVE", 30, started_at=start)
        # The sampler's fast rechecks. A reused replay start waits out the guard
        # (reads at 40-60 s), then needs a pair; a later replay start only a pair.
        for _ in range(4 if source == "HTTP_same" else 1):
            assert s.state.phase == "OFFLINE_CANDIDATE"
            s.feed("LIVE", 10, started_at=start)
        assert s.state.phase == "LIVE_CONFIRMED"


@pytest.mark.parametrize("source", [None, "RSS", "HTTP_same", "HTTP_later"])
def test_replay_during_session_keeps_offline_mail_after_long_restart(scenario, source):
    s = scenario
    start = 1020
    live_session(s, start)
    s.feed("LIVE", 30, started_at=start)
    replay_during_session(s, source, start)
    for _ in range(4):
        s.feed("LIVE", 30, started_at=start)
    assert s.state.confirmed == "LIVE_CONFIRMED" and s.events() == [("LIVE", "SENT")]
    s.restart(s.c.offline_restart_grace_seconds + 1)
    assert s.state.stale
    s.feed("LIVE", 30, started_at=start)
    # One read is not enough when a replay may have carried this start.
    assert s.state.stale == (source is not None)
    s.feed("LIVE", 30, started_at=start)
    assert not s.state.stale
    s.offline()
    assert s.worker() and not s.worker()
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT")]


# OFFLINE/REPLAY open a pending end; its two-read recheck is covered above.
@pytest.mark.parametrize("interruption", ["UNKNOWN", "other"])
def test_session_start_after_replay_needs_consecutive_matching_reads(
    scenario, interruption
):
    s = scenario
    start = 1020
    live_session(s, start)
    replay_during_session(s, "RSS", start)
    s.restart(s.c.offline_restart_grace_seconds + 1)
    s.feed("LIVE", 30, started_at=start)
    if interruption == "other":
        s.feed("LIVE", 30)  # Live, but without this session's start.
    else:
        s.feed("UNKNOWN", 30)
    s.feed("LIVE", 30, started_at=start)
    assert s.state.stale
    s.feed("LIVE", 30, started_at=start)
    assert not s.state.stale


def test_session_start_after_replay_needs_spaced_reads(scenario):
    s = scenario
    start = 1020
    live_session(s, start)
    replay_during_session(s, "RSS", start)
    s.restart(s.c.offline_restart_grace_seconds + 1)
    s.feed("LIVE", 30, started_at=start)
    s.feed("LIVE", s.c.live_confirm_gap_seconds - 1, started_at=start)
    assert s.state.stale
    s.feed("LIVE", 1, started_at=start)
    assert not s.state.stale
    s.restart(s.c.offline_restart_grace_seconds + 1)
    s.feed("LIVE", 30, started_at=start)
    s.feed("LIVE", s.state.evidence_gap_limit + 1, started_at=start)
    assert s.state.stale
    s.feed("LIVE", 30, started_at=start)
    assert not s.state.stale


@pytest.mark.parametrize("first", [REPLAY_START, 1020])
def test_pending_end_resume_restores_identity_only_if_both_reads_match(scenario, first):
    s = scenario
    start = 1020
    live_session(s, start)
    replay_during_session(s, "RSS", start)
    s.restart(s.c.offline_restart_grace_seconds + 1)
    s.feed("OFFLINE", 30)
    assert s.state.phase == "OFFLINE_CANDIDATE"
    s.feed("LIVE", 30, started_at=first)
    s.feed("LIVE", 10, started_at=start)
    # Either pair confirms the resume; only a matching pair confirms the session.
    assert s.state.phase == "LIVE_CONFIRMED"
    assert s.state.stale == (first != start)
    s.offline()
    s.worker()
    expected = "SENT" if first == start else "EXPIRED"
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", expected)]


def test_session_identity_restores_with_long_live_confirm_gap(scenario):
    s = scenario
    gap = 90  # Longer than the 68 s evidence gap limit of the default polling.
    s.restart(live_confirm_gap_seconds=gap)
    for _ in range(4):  # Routine 30 s polls; only reads 90 s apart pair up.
        s.feed("OFFLINE", 30)
    start = int(s.t)
    for _ in range(4):
        s.feed("LIVE", 30, started_at=start)
    assert s.worker()
    replay_during_session(s, "RSS", start)
    s.restart(s.c.offline_restart_grace_seconds + 1)
    for _ in range(3):
        s.feed("LIVE", 30, started_at=start)
        assert s.state.stale
    s.feed("LIVE", 30, started_at=start)  # 90 s after the first matching read.
    assert not s.state.stale
    s.offline()
    s.worker()
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT")]


def test_start_correction_confirms_with_long_live_confirm_gap(scenario):
    s = scenario
    gap = 90
    s.restart(live_confirm_gap_seconds=gap)
    for _ in range(4):
        s.feed("REPLAY", 30, started_at=REPLAY_START)
    assert s.state.confirmed == "OFFLINE"
    for _ in range(6):  # Two guarded reads, then a pair 90 s apart.
        s.feed("LIVE", 30, started_at=REPLAY_START)
    assert s.worker() and s.state.session_started_at is None
    corrected = int(s.t) - 30
    for _ in range(4):  # The fourth read is 90 s after the first.
        assert s.state.session_started_at is None
        s.feed("LIVE", 30, started_at=corrected)
    assert s.state.session_started_at == corrected
    assert s.events() == [("LIVE", "SENT")]


OWN, OTHER = "own", "other"


@pytest.mark.parametrize(
    "gap,reads,restored",
    [
        # A contradicting read between two matching ones breaks the run.
        (10, [(30, OWN), (5, OTHER), (5, OWN)], False),
        # The matching read that ends the pending end still starts a run.
        (10, [(30, OTHER), (10, OWN), (30, OWN)], True),
        # No sampling for longer than the evidence gap limit breaks the run.
        (90, [(30, OWN), (90, OWN)], False),
    ],
)
def test_pending_end_and_live_reads_share_one_identity_run(
    scenario, gap, reads, restored
):
    s = scenario
    start = 1020
    s.restart(live_confirm_gap_seconds=gap)
    step = min(gap, 30)  # Routine polls, or the faster confirmation recheck.
    reads_per_pair = gap // step + 1  # The last read is `gap` after the first.
    for _ in range(reads_per_pair):
        s.feed("OFFLINE", step)
    assert s.state.confirmed == "OFFLINE"
    for _ in range(reads_per_pair):
        s.feed("LIVE", step, started_at=start)
    assert s.state.confirmed == "LIVE_CONFIRMED"
    assert s.worker()
    replay_during_session(s, "RSS", start)
    s.restart(s.c.offline_restart_grace_seconds + 1)
    s.feed("OFFLINE", 30)
    assert s.state.phase == "OFFLINE_CANDIDATE"
    for delta, which in reads:
        s.feed("LIVE", delta, started_at=start if which == OWN else REPLAY_START)
    assert s.state.phase == "LIVE_CONFIRMED"
    assert s.state.stale == (not restored)
    s.offline()
    s.worker()
    expected = "SENT" if restored else "EXPIRED"
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", expected)]


# Starts missed while the service was down


@pytest.mark.parametrize("outage", [0, 600])
def test_restart_reports_start_after_last_offline_evidence(scenario, outage):
    s = scenario
    s.baseline()
    s.restart(30)
    for _ in range(outage // 30):
        s.feed("UNKNOWN", 30)
    start = s.t - 10
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events() == [("LIVE", "PENDING")]
    assert s.worker()


def test_start_in_same_second_as_last_offline_stays_silent(scenario):
    s = scenario
    s.baseline()
    offline_second = s.t
    s.restart(30)
    s.feed("LIVE", 5, started_at=offline_second)
    s.feed("LIVE", 10, started_at=offline_second)
    assert s.events() == [("LIVE", "SUPPRESSED_BASELINE")]


def test_unconfirmed_candidate_before_restart_does_not_hide_start(scenario):
    s = scenario
    s.baseline()
    start = s.t + 20
    s.feed("LIVE", 25, started_at=start)
    s.restart(30)
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events() == [("LIVE", "PENDING")]


def test_upgraded_database_falls_back_to_last_valid(scenario):
    s = scenario
    s.baseline()
    with s.db:
        s.db.execute("UPDATE monitor SET last_offline=NULL")
    s.restart(30)
    assert s.state.gap_reference == s.state.last_valid
    start = s.t - 10
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events() == [("LIVE", "PENDING")]


def test_old_database_gains_new_monitor_columns(tmp_path):
    from contextlib import closing

    from live_alert.storage import connect, initialize

    c = config(data_dir=str(tmp_path))
    with closing(connect(c.db)) as db:
        db.execute("CREATE TABLE monitor (room INTEGER PRIMARY KEY, confirmed TEXT)")
        initialize(db, c)
        columns = {r[1] for r in db.execute("PRAGMA table_info(monitor)")}
    assert {"mail_ready", "last_offline", "replay_at", "replay_start"} <= columns


# Brief drops that return with a newer platform start


@pytest.mark.parametrize("rss_interrupt", [False, True])
def test_brief_drop_with_new_start_continues_session(scenario, rss_interrupt):
    s = scenario
    live_session(s)
    s.feed("OFFLINE", 30)
    s.feed("OFFLINE", 30)
    if rss_interrupt:
        s.state.interrupt("possible_live")
    resumed = s.t + 25
    s.feed("LIVE", 30, started_at=resumed)
    s.feed("LIVE", 10, started_at=resumed)
    assert s.state.session == 1 and s.state.phase == "LIVE_CONFIRMED"
    assert s.state.session_started_at == resumed
    stored = s.db.execute("SELECT started_at FROM sessions WHERE id=1").fetchone()[0]
    assert stored == resumed
    s.offline()
    assert s.worker()
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT")]


def test_new_start_after_long_evidence_gap_still_rotates(scenario):
    s = scenario
    live_session(s)
    s.feed("OFFLINE", 30)
    for _ in range(10):
        s.feed("UNKNOWN", 30)
    new_start = s.t
    s.feed("LIVE", 5, started_at=new_start)
    s.feed("LIVE", 10, started_at=new_start)
    assert s.state.session == 2
    assert s.events() == [
        ("LIVE", "SENT"),
        ("OFFLINE", "SUPPRESSED_SESSION_CHANGE"),
        ("LIVE", "PENDING"),
    ]


# Adapter start times


def sample(room):
    with patch.object(web, "_http_get_json", return_value={"room": room}):
        return asyncio.run(Douyu(config()).sample())


@pytest.mark.parametrize("lead,trusted", [(60, True), (1000, False)])
def test_slightly_future_start_is_trusted(lead, trusted):
    start = int(time.time()) + lead
    o = sample(
        {"room_id": ROOM, "show_status": 1, "videoLoop": 0, "show_time": str(start)}
    )
    assert o.state == "LIVE" and o.started_at == (start if trusted else None)


def test_replay_observation_keeps_show_time():
    o = sample({"room_id": ROOM, "show_status": 1, "videoLoop": 1, "show_time": 100})
    assert o.state == "REPLAY" and o.started_at == 100


# A stuck event loop must exit the process


def test_loop_watchdog_exits_after_stall():
    ticks, exits = iter([100, 121]), []
    loop_watchdog(
        config(), lambda: 0, threading.Event(), lambda: next(ticks), exits.append, 0
    )
    assert exits == [1]


def test_loop_watchdog_quiet_while_alive_and_stops_on_halt():
    halt, exits, calls = threading.Event(), [], []

    def clock():
        calls.append(1)
        if len(calls) == 5:
            halt.set()
        return 1000.0

    loop_watchdog(config(), lambda: 990.0, halt, clock, exits.append, 0)
    assert not exits and len(calls) == 5


def test_heartbeat_marks_loop_alive(scenario):
    s = scenario
    service = Service(s.c, s.db, Douyu(s.c), lambda: s.t, lambda: s.t)
    service.loop_alive = 0

    async def one_beat():
        with suppress(TimeoutError):
            await asyncio.wait_for(service.heartbeat(), 0.1)

    asyncio.run(one_beat())
    assert service.loop_alive > 0


def test_service_exit_stops_watchdog_thread(scenario):
    s = scenario

    class Stuck:
        async def events(self):
            await asyncio.Event().wait()
            yield "connected"

        async def sample(self):
            s.t += 400  # The request itself hangs past the sampler limit.
            await asyncio.Event().wait()

    async def exercise():
        service = Service(s.c, s.db, Stuck(), lambda: s.t, lambda: s.t)
        with pytest.raises(RuntimeError, match="progress"):
            await asyncio.wait_for(service.run(), 3)

    asyncio.run(exercise())
    for thread in threading.enumerate():
        if thread.name == "loop-watchdog":
            thread.join(1)
            assert not thread.is_alive()


# Offline mail reports the broadcast length


@pytest.mark.parametrize(
    "seconds,text", [(59, "1 分钟"), (3600, "1 小时 0 分钟"), (-30, "0 分钟")]
)
def test_duration_text(seconds, text):
    assert duration(seconds) == text


def test_offline_mail_duration_uses_original_start_after_resume(scenario):
    s = scenario
    live_session(s, start=1020)
    s.feed("OFFLINE", 30)
    s.feed("OFFLINE", 30)
    resumed = s.t + 25
    s.feed("LIVE", 30, started_at=resumed)
    s.feed("LIVE", 10, started_at=resumed)
    for _ in range(6):
        s.feed("LIVE", 600, started_at=resumed)
    s.offline()
    assert s.worker()
    body = text(s.sent[-1])
    # 1020 -> first offline sample 60 s after the last live one: 63 minutes.
    assert body.splitlines()[-1] == "直播时长：约 1 小时 3 分钟"


def test_offline_mail_duration_unknown_without_trusted_start(scenario):
    s = scenario
    s.baseline()
    s.live()
    s.worker()
    s.offline()
    assert s.worker()
    assert "直播时长：未知" in text(s.sent[-1])


@pytest.mark.parametrize("start", [1020, None])
def test_status_preserves_original_live_latency_after_resume(scenario, start):
    from live_alert.cli import room_status
    from live_alert.state import local

    s = scenario
    live_session(s, start=start)
    accepted = s.t
    s.feed("OFFLINE", 30)
    resumed = s.t + 20
    s.feed("LIVE", 30, started_at=resumed)
    s.offline()
    assert s.worker()
    live, offline = room_status(s.db, s.c)["latest"]
    assert live["platform_started_at"] == (local(start) if start else None)
    assert live["platform_start_to_accept_seconds"] == (
        accepted - start if start else None
    )
    assert offline["platform_started_at"] == local(resumed)


def test_migration_backfills_first_start(scenario):
    from live_alert.cli import room_status

    s = scenario
    live_session(s, start=1020)
    with s.db:
        s.db.execute("ALTER TABLE sessions DROP COLUMN first_started_at")
    # status is read-only and can run before the first upgraded service start.
    assert room_status(s.db, s.c)["latest"][0]["platform_start_to_accept_seconds"] == 30
    initialize(s.db, s.c)
    row = s.db.execute("SELECT first_started_at FROM sessions WHERE id=1").fetchone()
    assert row[0] == 1020


def test_interrupted_first_start_migration_is_retried(scenario):
    import sqlite3

    s = scenario
    live_session(s, start=1020)
    with s.db:
        s.db.execute("ALTER TABLE sessions DROP COLUMN first_started_at")
        # Stands in for a crash after the column is added, before the backfill.
        s.db.execute("""CREATE TRIGGER interrupt BEFORE UPDATE ON sessions
                        BEGIN SELECT RAISE(ABORT, 'interrupted'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="interrupted"):
        initialize(s.db, s.c)
    columns = {r[1] for r in s.db.execute("PRAGMA table_info(sessions)")}
    assert "first_started_at" not in columns  # Rolled back with the backfill.
    with s.db:
        s.db.execute("DROP TRIGGER interrupt")
    initialize(s.db, s.c)
    row = s.db.execute("SELECT first_started_at FROM sessions WHERE id=1").fetchone()
    assert row[0] == 1020


# Generic environment variables must not block startup


def test_unused_generic_env_vars_do_not_block_startup():
    from live_alert.config import Config

    c = Config.load(
        {"TZ": "UTC", "PLATFORM": "linux", "INITIAL_BASELINE_SILENT": "true"}
    )
    assert c.notification_mode == "dry-run"
    with pytest.raises(ValueError, match="INITIAL_BASELINE_SILENT"):
        Config.load({"INITIAL_BASELINE_SILENT": "false"})


# An idle mail consumer must not contend for the write lock


def test_idle_mail_consumer_skips_write_lock(scenario):
    from contextlib import closing

    from live_alert.mail import process_one
    from live_alert.storage import connect

    s = scenario
    s.baseline()
    with closing(connect(s.c.db)) as other:
        other.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        # Before the fix this waited out the 5 s busy timeout and then raised.
        assert process_one(s.c, lambda *a: pytest.fail("nothing to send")) is False
        assert time.monotonic() - started < 1
        other.rollback()
    s.live()
    assert s.worker() and s.events() == [("LIVE", "SENT")]


# Worker protocol failures are reported, not raised


@pytest.mark.parametrize("output", ["not json", "[1, 2]", '{"unexpected": 1}', "7"])
def test_malformed_worker_output_is_protocol_error(monkeypatch, output):
    import sys

    from live_alert.http_worker import bounded_fetch

    original = asyncio.create_subprocess_exec

    async def spawn(*args, **kwargs):
        return await original(sys.executable, "-c", f"print({output!r})", **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr("live_alert.douyu.fetch_room", bounded_fetch)
    o = asyncio.run(Douyu(config()).sample())
    assert (o.state, o.reason) == ("UNKNOWN", "WorkerProtocolError")


# Mail headers and recipients


def test_mail_has_date_and_retry_stable_message_id(scenario):
    s = scenario
    s.baseline()
    s.live()
    assert s.worker("RETRYING")
    s.t += 10
    s.feed("LIVE", 0)
    assert s.worker()
    first, retry = s.sent
    assert first["Date"] and first["Message-ID"]
    assert first["Message-ID"] == retry["Message-ID"]
    assert first["Message-ID"].endswith("@example.org>")


def test_changed_recipient_never_gets_queued_notices(scenario):
    s = scenario
    s.baseline()
    s.live()
    s.restart(5, smtp_to_address="new@example.org")
    s.feed("LIVE", 5)
    assert not s.worker()
    assert not s.sent
    assert s.events() == [("LIVE", "SUPPRESSED_RECIPIENT_CHANGED")]


# Restore and restart must not repeat notices


def restored(s, backup, tmp_path, delay):
    from live_alert.cli import snapshot
    from live_alert.storage import connect

    target = tmp_path / "restored"
    target.mkdir()
    with patch("live_alert.cli.time.time", return_value=s.t + delay):
        snapshot(backup, target / "live_alert.db", restore=True)
    s.db = connect(target / "live_alert.db")
    s.restart(delay, data_dir=str(target))


def test_restore_does_not_resend_live_notice_sent_after_snapshot(scenario, tmp_path):
    from live_alert.cli import snapshot

    s = scenario
    s.baseline()
    backup = tmp_path / "offline.sqlite"
    snapshot(s.c.db, backup)
    start = s.t + 20
    s.feed("LIVE", 25, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.worker()
    restored(s, backup, tmp_path, 30)
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert not s.worker()
    assert s.events() == [("LIVE", "SUPPRESSED_RESTORE")]
    # Once seen offline after the restore, the next broadcast is announced.
    s.offline()
    new = s.t + 10
    s.feed("LIVE", 20, started_at=new)
    s.feed("LIVE", 10, started_at=new)
    assert s.events()[-1] == ("LIVE", "PENDING")


def test_restore_accepts_snapshot_from_older_schema(scenario, tmp_path):
    from contextlib import closing

    from live_alert.cli import snapshot
    from live_alert.storage import connect

    s = scenario
    s.baseline()
    old = tmp_path / "old.sqlite"
    snapshot(s.c.db, old)
    with closing(connect(old)) as db, db:
        db.execute("ALTER TABLE monitor DROP COLUMN restored_at")
    restored(s, old, tmp_path, 30)
    assert s.state.restored_at is not None


def test_restore_cannot_resend_when_platform_clock_is_ahead(scenario, tmp_path):
    from live_alert.cli import snapshot

    s = scenario
    s.baseline()
    backup = tmp_path / "before-live.sqlite"
    snapshot(s.c.db, backup)
    start = s.t + 100
    s.feed("LIVE", 25, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.worker()
    restored(s, backup, tmp_path, 5)
    assert s.t < start
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events() == [("LIVE", "SUPPRESSED_RESTORE")]
    assert not s.worker() and len(s.sent) == 1


@pytest.mark.parametrize("offset,deliver", [(120, False), (121, True)])
def test_restore_clock_skew_boundary(scenario, tmp_path, offset, deliver):
    from live_alert.cli import snapshot

    s = scenario
    s.baseline()
    backup = tmp_path / "offline.sqlite"
    snapshot(s.c.db, backup)
    restored(s, backup, tmp_path, 5)
    start = s.t + offset
    s.feed("LIVE", offset, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.worker() == deliver
    assert s.events() == [("LIVE", "SENT" if deliver else "SUPPRESSED_RESTORE")]


def test_restart_during_brief_drop_still_continues_session(scenario):
    s = scenario
    live_session(s)
    s.feed("OFFLINE", 30)
    s.restart(2)
    resumed = s.t + 20
    s.feed("LIVE", 25, started_at=resumed)
    s.feed("LIVE", 10, started_at=resumed)
    assert s.state.session == 1
    assert s.events() == [("LIVE", "SENT")]


# Hostile or malformed upstream data


def test_unstorable_title_characters_are_dropped(scenario):
    s = scenario
    # A lone surrogate cannot be stored by SQLite; U+202E reorders displayed text.
    title = "正常\ud800标题\u202e"
    o = sample({"room_id": ROOM, "show_status": 1, "videoLoop": 0, "room_name": title})
    assert o.title == "正常标题"
    s.baseline()
    s.feed("LIVE", 30, title=o.title)
    s.feed("LIVE", 10, title=o.title)
    assert s.worker()
    assert "标题: 正常标题" in text(s.sent[-1]).splitlines()
    # Defence in depth: raw text reaching the state machine is cleaned as well.
    s.offline()
    s.feed("LIVE", 30, title=title, owner="主播\ud800")
    s.feed("LIVE", 10, title=title, owner="主播\ud800")
    assert s.worker()
    assert s.sent[-1]["Subject"] == "🎙️ 主播 开播啦～"


def test_non_utf8_env_value_is_rejected_with_hint():
    from live_alert.config import Config

    # How os.environ presents "123=某主播" written to .env in GBK.
    gbk = "123=" + "某主播".encode("gbk").decode("utf-8", "surrogateescape")
    with pytest.raises(ValueError, match="UTF-8"):
        Config.load({"DOUYU_ROOMS": gbk})


def test_oversized_worker_output_is_protocol_error(monkeypatch):
    import sys

    from live_alert.http_worker import MAX_OUTPUT_BYTES, bounded_fetch

    original = asyncio.create_subprocess_exec

    async def spawn(*args, **kwargs):
        script = f"import sys; sys.stdout.write('x' * {MAX_OUTPUT_BYTES * 4})"
        return await original(sys.executable, "-c", script, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr("live_alert.douyu.fetch_room", bounded_fetch)
    o = asyncio.run(Douyu(config()).sample())
    assert (o.state, o.reason) == ("UNKNOWN", "WorkerProtocolError")


# Scheduling


def test_waiting_for_a_slot_is_not_a_sampler_stall(scenario):
    s = scenario
    service = Service(s.c, s.db, Douyu(s.c), lambda: s.t, lambda: s.t)
    service.queued = True
    s.t += 10_000

    async def one_beat():
        with suppress(TimeoutError):
            await asyncio.wait_for(service.heartbeat(), 0.1)

    asyncio.run(one_beat())  # Raises "stopped making progress" if counted.
    service.queued = False
    with pytest.raises(RuntimeError, match="progress"):
        asyncio.run(one_beat())


def test_repeatedly_disproved_rss_cannot_block_confirmation(scenario):
    s = scenario
    s.baseline()
    service = Service(s.c, s.db, Douyu(s.c), lambda: s.t, lambda: s.t)
    s.state = service.state

    def http(value, delta):
        o = s.feed(value, delta)
        # What sample_loop does with an accepted observation's verdict.
        service.judge_rss(o, superseded=False)
        return o

    for _ in range(2):  # Injected offline hints, each disproved by HTTP.
        http("LIVE", 30)
        service.event("possible_offline")
        assert s.state.window is None
    http("LIVE", 30)
    service.event("possible_offline")  # Now untrusted: the window survives.
    assert s.state.window is not None
    http("LIVE", 10)
    assert s.state.session == 1
    service.event("possible_offline")
    http("OFFLINE", 30)  # HTTP agrees again: RSS is trusted once more.
    assert service.rss_strikes == 0


# Missed starts after any evidence gap (restart while live, in-process outage)


@pytest.mark.parametrize(
    "age,expected", [(5 * 3600, "SUPPRESSED_BASELINE"), (300, "PENDING")]
)
def test_new_broadcast_after_downtime_while_live_obeys_missed_window(
    scenario, age, expected
):
    s = scenario
    live_session(s)
    s.restart(3 * 86400)
    start = s.t - age
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events()[1:] == [
        ("OFFLINE", "SUPPRESSED_SESSION_CHANGE"),
        ("LIVE", expected),
    ]


@pytest.mark.parametrize("window,deliver", [(0, False), (15, False), (900, True)])
def test_short_restart_while_live_honors_missed_window(scenario, window, deliver):
    s = scenario
    live_session(s)
    start = s.t + 20
    s.restart(40, startup_missed_event_window_seconds=window)
    s.feed("LIVE", 1, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.worker() == deliver
    assert s.events() == [
        ("LIVE", "SENT"),
        ("OFFLINE", "SUPPRESSED_SESSION_CHANGE"),
        ("LIVE", "SENT" if deliver else "SUPPRESSED_BASELINE"),
    ]


def test_observed_switch_after_restart_with_catchup_disabled(scenario):
    s = scenario
    live_session(s, start=1020)
    s.restart(40, startup_missed_event_window_seconds=0)
    s.feed("LIVE", 1, started_at=1020)
    start = s.t + 5
    s.feed("LIVE", 10, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.worker() and s.events()[-1] == ("LIVE", "SENT")


def test_restart_during_switch_keeps_evidence_before_candidate(scenario):
    s = scenario
    live_session(s)
    start = s.t + 5
    s.feed("LIVE", 10, started_at=start)
    s.restart(5)
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.worker() and s.events()[-1] == ("LIVE", "SENT")


def test_missing_show_time_after_restart_does_not_silence_later_switch(scenario):
    s = scenario
    live_session(s)
    s.restart(40, startup_missed_event_window_seconds=0)
    for _ in range(6):
        s.feed("LIVE", 30)
    start = s.t + 5
    s.feed("LIVE", 10, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.worker() and s.events()[-1] == ("LIVE", "SENT")


@pytest.mark.parametrize(
    "window,age,expected",
    [
        (0, 300, "SUPPRESSED_BASELINE"),
        (900, 2 * 3600, "SUPPRESSED_BASELINE"),
        (900, 300, "PENDING"),
    ],
)
def test_start_during_in_process_outage_obeys_missed_window(
    scenario, window, age, expected
):
    s = scenario
    s.c = s.state.c = replace(s.c, startup_missed_event_window_seconds=window)
    s.baseline()
    for _ in range(360):  # Three hours without a valid HTTP answer.
        s.feed("UNKNOWN", 30)
    start = s.t - age
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events() == [("LIVE", expected)]


def test_normal_detection_is_not_treated_as_missed(scenario):
    s = scenario
    s.c = s.state.c = replace(s.c, startup_missed_event_window_seconds=0)
    s.baseline()
    for _ in range(3):  # A few failures between valid polls are not a gap.
        s.feed("UNKNOWN", 10)
    s.feed("OFFLINE", 5)
    s.feed("LIVE", 30)
    s.feed("LIVE", 10)
    assert s.events() == [("LIVE", "PENDING")]


# Restore guard survives flicker, clears after a full quiet window


def test_restore_guard_survives_one_offline_flicker(scenario, tmp_path):
    from live_alert.cli import snapshot

    s = scenario
    s.baseline()
    backup = tmp_path / "offline.sqlite"
    snapshot(s.c.db, backup)
    start = s.t + 20
    s.feed("LIVE", 25, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.worker()
    restored(s, backup, tmp_path, 30)
    s.feed("OFFLINE", 5)  # A brief flicker, far shorter than an offline window.
    s.feed("OFFLINE", 5)
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events() == [("LIVE", "SUPPRESSED_RESTORE")]


def test_restore_guard_clears_after_quiet_window(scenario, tmp_path):
    from live_alert.cli import snapshot

    s = scenario
    s.baseline()
    backup = tmp_path / "offline.sqlite"
    snapshot(s.c.db, backup)
    restored(s, backup, tmp_path, 30)
    for _ in range(5):
        s.feed("OFFLINE", 30)  # 120 s continuously off after the restore.
    assert s.state.restored_at is None
    s.feed("LIVE", 30)  # Start time unknown, yet clearly after the restore.
    s.feed("LIVE", 10)
    assert s.events() == [("LIVE", "PENDING")]


@pytest.mark.parametrize("interruption", ["unknown", "sampling_pause", "replay_guard"])
def test_restore_guard_requires_uninterrupted_offline_evidence(
    scenario, tmp_path, interruption
):
    from live_alert.cli import snapshot

    s = scenario
    s.baseline()
    backup = tmp_path / "before-live.sqlite"
    snapshot(s.c.db, backup)
    start = s.t + 3
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.worker()
    restored(s, backup, tmp_path, 30)
    s.feed("REPLAY", 5, started_at=start - 100)
    s.feed("OFFLINE", 30)
    if interruption == "sampling_pause":
        s.t += 120
    else:
        for _ in range(4):
            if interruption == "unknown":
                s.feed("UNKNOWN", 30)
            else:
                s.feed("REPLAY", 15, started_at=start - 100)
                s.feed("LIVE", 15, started_at=start - 100)
    s.feed("OFFLINE", 5)
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events() == [("LIVE", "SUPPRESSED_RESTORE")]
    assert not s.worker()


@pytest.mark.parametrize("old_session", [False, True])
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize(
    "age,expected", [(7200, "SUPPRESSED_BASELINE"), (100, "PENDING")]
)
def test_interrupted_recovery_keeps_pre_outage_reference(
    scenario, old_session, restart, age, expected
):
    s = scenario
    if old_session:
        live_session(s)
    else:
        s.baseline()
    s.feed("UNKNOWN", 3 * 3600)
    start = s.t - age
    s.feed("LIVE", 5, started_at=start)
    s.feed("UNKNOWN", 5)
    if restart:
        s.restart()
    s.feed("LIVE", 5, started_at=start)
    s.feed("LIVE", 10, started_at=start)
    assert s.events()[-1] == ("LIVE", expected)
    assert s.worker() == (expected == "PENDING")


# A queued offline notice survives a live-looking flicker


def test_queued_offline_notice_survives_live_flicker(scenario):
    s = scenario
    live_session(s)
    s.offline()
    assert s.worker("RETRYING")
    s.feed("LIVE", 5)
    s.t += 10
    assert not s.worker()
    s.feed("OFFLINE", 10)
    assert s.worker()
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT")]


# Injected RSS through the real sampler cannot hold a room down


def test_rss_injected_into_every_request_cannot_block_confirmation(scenario):
    s = scenario
    s.baseline()
    samples = []

    class Injected:
        async def events(self):
            await asyncio.Event().wait()
            yield "connected"

        async def sample(self):
            # A forged "offline" hint lands while every request is in flight.
            service.event("possible_offline")
            samples.append(s.t)
            if s.state.session is not None or len(samples) > 40:
                service.stop.set()
            return Observation("LIVE", "fixture", s.t, s.t, s.t, started_at=990)

    service = Service(s.c, s.db, Injected(), lambda: s.t, lambda: s.t)
    s.state = service.state

    async def wait(delay):
        s.t += delay

    async def exercise():
        with patch.object(service, "wait_for_sample", wait):
            await service.sample_loop()

    asyncio.run(exercise())
    assert s.state.session is not None, f"never confirmed after {len(samples)} samples"
    assert len(samples) <= 6


def test_offline_confirm_samples_upper_bound():
    from live_alert.config import Config

    with pytest.raises(ValueError, match="OFFLINE_CONFIRM_SAMPLES"):
        Config.load({"OFFLINE_CONFIRM_SAMPLES": "11"})
    assert Config.load({"OFFLINE_CONFIRM_SAMPLES": "10"})
