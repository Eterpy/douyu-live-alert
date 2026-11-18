from dataclasses import replace

import pytest

from live_alert.douyu import Observation


def test_full_cycle_two_template_contents_and_order(scenario):
    s = scenario
    s.baseline()
    s.feed("LIVE", 30, title="fixture")
    s.feed("LIVE", 9)
    assert not s.events()
    s.feed("LIVE", 1, title="fixture")
    assert s.worker()
    s.offline()
    assert s.worker()
    assert not s.worker()
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", "SENT")]
    start, end = s.sent
    assert "[斗鱼开播]" in start["Subject"] and "[斗鱼下播]" in end["Subject"]
    for msg in s.sent:
        assert (
            "https://www.douyu.com/10001" in msg.get_content()
            and "会话：1" in msg.get_content()
        )
    assert "fixture" in start.get_content() and "+08:00" in start.get_content()
    assert (
        "首次检测到离线时间" in end.get_content()
        and "120 秒，3 次" in end.get_content()
    )
    s.live()
    assert s.state.session == 2 and s.events()[-1] == ("LIVE", "PENDING")


@pytest.mark.parametrize("interruption", ["REPLAY", "UNKNOWN", "OFFLINE"])
def test_live_candidate_interruption(scenario, interruption):
    s = scenario
    s.baseline()
    s.feed("LIVE", 20)
    s.feed(interruption, 10)
    s.feed("LIVE", 10)
    assert not s.events()


def test_unknown_and_conflicting_baseline(scenario):
    s = scenario
    for value in ["UNKNOWN", "OFFLINE", "LIVE", "UNKNOWN", "LIVE"]:
        s.feed(value, 10)
    assert s.state.confirmed == "BASELINING" and not s.events()
    s.feed("LIVE", 10)
    s.offline()
    assert s.events() == [
        ("LIVE", "SUPPRESSED_BASELINE"),
        ("OFFLINE", "SUPPRESSED_BASELINE"),
    ]


@pytest.mark.parametrize("interrupt", ["LIVE", "UNKNOWN", "disconnect"])
def test_continuity_must_restart(scenario, interrupt):
    s = scenario
    s.baseline()
    s.live()
    s.feed("OFFLINE", 5)
    s.feed("REPLAY", 60)
    if interrupt == "disconnect":
        s.state.interrupt("disconnected")
    else:
        s.feed(interrupt, 59)
    s.feed("OFFLINE", 1)
    s.feed("REPLAY", 119)
    assert s.state.session == 1 and len(s.events()) == 1
    s.feed("REPLAY", 1)
    assert s.state.confirmed == "OFFLINE"


def test_distinct_independent_and_fresh_samples(scenario):
    s = scenario
    s.baseline()
    s.live()
    first = s.feed("OFFLINE", 20)
    for _ in range(100):
        s.state.observe(first, "duplicate", s.t + 121)
    # Duplicate HTTP requests with equal timestamps cannot inflate the count.
    s.feed("OFFLINE")
    s.feed("OFFLINE", 120)
    assert s.state.window.count == 2
    late = Observation("REPLAY", "late", s.t + 5, s.t + 5, s.t + 5)
    s.state.observe(late, "delayed", late.end + 31)
    assert s.state.session == 1
    s.feed("OFFLINE", 40)
    assert s.events()[-1][0] == "OFFLINE"
    for _ in range(100):
        s.feed("OFFLINE", 1)
    assert len(s.events()) == 2


@pytest.mark.parametrize(
    "gap,expected", [(599, "PENDING"), (600, "PENDING"), (601, "EXPIRED")]
)
def test_restart_uses_complete_new_window(scenario, gap, expected):
    s = scenario
    s.baseline()
    s.live()
    s.worker()
    s.feed("OFFLINE", 30)
    s.feed("OFFLINE", 60)
    s.restart(gap)
    s.feed("OFFLINE")
    s.feed("OFFLINE", 119)
    assert s.state.session == 1
    s.feed("OFFLINE", 1)
    assert s.events()[-1] == ("OFFLINE", expected)


def test_restart_live_then_offline_and_missed_start(scenario):
    s = scenario
    s.baseline()
    s.live()
    s.worker()
    s.restart()
    for _ in range(100):
        s.feed("LIVE", 1)
    assert s.events() == [("LIVE", "SENT")]
    s.offline()
    s.worker()
    s.restart()
    s.feed("OFFLINE", 60)
    assert len(s.events()) == 2
    s.restart(400)
    s.live()
    s.offline()
    assert s.events()[-2:] == [
        ("LIVE", "SUPPRESSED_BASELINE"),
        ("OFFLINE", "SUPPRESSED_BASELINE"),
    ]


def test_network_outage_cannot_end_session(scenario):
    s = scenario
    s.baseline()
    s.live()
    for _ in range(20):
        s.feed("UNKNOWN", 60)
    assert len(s.events()) == 1
    s.offline()
    assert s.events()[-1] == ("OFFLINE", "EXPIRED")


def test_disabling_offline_still_rearms(scenario):
    s = scenario
    s.c = s.state.c = replace(s.c, notify_offline=False)
    s.baseline()
    s.live()
    s.offline()
    s.live()
    assert s.events() == [
        ("LIVE", "PENDING"),
        ("OFFLINE", "SUPPRESSED_DISABLED"),
        ("LIVE", "PENDING"),
    ]


@pytest.mark.parametrize(
    "start_offset,expected",
    [(-1, "SUPPRESSED_BASELINE"), (20, "PENDING"), (1000, "SUPPRESSED_BASELINE")],
)
def test_opt_in_missed_event_requires_trusted_fresh_time(
    scenario, start_offset, expected
):
    s = scenario
    s.baseline()
    started = s.t + start_offset
    s.restart(30, startup_missed_event_window_seconds=90)
    s.feed("LIVE", 1, started_at=started)
    s.feed("LIVE", 10, started_at=started)
    assert s.events() == [("LIVE", expected)]


@pytest.mark.parametrize("closing", [False, True])
def test_sampling_pause_requires_new_window(scenario, closing):
    s = scenario
    s.baseline()
    if closing:
        s.live()
        s.worker()
        s.feed("REPLAY", 10)
        s.feed("OFFLINE", 60)
        s.feed("OFFLINE", 600)
        assert s.events() == [("LIVE", "SENT")]
        assert s.state.window.count == 1
        s.feed("OFFLINE", 60)
        s.feed("OFFLINE", 60)
        assert s.events()[-1] == ("OFFLINE", "PENDING")
    else:
        # After a sampling pause the start must be trusted and recent to announce.
        s.feed("LIVE", 10)
        start = s.t + 590
        s.feed("LIVE", 600, started_at=start)
        assert not s.events()
        s.feed("LIVE", 10, started_at=start)
        assert s.events() == [("LIVE", "PENDING")]


def test_two_different_live_starts_cannot_confirm_one_session(scenario):
    s = scenario
    s.baseline()
    s.feed("LIVE", 30, started_at=s.t)
    new_start = s.t + 5
    s.feed("LIVE", 10, started_at=new_start)
    assert not s.events()
    s.feed("LIVE", 10, started_at=new_start)
    assert s.events() == [("LIVE", "PENDING")]


@pytest.mark.parametrize("new_start", [None, 1600, 1020])
def test_long_restart_reconciles_session_identity_before_pairing(scenario, new_start):
    s = scenario
    s.baseline()
    s.feed("LIVE", 30, started_at=1020)
    s.feed("LIVE", 10, started_at=1020)
    s.worker()
    s.restart(700)
    s.feed("LIVE", 1, started_at=new_start)
    s.restart(1)
    s.feed("LIVE", 1, started_at=new_start)
    s.offline()
    expect = "PENDING" if new_start == 1020 else "EXPIRED"
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", expect)]


def test_live_response_stale_at_commit_cannot_open_session(scenario):
    s = scenario
    s.baseline()
    s.feed("LIVE", 20)
    late = Observation("LIVE", "slow_consumer", s.t + 10, s.t + 10, s.t + 10)
    s.state.observe(late, "timer", late.end + 200)
    assert not s.events() and s.state.window is None


def test_host_suspend_gap_cannot_bridge_offline_window(scenario):
    s = scenario
    s.baseline()
    s.live()
    s.feed("REPLAY", 10)
    s.feed("OFFLINE", 60)
    recovered = Observation("OFFLINE", "fixture", s.t + 60, s.t + 60, s.t + 360)
    s.state.observe(recovered, "timer", recovered.end)
    assert s.state.session == 1 and s.state.window.count == 1


def test_unrecognized_observations_never_confirm_offline(scenario):
    s = scenario
    s.baseline()
    s.live()
    for _ in range(4):
        s.feed("UNRECOGNIZED", 60)
    assert s.state.session == 1 and s.state.window is None


def test_newer_start_than_previous_confirmation_cannot_pair(scenario):
    s = scenario
    s.baseline()
    s.live()
    s.worker()
    confirmed = s.t
    s.restart(120)
    s.feed("LIVE", 1, started_at=confirmed + 45)
    s.offline()
    assert s.events() == [("LIVE", "SENT"), ("OFFLINE", "EXPIRED")]
