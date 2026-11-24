import re
from datetime import datetime

from conftest import html, text

from live_alert.mail import message


def broadcast(s, title="今晚一起看比赛", owner="测试主播", start_offset=30):
    s.t = datetime.fromisoformat("2026-11-22T20:19:42+08:00").timestamp()
    s.baseline()
    started = int(s.t + start_offset) if start_offset is not None else None
    s.feed("LIVE", 30, started_at=started, title=title, owner=owner)
    s.feed("LIVE", 10, started_at=started, title=title, owner=owner)
    assert s.worker()
    s.offline()
    assert s.worker()
    return s.sent


def test_notices_carry_text_and_html_alternatives(scenario):
    live, offline = broadcast(scenario)
    for msg in (live, offline):
        assert msg.get_content_type() == "multipart/alternative"
        assert [p.get_content_type() for p in msg.iter_parts()] == [
            "text/plain",
            "text/html",
        ]
        # RFC 5322 line limit: quoted-printable keeps long HTML lines short.
        assert max(map(len, msg.as_bytes().split(b"\r\n"))) <= 998
    # The text part is unchanged for clients without HTML.
    assert text(live).splitlines()[0] == "📣 直播间有新动静啦～"
    page = html(live)
    for fragment in (
        "🎙️ 测试主播 开播啦～",
        "今晚一起看比赛",
        "2026-11-22 20:20:32",
        'href="https://www.douyu.com/10001"',
        "斗鱼开播提醒",
        "测试主播 正在直播：今晚一起看比赛",  # Inbox preview line.
    ):
        assert fragment in page
    page = html(offline)
    assert "去直播间看看" not in page
    for fragment in (
        "🌙 测试主播 下播休息啦～",
        "本场标题",
        "约 1 分钟",
        "按开播时间估算",
        "20:23:32",
        "2026-11-22",
        "测试主播 已下播，本场约 1 分钟",
    ):
        assert fragment in page


def test_html_escapes_platform_text(scenario):
    title = "<script>alert(\"x\")</script> & 'q'"
    live, offline = broadcast(scenario, title=title, owner="<b>主播</b>")
    for msg in (live, offline):
        page = html(msg)
        assert "<script>" not in page and "<b>" not in page
        assert (
            "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt; &amp; &#x27;q&#x27;"
            in page
        )
        assert "&lt;b&gt;主播&lt;/b&gt;" in page
        # Plain text shows the title as received.
        assert title in text(msg)


def test_html_without_title_or_trusted_start(scenario):
    live, offline = broadcast(scenario, title="", start_offset=None)
    assert "未获取" in html(live) and "斗鱼房间" not in html(live)
    page = html(offline)
    assert "直播时长：约 3 分钟" in text(offline)
    assert ">约 3 分钟</div>" in page
    assert "测试主播 已下播，本场约 3 分钟" in page
    assert "估算" not in page and "缺少可信开播时间" not in page


def test_configured_sender_name_is_escaped_in_footer(scenario):
    s = scenario
    s.restart(smtp_from_name="提醒 <bot>")
    live, _ = broadcast(s)
    assert "✨ 提醒 &lt;bot&gt;\n" in html(live)
    assert "独立核实" not in html(live)


def test_notice_queued_before_upgrade_stays_plain_text(scenario):
    s = scenario
    s.baseline()
    s.live()
    # A row written by 2.0.x has no HTML part.
    with s.db:
        s.db.execute("UPDATE outbox SET html=NULL")
    assert s.worker()
    msg = s.sent[-1]
    assert msg.get_content_type() == "text/plain"
    assert text(msg).startswith("📣 直播间有新动静啦～")


def test_upgrade_adds_html_column_to_outbox(scenario):
    from live_alert.storage import initialize

    s = scenario
    s.baseline()
    s.live()
    with s.db:
        s.db.execute("ALTER TABLE outbox DROP COLUMN html")
    initialize(s.db, s.c)
    columns = {r[1] for r in s.db.execute("PRAGMA table_info(outbox)")}
    assert "html" in columns
    s.feed("LIVE", 1)  # A restart waits for fresh evidence before sending.
    assert s.worker() and s.sent[-1].get_content_type() == "text/plain"


def test_message_without_html_is_unchanged(scenario):
    msg = message(scenario.c, "user@example.org", "主题", "正文")
    assert msg.get_content_type() == "text/plain"
    assert re.search(
        r"^Content-Transfer-Encoding: quoted-printable", str(msg), re.MULTILINE
    )
