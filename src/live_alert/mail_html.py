"""HTML versions of the notices. The plain-text body stays the fallback part.

Mail clients ignore most CSS: layout uses tables and inline styles only, with no
external images or fonts. Text from the platform or configuration is escaped.
"""

from html import escape

FONT = (
    "-apple-system,BlinkMacSystemFont,'PingFang SC','Hiragino Sans GB',"
    "'Microsoft YaHei','Noto Sans CJK SC','Helvetica Neue',Arial,sans-serif"
)


def _page(
    subject: str, preheader: str, page: str, shadow: str, rows: str, footer: str
) -> str:
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light">
<title>{escape(subject)}</title>
</head>
<body style="margin:0;padding:0;background:{page};">
<div style="display:none;max-height:0;overflow:hidden;opacity:0;">{escape(preheader)}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:{page};">
<tr><td align="center" style="padding:32px 12px;">
<table role="presentation" width="560" cellpadding="0" cellspacing="0" border="0" style="width:100%;max-width:560px;background:#ffffff;border-radius:18px;overflow:hidden;box-shadow:{shadow};font-family:{FONT};">
{rows}
{footer}
</table>
</td></tr>
</table>
</body>
</html>
"""


def _title_block(
    label: str, title: str, muted: str, tint: str, accent: str, ink: str
) -> str:
    return f"""<tr><td style="padding:28px 32px 8px;">
<div style="font-size:12px;color:{muted};letter-spacing:1px;">{label}</div>
<div style="margin-top:8px;padding:14px 16px;background:{tint};border-left:4px solid {accent};border-radius:8px;font-size:18px;line-height:1.5;font-weight:600;color:{ink};word-break:break-word;">{escape(title or "未获取")}</div>
</td></tr>"""


def live_html(
    *,
    subject: str,
    name: str,
    title: str,
    room_id: int,
    confirmed_at: str,
    brand: str,
) -> str:
    url = f"https://www.douyu.com/{room_id}"
    preheader = f"{name} 正在直播：{title}" if title else f"{name} 正在直播"
    rows = f"""<tr><td bgcolor="#ff7a45" style="background:#ff7a45;background-image:linear-gradient(135deg,#ff9a3c 0%,#ff5e62 100%);padding:28px 32px 26px;">
<span style="display:inline-block;background:rgba(255,255,255,0.22);color:#ffffff;font-size:12px;font-weight:600;letter-spacing:1.5px;padding:4px 12px;border-radius:999px;"><span style="color:#fff3b0;">&#9679;</span>&nbsp;LIVE · 正在直播</span>
<div style="margin-top:14px;color:#ffffff;font-size:26px;line-height:1.35;font-weight:700;word-break:break-word;">🎙️ {escape(name)} 开播啦～</div>
<div style="margin-top:6px;color:#fff1e6;font-size:14px;">📣 直播间有新动静啦，快来看看吧</div>
</td></tr>
{_title_block("直播标题", title, "#a08670", "#fff6ef", "#ff7a45", "#3a2a1e")}
<tr><td style="padding:12px 32px 4px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="font-size:14px;color:#5b4a3d;">
<tr>
<td width="120" style="padding:10px 0;border-bottom:1px dashed #f0e2d6;color:#a08670;">⏰ 确认开播</td>
<td style="padding:10px 0;border-bottom:1px dashed #f0e2d6;font-variant-numeric:tabular-nums;">{escape(confirmed_at)}</td>
</tr>
<tr>
<td width="120" style="padding:10px 0;color:#a08670;">📺 直播间</td>
<td style="padding:10px 0;"><a href="{url}" style="color:#e8590c;text-decoration:none;">douyu.com/{room_id}</a></td>
</tr>
</table>
</td></tr>
<tr><td align="center" style="padding:22px 32px 30px;">
<a href="{url}" style="display:inline-block;background:#ff6a3d;color:#ffffff;font-size:16px;font-weight:600;text-decoration:none;padding:13px 40px;border-radius:999px;box-shadow:0 4px 12px rgba(255,106,61,0.35);">进入直播间 &rarr;</a>
</td></tr>"""
    footer = f"""<tr><td style="padding:16px 32px 22px;background:#fbf8f4;border-top:1px solid #f2ebe3;font-size:12px;line-height:1.6;color:#b3a291;text-align:center;">
✨ {escape(brand)}
</td></tr>"""
    return _page(
        subject, preheader, "#f5f1eb", "0 6px 24px rgba(120,80,40,0.10)", rows, footer
    )


def offline_html(
    *,
    subject: str,
    name: str,
    title: str,
    date: str,
    time: str,
    duration: str | None,
    duration_note: str,
    brand: str,
) -> str:
    """duration is None when it cannot be determined; duration_note says how it
    was derived or why it is unknown, or is empty."""
    preheader = f"{name} 已下播，本场{duration}" if duration else f"{name} 已下播"
    tile = "background:#f7f7fc;border-radius:12px;padding:16px 18px;vertical-align:top;"
    value = "margin-top:6px;font-size:20px;font-weight:700;color:#3b3f8f;font-variant-numeric:tabular-nums;"
    small = "margin-top:2px;font-size:12px;color:#6b6f91;"
    note_row = (
        f'<div style="{small}">{escape(duration_note)}</div>' if duration_note else ""
    )
    rows = f"""<tr><td bgcolor="#34346e" style="background:#34346e;background-image:linear-gradient(135deg,#232452 0%,#4b3f8f 100%);padding:28px 32px 26px;">
<span style="float:right;color:#c9cdf5;font-size:13px;letter-spacing:6px;">✦ · ✧ · ✦</span>
<span style="display:inline-block;background:rgba(255,255,255,0.14);color:#d9dcff;font-size:12px;font-weight:600;letter-spacing:1.5px;padding:4px 12px;border-radius:999px;">OFFLINE · 已下播</span>
<div style="margin-top:14px;color:#ffffff;font-size:26px;line-height:1.35;font-weight:700;word-break:break-word;">🌙 {escape(name)} 下播休息啦～</div>
<div style="margin-top:6px;color:#c9cdf5;font-size:14px;">直播间安静下来啦，记录放在这里～</div>
</td></tr>
{_title_block("本场标题", title, "#8a8fb5", "#f4f4fd", "#6c63d9", "#262a4a")}
<tr><td style="padding:16px 32px 24px;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
<tr>
<td width="48%" valign="top" bgcolor="#f7f7fc" style="{tile}">
<div style="font-size:12px;color:#8a8fb5;">⏱️ 直播时长</div>
<div style="{value}">{escape(duration or "未知")}</div>
{note_row}
</td>
<td width="4%" style="font-size:0;line-height:0;">&nbsp;</td>
<td width="48%" valign="top" bgcolor="#f7f7fc" style="{tile}">
<div style="font-size:12px;color:#8a8fb5;">🌃 确认下播</div>
<div style="{value}">{escape(time)}</div>
<div style="{small}">{escape(date)}</div>
</td>
</tr>
</table>
</td></tr>"""
    footer = f"""<tr><td style="padding:16px 32px 22px;background:#f8f8fc;border-top:1px solid #ececf6;font-size:12px;line-height:1.6;color:#a3a7c7;text-align:center;">
🌙 辛苦啦，下次开播再提醒你 · {escape(brand)}
</td></tr>"""
    return _page(
        subject, preheader, "#eef0f7", "0 6px 24px rgba(40,45,90,0.12)", rows, footer
    )
