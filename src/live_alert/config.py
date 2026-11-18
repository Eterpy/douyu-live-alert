import os
from collections.abc import Mapping
from dataclasses import dataclass, field, fields, replace
from email.errors import HeaderParseError
from email.headerregistry import Address
from pathlib import Path
from typing import Any

MAX_ROOMS = 20
# Per-room view fields, filled from DOUYU_ROOMS by for_room(), never read from env.
ROOM_FIELDS = ("room_id", "room_name")


@dataclass(frozen=True)
class Config:
    room_id: int = 0
    room_name: str = ""
    rooms: str = ""
    transport: str = "tcp"
    notification_mode: str = "dry-run"
    data_dir: str = "./data"
    poll_interval_seconds: int = 30
    live_confirm_samples: int = 2
    live_confirm_gap_seconds: int = 10
    offline_confirm_seconds: int = 120
    offline_confirm_samples: int = 3
    offline_final_evidence_max_age_seconds: int = 30
    offline_restart_grace_seconds: int = 600
    max_notification_age_seconds: int = 900
    startup_missed_event_window_seconds: int = 900
    observation_retention_days: int = 30
    http_timeout_seconds: int = 8
    backoff_initial_seconds: int = 5
    backoff_max_seconds: int = 300
    notify_live: bool = True
    notify_offline: bool = True
    offline_requires_live_email: bool = True
    notify_offline_for_baseline_live: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_security: str = "starttls"
    smtp_username: str = field(default="", repr=False)
    smtp_password: str = field(default="", repr=False)
    smtp_from_address: str = ""
    smtp_from_name: str = "斗鱼开播提醒"
    smtp_to_address: str = ""
    smtp_timeout_seconds: int = 15
    smtp_total_timeout_seconds: int = 45
    smtp_preconnect: bool = True
    smtp_max_attempts: int = 4
    log_level: str = "INFO"
    app_env: str = "production"

    @property
    def db(self) -> Path:
        return Path(self.data_dir) / "live_alert.db"

    def require_smtp(self) -> None:
        for key in (
            "smtp_host",
            "smtp_username",
            "smtp_password",
            "smtp_from_address",
            "smtp_to_address",
        ):
            value = getattr(self, key)
            if (
                not value
                or value.startswith("REPLACE_")
                or value.endswith("@example.invalid")
            ):
                raise ValueError(f"请设置 {key.upper()} 再发送邮件")

    def room_list(self) -> list[tuple[int, str]]:
        """Parse DOUYU_ROOMS: comma-separated `room_id` or `room_id=display name`."""
        rooms: list[tuple[int, str]] = []
        for item in self.rooms.split(","):
            if not item.strip():
                continue
            raw_id, _, name = item.partition("=")
            raw_id = raw_id.strip()
            # int() would also accept "1_0001", "+1" and full-width digits.
            if not (raw_id.isascii() and raw_id.isdigit()):
                raise ValueError(f"DOUYU_ROOMS 中的房间号无效：{raw_id!r}")
            room_id = int(raw_id)
            if not 1 <= room_id <= 2**63 - 1:
                raise ValueError(f"DOUYU_ROOMS 中的房间号无效：{room_id}")
            rooms.append((room_id, name.strip()))
        if not rooms:
            raise ValueError("请在 DOUYU_ROOMS 中至少配置一个房间号")
        if len({room_id for room_id, _ in rooms}) != len(rooms):
            raise ValueError("DOUYU_ROOMS 含重复的房间号")
        if len(rooms) > MAX_ROOMS:
            raise ValueError(f"DOUYU_ROOMS 最多 {MAX_ROOMS} 个房间")
        return rooms

    def for_room(self, room_id: int, room_name: str = "") -> "Config":
        return replace(self, room_id=room_id, room_name=room_name)

    def room_configs(self, only: int | None = None) -> list["Config"]:
        rooms = self.room_list()
        if only is not None:
            rooms = [r for r in rooms if r[0] == only]
            if not rooms:
                raise ValueError(f"房间 {only} 不在 DOUYU_ROOMS 中")
        return [self.for_room(room_id, name) for room_id, name in rooms]

    @classmethod
    def load(cls, env: Mapping[str, str] | None = None) -> "Config":
        env = os.environ if env is None else env
        for legacy in ("DOUYU_ROOM_ID", "DOUYU_ROOM_NAME"):
            if legacy in env:
                raise ValueError(
                    f"{legacy} 已改为 DOUYU_ROOMS，例如 DOUYU_ROOMS=123456=主播名称"
                )
        # Older .env files carry this; silence on first start cannot be turned off.
        if env.get("INITIAL_BASELINE_SILENT", "true").lower() != "true":
            raise ValueError(
                "INITIAL_BASELINE_SILENT 不支持关闭：首次启动时已在播的场次总是静默"
            )
        aliases = {
            "rooms": "DOUYU_ROOMS",
            "transport": "DOUYU_TRANSPORT",
        }
        kwargs: dict[str, Any] = {}
        known = set()
        for item in fields(cls):
            if item.name in ROOM_FIELDS:
                continue
            key = aliases.get(item.name, item.name.upper())
            known.add(key)
            if key not in env:
                continue
            value = env[key]
            if item.type is bool:
                if value.lower() not in ("true", "false"):
                    raise ValueError(f"{key} 应为 true 或 false")
                kwargs[item.name] = value.lower() == "true"
            elif item.type is int:
                try:
                    kwargs[item.name] = int(value)
                except ValueError:
                    raise ValueError(f"{key} 应为整数") from None
            else:
                if any(ord(c) < 32 for c in value):
                    raise ValueError(f"{key} 含控制字符")
                try:
                    # Non-UTF-8 bytes (e.g. a GBK-saved .env) decode to surrogates.
                    value.encode("utf-8")
                except UnicodeEncodeError:
                    raise ValueError(
                        f"{key} 不是有效的 UTF-8 文本，请用 UTF-8 编码保存 .env"
                    ) from None
                kwargs[item.name] = value
        prefixes = (
            "DOUYU_",
            "SMTP_",
            "NOTIFICATION_",
            "NOTIFY_",
            "OFFLINE_",
            "LIVE_",
            "POLL_",
            "STARTUP_MISSED_",
            "HTTP_TIMEOUT_",
            "MAX_NOTIFICATION_",
            "OBSERVATION_RETENTION_",
            "BACKOFF_",
        )
        for key in env:
            if key.startswith(prefixes) and key not in known:
                raise ValueError(f"未知配置 {key}")
        config = cls(**kwargs)
        limits = {
            "poll_interval_seconds": (30, 300),
            "live_confirm_samples": (2, 20),
            "live_confirm_gap_seconds": (10, 300),
            "offline_confirm_seconds": (120, 86400),
            # More samples per window would keep ending rooms on the HTTP pool.
            "offline_confirm_samples": (3, 10),
            "smtp_port": (1, 65535),
            "http_timeout_seconds": (1, 120),
            "smtp_timeout_seconds": (1, 90),
            "smtp_total_timeout_seconds": (1, 90),
            "smtp_max_attempts": (1, 20),
            "startup_missed_event_window_seconds": (0, 86400),
            "offline_restart_grace_seconds": (0, 86400),
        }
        for item in fields(cls):
            if item.type is int and item.name not in ROOM_FIELDS:
                lo, hi = limits.get(item.name, (1, 31536000))
                if not lo <= getattr(config, item.name) <= hi:
                    raise ValueError(
                        f"{aliases.get(item.name, item.name.upper())} 应在 {lo}–{hi} 之间"
                    )
        if config.rooms.strip():
            config.room_list()
        if config.offline_confirm_seconds >= config.max_notification_age_seconds:
            raise ValueError(
                "MAX_NOTIFICATION_AGE_SECONDS 必须大于 OFFLINE_CONFIRM_SECONDS"
            )
        if config.smtp_timeout_seconds > config.smtp_total_timeout_seconds:
            raise ValueError("SMTP_TOTAL_TIMEOUT_SECONDS 不得小于 SMTP_TIMEOUT_SECONDS")
        choices = {
            "notification_mode": ("dry-run", "send"),
            "transport": ("tcp", "ws"),
            "smtp_security": ("starttls", "ssl"),
            "log_level": ("DEBUG", "INFO", "WARNING", "ERROR"),
        }
        for name, options in choices.items():
            if getattr(config, name) not in options:
                raise ValueError(f"{name.upper()} 可用值 {options}")
        if (
            config.smtp_port == 587
            and config.smtp_security != "starttls"
            or (config.smtp_port == 465 and config.smtp_security != "ssl")
        ):
            raise ValueError("SMTP_PORT 与 SMTP_SECURITY 不匹配")
        if config.backoff_max_seconds < config.backoff_initial_seconds:
            raise ValueError("BACKOFF_MAX_SECONDS 小于 BACKOFF_INITIAL_SECONDS")
        for name in ("data_dir", "smtp_from_name", "app_env"):
            if not getattr(config, name).strip():
                raise ValueError(f"{name.upper()} 不能为空")
        for name in ("smtp_from_address", "smtp_to_address"):
            value = getattr(config, name)
            if not value:
                continue  # Required only for sending; see require_smtp().
            try:
                addr = Address(addr_spec=value)
                if not addr.username or not addr.domain or not value.isascii():
                    raise ValueError()
            except (ValueError, HeaderParseError):
                raise ValueError(f"{name.upper()} 需要一个有效邮箱地址") from None
        if config.notification_mode == "send":
            config.require_smtp()
        return config
