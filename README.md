# 斗鱼开播提醒 · douyu-live-alert

斗鱼主播开播、下播时发送邮件提醒。一个 Docker 容器可同时监控多个直播间，所有状态都经过多次 HTTP 核实才会发信，尽量做到不误报、不重复。

- **准确**：开播需两次相隔至少 10 秒的 HTTP 确认，下播需稳定 2 分钟、至少 3 份证据；录像轮播不算开播，网络故障不会被当成下播。
- **及时**：订阅斗鱼实时状态事件，收到即核实；另有 30 秒轮询兜底。正常情况下开播后十几秒内提交邮件。
- **不重复**：每场直播每种提醒只发一次；重启、断网、进程被强杀后也不会重复发送。
- **省心**：首次启动不打扰（已在播的场次静默）；短暂断流不会重复提醒开播；下播邮件附直播时长。
- **轻量**：单容器、SQLite 存储、无需开放任何端口，内存上限 384 MiB。

> 本项目与斗鱼官方无关，依赖斗鱼网页端的非官方接口，接口变化可能导致服务失效。

## 工作方式

```
斗鱼实时状态事件 ─┐
                  ├─► HTTP 核实（betard 接口）─► 状态机 ─► SQLite ─► SMTP 邮件
30 秒定时轮询 ────┘
```

实时事件只用来尽快触发核实，是否开播、下播完全以 HTTP 结果为准。详细规则见 [设计说明](docs/design.md)。

## 快速开始

需要一台能访问斗鱼的 Linux 主机，安装 Docker Engine 和 Docker Compose v2。

```sh
git clone https://github.com/Eterpy/douyu-live-alert.git
cd douyu-live-alert
cp .env.example .env
chmod 600 .env
sudo install -d -m 750 -o 10001 -g 10001 data   # 容器以 UID/GID 10001 运行
```

编辑 `.env`，填写要监控的房间号（必须是真实房间号，见下一节）：

```sh
DOUYU_ROOMS=123456,654321=某主播
```

启动并检查：

```sh
docker compose up -d --build
docker compose ps
docker compose logs --tail=50 alert
docker compose exec -T alert python -m live_alert check-once   # 立即查询一次各房间状态
docker compose exec -T alert python -m live_alert status       # 查看监控状态和最近邮件
```

默认处于 `dry-run` 模式：只在数据库中记录模拟邮件，不会真正发信。确认判断无误后再启用邮件（见 [启用邮件](#启用邮件)）。

### 房间号与靓号

房间号是直播间网址 `https://www.douyu.com/<房间号>` 中的数字。部分主播使用靓号（短号），靓号无法用于监控：斗鱼接口会返回错误页面，`check-once` 显示状态 `UNKNOWN`（原因通常是 `JSONDecodeError`）。用下面的命令换成真实房间号：

```sh
docker compose run --rm --no-deps alert resolve-room <靓号>
```

### 监控多个房间

`DOUYU_ROOMS` 用逗号分隔，最多 20 个，可以写成 `房间号=显示名称` 指定邮件中的主播名；不写名称时使用斗鱼返回的主播昵称。修改后执行 `docker compose up -d --force-recreate` 生效。新加入的房间与首次启动一样，若正在直播则静默这一场。移除的房间停止监控，历史记录保留在数据库中。

所有房间共用一个进程和数据库。任何一个房间出现无法恢复的错误时，整个进程退出并由 Docker 重启，各房间的状态都能正确恢复。

## 启用邮件

1. 在 `.env` 填写 SMTP 信息：

   ```sh
   SMTP_HOST=smtp.example.com
   SMTP_PORT=587
   SMTP_SECURITY=starttls        # 587 用 starttls，465 用 ssl
   SMTP_USERNAME=你的登录名
   SMTP_PASSWORD=你的密码或授权码
   SMTP_FROM_ADDRESS=alerts@example.com
   SMTP_TO_ADDRESS=you@example.com
   ```

2. 重建容器并发送一封测试邮件（即使处于 dry-run 也会真实发送，主题带 `[TEST]`）：

   ```sh
   docker compose up -d --force-recreate
   docker compose exec -T alert python -m live_alert test-email
   ```

3. 建议先保持 `dry-run` 观察一两个完整的开播到下播周期，用 `status` 核对模拟邮件与实际情况一致。然后把 `NOTIFICATION_MODE` 改为 `send`，再次执行 `docker compose up -d --force-recreate`。

常见邮件服务商（以服务商最新说明为准）：

| 服务商 | SMTP_HOST | 端口 / 加密 | 说明 |
| --- | --- | --- | --- |
| QQ 邮箱 | `smtp.qq.com` | 465 / `ssl` | 在邮箱设置中开启 SMTP，密码填授权码 |
| 网易 163 邮箱 | `smtp.163.com` | 465 / `ssl` | 开启 SMTP 服务，密码填授权码 |
| Gmail | `smtp.gmail.com` | 587 / `starttls` | 需开启两步验证并使用应用专用密码 |
| Brevo | `smtp-relay.brevo.com` | 587 / `starttls` | 用 SMTP Login 和 SMTP Key，发件地址需先验证 |

使用个人邮箱时，发件地址通常必须与登录账号一致。`.env` 请用 UTF-8 编码保存，否则中文名称会让服务拒绝启动。

### 邮件示例

新生成的开播和下播提醒同时包含 HTML 和纯文本两个版本。邮箱以 HTML 显示时，开播提醒使用暖橙色卡片，带“进入直播间”按钮。下播提醒使用夜空紫卡片，突出显示直播时长和下播时间。以纯文本阅读时，效果见下方示例。

`test-email` 发出的测试邮件，以及升级到 2.1.0 前已排队的提醒，都只包含纯文本。

```
主题：🎙️ 某主播 开播啦～

📣 直播间有新动静啦～
直播间：https://www.douyu.com/123456
标题: 今晚一起看比赛
本场确认开播时间：2026-11-18 20:00:15
```

```
主题：🌙 某主播 下播休息啦～

🌙 直播间安静下来啦，记录放在这里～
直播间：https://www.douyu.com/123456
标题: 今晚一起看比赛
确认下播时间：2026-11-18 23:43:03
直播时长：约 3 小时 41 分钟
```

直播时长优先从斗鱼提供的本场可信开播时间算到首次检测到下播。若确认开播时缺少可信的平台时间，就取两封邮件中“本场确认开播时间”和“确认下播时间”的差值。结果以分钟显示。以下场次的开播提醒被静默或受恢复保护：首次启动时已在播的场次，停机或证据中断期间开播、但不符合补报条件的场次，以及从备份恢复后受恢复保护的场次。服务确认这些场次开播的时间晚于实际开播，所以缺少可信平台时间时，直播时长显示为“未知”。停机期间开播、符合补报条件的场次会照常提醒，并且一定带有可信平台时间。直播期间主机时钟回拨、算出的时长为负时，也显示为“未知”。

## 配置参考

所有配置通过环境变量提供，Compose 从 `.env` 读取；完整模板见 [.env.example](.env.example)。未知或越界的配置会让服务拒绝启动。

**基本**

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DOUYU_ROOMS` | （必填） | 监控的房间，`房间号` 或 `房间号=显示名称`，逗号分隔，最多 20 个 |
| `NOTIFICATION_MODE` | `dry-run` | `dry-run` 只记录模拟邮件；`send` 真实发送 |
| `SMTP_HOST` | | SMTP 服务器，发信时必填 |
| `SMTP_PORT` | `587` | 587 须配 `starttls`，465 须配 `ssl` |
| `SMTP_SECURITY` | `starttls` | `starttls` 或 `ssl`，均校验证书 |
| `SMTP_USERNAME` / `SMTP_PASSWORD` | | SMTP 登录凭据，发信时必填 |
| `SMTP_FROM_ADDRESS` | | 发件地址，发信时必填 |
| `SMTP_FROM_NAME` | `斗鱼开播提醒` | 发件人显示名 |
| `SMTP_TO_ADDRESS` | | 收件地址（一个），发信时必填 |

**通知策略**

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `NOTIFY_LIVE` / `NOTIFY_OFFLINE` | `true` | 是否发送开播 / 下播邮件；关闭后仍会正常判定场次 |
| `OFFLINE_REQUIRES_LIVE_EMAIL` | `true` | 只有同场开播邮件已被接受才发下播邮件 |
| `NOTIFY_OFFLINE_FOR_BASELINE_LIVE` | `false` | 为首次启动时已在播的场次发送下播邮件（需同时关闭上一项） |
| `STARTUP_MISSED_EVENT_WINDOW_SECONDS` | `900` | 停机、网络故障等证据中断期间开播的场次，开播时间在此秒数内照常提醒；`0` 表示一律静默（0–86400） |

**高级**（一般无需修改）

| 变量 | 默认值 | 范围 | 说明 |
| --- | --- | --- | --- |
| `DOUYU_TRANSPORT` | `tcp` | `tcp` / `ws` | 实时事件通道；TCP 8601 被拦截时用 `ws`（WSS 8506） |
| `POLL_INTERVAL_SECONDS` | `30` | 30–300 | 定时轮询间隔 |
| `LIVE_CONFIRM_SAMPLES` | `2` | 2–20 | 确认开播所需 HTTP 证据数 |
| `LIVE_CONFIRM_GAP_SECONDS` | `10` | 10–300 | 开播证据最小间隔 |
| `OFFLINE_CONFIRM_SECONDS` | `120` | 120–86400 | 下播确认窗口 |
| `OFFLINE_CONFIRM_SAMPLES` | `3` | 3–10 | 确认下播所需非直播证据数 |
| `OFFLINE_FINAL_EVIDENCE_MAX_AGE_SECONDS` | `30` | ≥1 | 下播最后一份证据的最大时效 |
| `OFFLINE_RESTART_GRACE_SECONDS` | `600` | 0–86400 | 停机超过此时长后发现已下播，不再发送下播邮件 |
| `MAX_NOTIFICATION_AGE_SECONDS` | `900` | ≥1 | 邮件有效期，须大于下播确认窗口 |
| `HTTP_TIMEOUT_SECONDS` | `8` | 1–120 | 单次 HTTP 核实总时限 |
| `BACKOFF_INITIAL_SECONDS` / `BACKOFF_MAX_SECONDS` | `5` / `300` | ≥1 | HTTP 失败退避；上限不得小于起始值 |
| `SMTP_TIMEOUT_SECONDS` | `15` | 1–90 | 单次 SMTP 操作超时 |
| `SMTP_TOTAL_TIMEOUT_SECONDS` | `45` | 1–90 | 一次发送尝试的总时限，不得小于 `SMTP_TIMEOUT_SECONDS` |
| `SMTP_PRECONNECT` | `true` | | 开播候选出现时提前建立 SMTP 连接以降低延迟 |
| `SMTP_MAX_ATTEMPTS` | `4` | 1–20 | 单封邮件最大尝试次数 |
| `OBSERVATION_RETENTION_DAYS` | `30` | ≥1 | HTTP 观察记录保留天数 |
| `LOG_LEVEL` | `INFO` | | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `DATA_DIR` | `./data` | | 数据目录；Compose 中固定为 `/app/data` |
| `APP_ENV` | `production` | | 写入启动日志的环境标签，不影响行为 |

邮件中的时间固定为北京时间（`Asia/Shanghai`），与主机或容器的 `TZ` 设置无关。

## 命令

容器内用 `docker compose exec -T alert python -m live_alert <命令>`，本地开发用 `python -m live_alert <命令>`。

| 命令 | 说明 |
| --- | --- |
| `run` | 启动监控服务（容器默认命令） |
| `status [--room ID]` | 以 JSON 输出各房间的监控状态、最近一封开播和下播邮件及延迟数据 |
| `healthcheck [--room ID]` | 检查服务心跳，任一房间心跳超过 45 秒未更新、已停止或尚未启动时返回非零；上游 `DEGRADED` 不算失败（Docker 健康检查使用） |
| `check-once [--room ID]` | 立即查询一次房间状态并输出原始字段，不影响监控；结果未知时返回非零 |
| `resolve-room <房间号>` | 输出从斗鱼页面查到的真实房间号。页面缺少有效房间号时，命令报错并返回非零退出码。 |
| `test-email` | 发送一封 `[TEST]` 测试邮件 |
| `backup <文件>` | 在线生成数据库一致性快照 |
| `restore <文件>` | 从快照恢复到空的数据目录 |

## 运维

**查看状态**　`status` 中 `monitor.confirmed` 为 `BASELINING`（首次采样中）、`OFFLINE` 或 `LIVE_CONFIRMED`；`health` 为 `STARTING`（刚启动，尚未完成第一次心跳）、`OK`、`DEGRADED`（超过 10 分钟没有有效 HTTP 结果或实时连接持续断开，服务会继续重试）或 `STOPPED`。邮件状态含义见 [设计说明](docs/design.md#邮件状态)。

**没收到邮件时**

- 确认 `NOTIFICATION_MODE=send`，并用 `test-email` 验证 SMTP。
- 查看 `status` 中邮件的 `state` 和 `reason`：`SUPPRESSED_BASELINE` 表示首次启动或停机期间已在播的场次；`SUPPRESSED_NO_LIVE_NOTICE` 表示同场开播邮件没发成功；`EXPIRED` 表示证据过旧或超过有效期；`SUPPRESSED_RECIPIENT_CHANGED` 表示更换收件人前排队的邮件，不会再发给旧地址；`UNCERTAIN` 表示正文可能已提交但没收到回执，请到邮件服务商后台核对。
- `check-once` 持续显示 `UNKNOWN` 时，先用 `resolve-room` 确认房间号不是靓号；其余原因（如 `http_429`、`URLError`、`TimeoutError`）多为网络或限流问题。

**日志**　状态变化、HTTP 耗时、邮件结果和原因以单行 JSON 记录，带房间号与场次号；实时事件连接的建立、断开与重连由 aiodouyu 以普通文本记录。Compose 已配置日志轮转（3 × 5 MiB）。

**健康与自动恢复**　容器设置了 `restart: unless-stopped`。采样停滞、事件循环卡死或任一房间出错时，进程会自行退出并由 Docker 重启。

**单实例**　同一数据目录只能运行一个实例；看到“数据目录已有监控实例运行”时，检查是否有另一个容器挂载了同一个目录。

### 备份与恢复

```sh
docker compose exec -T alert python -m live_alert backup /app/data/snapshot-20261118.sqlite
install -d -m 700 backups
sudo install -m 600 data/snapshot-20261118.sqlite backups/
```

快照包含收件地址和通知历史，请妥善保存；`.env` 另行备份。

恢复时先停止服务并保留原数据目录：

```sh
docker compose down
mv -T data data.before-restore
sudo install -d -m 750 -o 10001 -g 10001 data
sudo install -m 600 -o 10001 -g 10001 backups/snapshot-20261118.sqlite data/restore-source.sqlite
docker compose run --rm --no-deps alert restore /app/data/restore-source.sqlite
docker compose up -d
```

恢复后，快照中尚未完成的邮件记为 `UNCERTAIN`，尚未结束的场次会跳过下播提醒。为避免重复发信，恢复后只有平台开播时间明确晚于恢复时刻 120 秒以上的场次才有资格发送开播提醒，因为平台时钟可能比本机快 120 秒。房间连续约 2 分钟处于非直播状态后，这项保护解除，新的直播照常提醒。

### 升级与卸载

升级前先备份，然后：

```sh
git pull
docker compose up -d --build
```

数据库结构会自动迁移。版本变更和升级注意事项见 [CHANGELOG](CHANGELOG.md)。卸载执行 `docker compose down`，按需删除 `data/` 和 `.env`。

## 本地开发

需要 Python 3.12 或更高版本。

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
python -m pytest -q
ruff check src tests
ruff format --check src tests
mypy src
```

测试完全离线运行：使用固定的接口响应、虚拟时钟、临时数据库和假 SMTP，并禁止真实网络连接。本地运行服务（需先按上文安装，HTTP 与 SMTP 子进程从已安装的包加载代码）：`DOUYU_ROOMS=123456 python -m live_alert run`（数据默认写入 `./data`）。

代码结构：

| 文件 | 内容 |
| --- | --- |
| `src/live_alert/service.py` | 多房间调度、采样循环、心跳与看门狗 |
| `src/live_alert/douyu.py` | 斗鱼接口适配与字段校验 |
| `src/live_alert/http_worker.py` | 有大小和时间上限的 HTTP 子进程（房间状态、靓号解析） |
| `src/live_alert/state.py` | 开播/下播状态机与场次 |
| `src/live_alert/mail.py`、`smtp_worker.py` | 邮件队列与有时限的 SMTP 子进程 |
| `src/live_alert/storage.py` | SQLite 结构与迁移 |
| `src/live_alert/config.py` | 环境变量解析与校验 |
| `src/live_alert/cli.py` | 命令行 |

欢迎提交 Issue 和 Pull Request。修改判定逻辑时请补充对应的离线测试。

## 致谢与许可

- 斗鱼协议与接口解析基于 [aiodouyu](https://pypi.org/project/aiodouyu/)。
- 本项目以 [MIT 许可证](LICENSE) 发布。安全问题请参阅 [SECURITY.md](SECURITY.md)。
