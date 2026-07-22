# burst-guard

`burst-guard` 是一个独立运行的 Telegram userbot。每个群组、每个指定用户分别维护最近 `X` 条该用户消息；窗口内的视频候选消息达到 `Y` 条时，服务等概率随机保留 1 条并删除其余候选消息。它不下载媒体、不读取链接页面，也不扫描历史消息。

首个版本只支持单实例运行，并且必须使用专门注册的 Telegram 用户账号。不要使用个人日常账号；userbot 的使用还必须符合 Telegram 平台规则和适用法律。

## 行为

- 指定用户发送的 `video`、`video_note`、MIME 为 `video/*` 的文档，以及包含配置视频域名 URL 的消息都是候选并计入 `Y`；Telegram GIF 动画、动态贴纸和视频贴纸不按视频媒体计入。
- 目标用户普通文本占用该用户窗口位置；其他用户、匿名管理员和机器人消息不进入该目标用户窗口。
- 窗口始终只保留该目标用户最新的 `X` 条消息。候选达到 `Y` 条时立即随机保留 1 条，后续消息继续滚动窗口。
- 阈值清理至少成功删除一条消息后，服务在群内发送“检测到 @用户名 正在发送垃圾视频，已制裁，今日共制裁 x 次”；次数按群组和目标用户分别统计，并在 UTC 日期变化时归零。
- 机器人和 Telegram 服务消息不会触发窗口更新。解析视频优先按回复消息、`Source` 原链接关联；来自 `BURST_GUARD_PARSER_SENDER_IDS`、没有可识别原链接且未回复源消息的视频，则按发送顺序关联目标用户尚未匹配的链接消息。
- 不同群组的状态完全隔离，进程重启后不恢复状态。

## 准备账号

1. 在 <https://my.telegram.org> 创建应用并取得 API ID 和 API Hash。
2. 准备一个启用两步验证的专用 Telegram 用户账号。
3. 将专用账号加入每个受管群，并授予管理员和删除消息权限。
4. 复制配置模板：`cp .env.example .env`，填写凭据、专用账号数字 ID、群组 ID 和目标用户。
5. 确保数据目录仅对服务账户开放。容器内进程 UID/GID 为 `10001:10001`。

API Hash、`.env` 和会话文件都不得提交到仓库或写入日志。会话文件等同于账号登录权限。

## 本地运行

安装依赖并创建会话：

```bash
uv sync --frozen
uv run python -m burst_guard.login
chmod 600 /path/to/burst-guard.session
uv run python -m burst_guard
```

常驻服务不会交互式询问手机号、验证码或两步验证密码。会话缺失、账号不匹配或任一群权限不足时，启动检查会立即失败。

配置默认 `BURST_GUARD_ENABLED=false` 且 `BURST_GUARD_DRY_RUN=true`。首次部署应先启用功能但保持试运行：

```dotenv
BURST_GUARD_ENABLED=true
BURST_GUARD_DRY_RUN=true
```

## Docker Compose

先创建只允许服务 UID 访问的数据目录，再执行独立登录命令：

```bash
mkdir -p data
sudo chown 10001:10001 data
chmod 700 data
docker compose --profile login run --rm login
docker compose up -d burst-guard
```

不要同时运行 `login` 与常驻服务。容器以非 root 用户、只读根文件系统、无 Linux capabilities 运行；只有 `/app/data` 和临时目录可写。

健康检查默认只绑定宿主机 `127.0.0.1`：

```text
GET /health/live
GET /health/ready
```

`ready` 仅在配置有效、启动检查通过、MTProto 已连接且更新监听运行时返回 HTTP 200。

## 配置

完整键名和安全默认值见 [`.env.example`](.env.example)。主要规则如下：

- `BURST_GUARD_CHAT_IDS` 是至少一个逗号分隔的群组 ID。
- `BURST_GUARD_TARGETS` 接受正整数用户 ID 或用户名。生产环境优先使用稳定的数字 ID。
- `BURST_GUARD_WINDOW_SIZE` 是每个目标用户的滚动消息窗口大小 `X`；旧配置名 `BURST_GUARD_GROUP_SIZE` 仍兼容。
- `BURST_GUARD_THRESHOLD` 是触发随机保留的视频候选消息数 `Y`。
- `X`、`Y` 都必须为正整数，且 `X >= Y`。
- `BURST_GUARD_VIDEO_DOMAINS` 仅填写域名，不填写 scheme 或路径；子域名自动匹配，短链接域名必须显式列出。
- `BURST_GUARD_PARSER_SENDER_IDS` 是解析服务发送者的数字 ID，多个 ID 用逗号分隔；示例配置为 `7947627028`。
- `BURST_GUARD_DELETE_BATCH_SIZE` 范围为 1 到 100。
- `TELEGRAM_SESSION_PATH` 在容器部署中应位于 `/app/data`。

日志为单行 JSON，只记录 ID、计数、状态和错误类型，不记录消息正文、完整 URL、API Hash 或会话内容。

解析输出不依赖 Telegram `bot` 标志。关联优先级依次为回复消息 ID、视频说明文字或 `Source` 隐藏链接等位置提取出的原链接、配置解析发送者的下一条视频 FIFO。链接源消息即使被发送者立即删除，已经登记的内存关联仍保留到 `BURST_GUARD_IDEMPOTENCY_TTL_SECONDS`。只有实际含视频媒体且能关联到候选的解析消息才可能删除；普通消息仍被忽略。同一原链接同时属于保留项和淘汰项时不会按链接删除，以避免误伤保留视频。解析输出的后续补删不会重复发送制裁通知或增加次数；进程重启后当日计数从零开始。

## 验收

先保持 `BURST_GUARD_DRY_RUN=true`，运行真实群验收监听：

```bash
uv run python scripts/verify_live.py --duration 180 --minimum-candidates 3 --minimum-planned 2
```

在测试群依次验证目标用户最新 `X` 条消息窗口、达到 `Y` 条立即结算、目标用户普通文本占位、其他用户和机器人不占位及非白名单群。脚本只汇总进程内计数，不输出消息内容。试运行通过后才将 `BURST_GUARD_DRY_RUN=false`，并用 `--allow-real-delete` 明确允许验收脚本执行真实删除。

## 开发检查

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src
uv run pytest
uv run pip-audit --skip-editable
docker compose config --quiet
docker build -t burst-guard:test .
```

核心业务模型位于 `src/burst_guard`：分类器是纯函数；状态服务按群组加锁并在锁内完成抽样状态推进；处理器取得清理请求后才在锁外调用 Telegram API。删除按批处理，`FloodWaitError` 按服务端秒数有限重试，单批失败不会中止后续批次。
