# burst-guard

`burst-guard` 是一个独立运行的 Telegram userbot。每个群组按非机器人消息顺序切分为固定的 `X` 条消息一组；一组内指定用户的视频候选消息达到 `Y` 条时，服务等概率随机保留 1 条并删除其余候选消息。它不下载媒体、不读取链接页面，也不扫描历史消息。

首个版本只支持单实例运行，并且必须使用专门注册的 Telegram 用户账号。不要使用个人日常账号；userbot 的使用还必须符合 Telegram 平台规则和适用法律。

## 行为

- 指定用户发送的 `video`、`video_note`、MIME 为 `video/*` 的文档，以及包含配置视频域名 URL 的消息都是候选并计入 `Y`。
- 目标用户普通文本、其他真人、另一目标用户和匿名管理员消息都会占用一个分组位置，但不会提前结束分组。
- 机器人和 Telegram 服务消息不占分组位置。机器人上传的视频如果携带被淘汰候选的原链接，会与源消息一同删除或在延迟到达时立即删除。
- 仅在一组累计到 `X` 条非机器人消息时结算。候选少于 `Y` 条则全部保留；达到 `Y` 条则从该组全部候选中等概率保留 1 条。
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
- `BURST_GUARD_GROUP_SIZE` 是每个固定分组包含的非机器人消息数 `X`。
- `BURST_GUARD_THRESHOLD` 是触发随机保留的视频候选消息数 `Y`。
- `X`、`Y` 都必须为正整数，且 `X >= Y`。
- `BURST_GUARD_VIDEO_DOMAINS` 仅填写域名，不填写 scheme 或路径；子域名自动匹配，短链接域名必须显式列出。
- `BURST_GUARD_DELETE_BATCH_SIZE` 范围为 1 到 100。
- `TELEGRAM_SESSION_PATH` 在容器部署中应位于 `/app/data`。

日志为单行 JSON，只记录 ID、计数、状态和错误类型，不记录消息正文、完整 URL、API Hash 或会话内容。

解析机器人关联不依赖回复关系：服务会规范化视频说明文字或 URL entity 中的原链接，并在内存中保留到 `BURST_GUARD_IDEMPOTENCY_TTL_SECONDS`。只有实际含视频媒体、链接域名已配置且原链接与被淘汰候选精确匹配的机器人消息才会删除；普通机器人消息仍被忽略。同一原链接同时属于保留项和淘汰项时不会按链接删除，以避免误伤保留视频。

## 验收

先保持 `BURST_GUARD_DRY_RUN=true`，运行真实群验收监听：

```bash
uv run python scripts/verify_live.py --duration 180 --minimum-candidates 3 --minimum-planned 2
```

在测试群依次验证未满 `X` 条时不结算、第 `X` 条触发结算、普通真人消息占位、机器人消息不占位及非白名单群。脚本只汇总进程内计数，不输出消息内容。试运行通过后才将 `BURST_GUARD_DRY_RUN=false`，并用 `--allow-real-delete` 明确允许验收脚本执行真实删除。

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
