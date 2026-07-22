import asyncio
import os
from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from itertools import batched
from typing import Any, BinaryIO, Literal, cast

from easy_ai18n import PreLocaleSelector
from parsehub.types import (
    AniFile,
    AniRef,
    AnyMediaRef,
    AnyParseResult,
    ImageFile,
    LivePhotoFile,
    PostType,
    VideoFile,
)
from pyrogram import Client, enums, filters
from pyrogram.errors import FloodWait, Forbidden, SlowmodeWait, WebpageCurlFailed, WebpageMediaEmpty
from pyrogram.types import (
    InlineKeyboardButton as Ikb,
)
from pyrogram.types import (
    InlineKeyboardMarkup as Ikm,
)
from pyrogram.types import (
    InputMediaAnimation,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    LinkPreviewOptions,
    Message,
)

from core import bs
from db import get_session
from i18n import t_
from log import logger
from plugins.burst_guard import burst_guard, delete_burst_messages
from plugins.filters import forwarded_from_bot_filter, platform_filter, via_me_filter
from plugins.helpers import (
    ProcessedMedia,
    build_caption,
    build_caption_by_str,
    create_richtext_telegraph,
    resolve_media_info,
)
from repo.user_settings import UserConfig
from services import AccountService, ParseService
from services.burst_guard import OutputTracker
from services.cache import CacheEntry, CacheMedia, CacheMediaType, CacheParseResult, parse_cache, persistent_cache
from services.pipeline import ParsePipeline, PipelineResult, StatusReporter
from utils.helpers import pack_dir_to_tar_gz, to_list, with_request_id
from utils.rate_limit import ParseRateLimitExceeded, parse_rate_limit

logger = logger.bind(name="Parse")
SKIP_DOWNLOAD_THRESHOLD = 0
GIF_ONLY_SKIP_DOWNLOAD_COUNT_THRESHOLD = 5
MAX_RETRIES = 5

_output_tracker: ContextVar[OutputTracker | None] = ContextVar("parse_output_tracker", default=None)
_GROUP_CHAT_TYPES = frozenset({enums.ChatType.GROUP, enums.ChatType.SUPERGROUP})


async def _create_output_tracker(client: Client, message: Message) -> OutputTracker | None:
    chat = message.chat
    if not bs.burst_guard_enabled or chat is None or chat.type not in _GROUP_CHAT_TYPES:
        return None
    chat_id = chat.id
    source_message_id = message.id
    if chat_id is None or source_message_id is None:
        return None

    async def delete_messages(tracked_chat_id: int, message_ids: tuple[int, ...]) -> None:
        await delete_burst_messages(client, tracked_chat_id, message_ids)

    return await burst_guard.candidate_output_tracker(chat_id, source_message_id, delete_messages)


def _sent_message_ids(sent: Any) -> tuple[int, ...]:
    if sent is None:
        return ()
    if isinstance(sent, Sequence) and not isinstance(sent, (str, bytes)):
        possible_ids = [getattr(message, "id", None) for message in sent]
    else:
        possible_ids = [getattr(sent, "id", None)]
    return tuple(message_id for message_id in possible_ids if isinstance(message_id, int))


async def _track_output[T](sent: T) -> T:
    tracker = _output_tracker.get()
    if tracker is not None and (message_ids := _sent_message_ids(sent)):
        await tracker.track(message_ids)
    return sent


async def _send_tracked[T](send_coro_fn: Callable[[], Awaitable[T]]) -> T:
    return await _track_output(await _send_with_rate_limit(send_coro_fn))


def _media_input(media: str | BinaryIO | None) -> str | BinaryIO:
    return cast(str | BinaryIO, media)


async def _send_with_rate_limit[T](
    send_coro_fn: Callable[[], Awaitable[T]],
) -> T:
    """带自动重试的发送包装器。

    Args:
        send_coro_fn: 返回协程的可调用对象（lambda 或函数），每次重试会重新调用
    """
    for attempt in range(MAX_RETRIES):
        try:
            return await send_coro_fn()
        except (FloodWait, SlowmodeWait) as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"{e.ID} 重试 ({attempt + 1}/{MAX_RETRIES})，等待 {e.value}s")
                await asyncio.sleep(e.value)
            else:
                raise
        except Forbidden as e:
            logger.warning(f"消息发送失败, Bot 无权限: {e}")
    raise RuntimeError("发送重试失败")


class MessageStatusReporter(StatusReporter):
    """基于 Telegram Message 的状态报告器"""

    def __init__(self, user_msg: Message, *, _t: PreLocaleSelector, user_config: UserConfig):
        self._user_msg = user_msg
        self._msg: Message | None = None
        self._t = _t
        self._user_config = user_config

    async def report(self, text: str) -> None:
        if self._user_config.noprogress:
            return
        await self._edit_text(f"**▎{text}**")

    async def report_error(self, stage: str, error: Exception) -> None:
        text = self._t(f"**▎{stage}错误:** \n```\n{error}```")
        if bs.demo_mode:
            text += self._t("\n\n**问题反馈: @MisakaSisters**")
        await self._edit_text(
            text,
            link_preview_options=LinkPreviewOptions(is_disabled=True),
        )
        if self._user_config.keep_error_log:
            return

        async def fn() -> None:
            await asyncio.sleep(15)
            if self._msg:
                await self._msg.delete()

        loop = asyncio.get_running_loop()
        loop.create_task(fn())

    async def dismiss(self) -> None:
        if self._msg:
            await self._msg.delete()

    async def _edit_text(self, text: str, **kwargs: Any) -> None:
        try:
            if self._msg is None:
                self._msg = await _track_output(await self._user_msg.reply_text(text, **kwargs))
            else:
                if self._msg.text != text:
                    await self._msg.edit_text(text, **kwargs)
        except (FloodWait, SlowmodeWait):
            pass
        except Forbidden as e:
            logger.warning(f"消息发送失败, Bot 无权限: {e}")


# ── Handler ──────────────────────────────────────────────────────────


@Client.on_message(
    filters.command(["jx", "jxjx", "raw", "zip"])
    | ((filters.text | filters.caption) & ~via_me_filter & platform_filter(True) & ~forwarded_from_bot_filter)
)
async def jx(cli: Client, msg: Message) -> None:
    tracker = await _create_output_tracker(cli, msg)
    context_token = _output_tracker.set(tracker)
    try:
        await _process_message(cli, msg)
    except asyncio.CancelledError:
        current_task = asyncio.current_task()
        if current_task is not None and current_task.cancelling():
            raise
        logger.info(
            "候选解析任务已取消: chat_id={}, message_id={}",
            msg.chat.id if msg.chat else None,
            msg.id,
        )
    finally:
        if tracker is not None and msg.chat is not None and msg.chat.id is not None and msg.id is not None:
            await burst_guard.finish_processing(msg.chat.id, msg.id)
        _output_tracker.reset(context_token)


async def _process_message(cli: Client, msg: Message) -> None:
    mode = "preview"
    bypass_cache = False
    lang = None
    user_config = UserConfig()

    if msg.from_user:
        async with get_session() as session:
            current = await AccountService(session, msg.from_user.id).ensure_account()
            user_config = current.config
            lang = current.lang
            mode = user_config.default_mode

    _t = t_[lang]

    if msg.command:
        match msg.command[0]:
            case "raw":
                mode = "raw"
            case "jx":
                mode = "preview"
            case "jxjx":
                mode = "preview"
                bypass_cache = True
            case "zip":
                mode = "zip"

        text = " ".join(msg.command[1:]) if msg.command[1:] else ""
        if not text and msg.reply_to_message:
            text = msg.reply_to_message.text or msg.reply_to_message.caption or ""
        if not text:
            await _track_output(await msg.reply_text(_t("**▎请加上链接或回复一条消息**")))
            return
    else:
        text = msg.text or msg.caption or ""

    tokens = text.strip().split()
    urls = list({i for i in tokens if ParseService().parser.get_platform(i)})[:10]

    if not urls:
        await _track_output(await msg.reply_text(_t("**▎不支持的平台**")))
        return

    tasks: list[asyncio.Future[None]] = [
        asyncio.ensure_future(
            _handle_parse_request(
                cli,
                msg,
                url=url,
                mode=mode,
                delete_share_url_msg=user_config.auto_delete_url,
                bypass_cache=bypass_cache,
                _t=_t,
                user_config=user_config,
            )
        )
        for url in urls
    ]
    tracker = _output_tracker.get()
    if tracker is not None and msg.chat is not None and msg.chat.id is not None and msg.id is not None:
        for task in tasks:
            await burst_guard.attach_task(msg.chat.id, msg.id, task)
    try:
        await asyncio.gather(*tasks)
    finally:
        if tracker is not None and msg.chat is not None and msg.chat.id is not None and msg.id is not None:
            for task in tasks:
                await burst_guard.detach_task(msg.chat.id, msg.id, task)


# ── 主流程 ───────────────────────────────────────────────────────────


def _get_parse_user_id(_: Client, msg: Message, **__: Any) -> int | None:
    return msg.chat.id if msg.chat else None


@with_request_id
async def _handle_parse_request(
    cli: Client,
    msg: Message,
    *,
    url: str,
    mode: Literal["raw", "preview", "zip"] | str = "preview",
    delete_share_url_msg: bool = False,
    bypass_cache: bool = False,
    _t: PreLocaleSelector,
    user_config: UserConfig,
) -> None:
    try:
        await handle_parse(
            cli,
            msg,
            url=url,
            mode=mode,
            delete_share_url_msg=delete_share_url_msg,
            bypass_cache=bypass_cache,
            _t=_t,
            user_config=user_config,
        )
    except ParseRateLimitExceeded as e:
        if e.should_notify:
            logger.warning(
                f"速率限制 {e.retry_after:.1f}s, chat_id={msg.chat.id if msg.chat else None}, msg_id={msg.id}"
            )
            text = _t(f"**▎解析过于频繁, 请在 {e.retry_after:.1f}s 后重试**")
            if bs.demo_mode:
                text += _t(
                    "\n\n>**为保障所有用户的使用体验, 当前已启用速率限制**\n\n"
                    ">本项目为开源项目, 如有高频或批量解析需求, 建议自行部署实例, "
                    "以免触发 Telegram API 全局速率限制\n\n"
                    "**开源地址: [GitHub](https://github.com/z-mio/parse_hub_bot)**"
                )
            msg = await _track_output(
                await msg.reply_text(text, link_preview_options=LinkPreviewOptions(is_disabled=True))
            )

            async def fn(retry_after: float) -> None:
                await asyncio.sleep(retry_after)
                await msg.delete()

            loop = asyncio.get_running_loop()
            loop.create_task(fn(e.retry_after))


@parse_rate_limit(_get_parse_user_id)
async def handle_parse(
    cli: Client,
    msg: Message,
    *,
    url: str,
    mode: Literal["raw", "preview", "zip"] | str = "preview",
    delete_share_url_msg: bool = False,
    bypass_cache: bool = False,
    _t: PreLocaleSelector,
    user_config: UserConfig,
) -> None:
    chat_id = msg.chat.id if msg.chat else None
    logger.info(f"收到解析请求: url={url}, chat_id={chat_id}, msg_id={msg.id}, mode={mode}")
    if bypass_cache:
        logger.debug("bypass_cache=True 绕过缓存")
    if delete_share_url_msg:
        logger.debug(f"自动删除分享链接消息: chat_id={chat_id}, msg_id: {msg.id}")
        try:
            await msg.delete()
        except Exception as e:
            logger.warning(f"删除分享链接消息失败: chat_id={chat_id}, msg_id: {msg.id}, error: {e}")

    reporter = MessageStatusReporter(msg, _t=_t, user_config=user_config)
    match mode:
        case "raw":
            use_caching = False
            skip_media_processing = True
            singleflight = False
            save_metadata = False
        case "zip":
            use_caching = False
            skip_media_processing = True
            singleflight = False
            save_metadata = True
        case _:
            use_caching = True
            skip_media_processing = False
            singleflight = not bypass_cache
            save_metadata = False
    try:
        raw_url = await ParseService().get_raw_url(url)
    except Exception as e:
        await reporter.report_error(_t("获取原始链接"), e)
        return

    if use_caching and not bypass_cache and (cached := await persistent_cache.get(raw_url)):
        logger.debug("file_id 缓存命中, 直接发送")
        await _send_cached(msg, cached, raw_url, user_config=user_config)
        return

    cached_parse_result = None if bypass_cache else await parse_cache.get(raw_url)
    with ParsePipeline(
        url,
        raw_url,
        reporter,
        parse_result=cached_parse_result,
        singleflight=singleflight,
        skip_media_processing=skip_media_processing,
        skip_download_threshold=SKIP_DOWNLOAD_THRESHOLD,
        gif_only_skip_download_count_threshold=GIF_ONLY_SKIP_DOWNLOAD_COUNT_THRESHOLD if mode == "preview" else 0,
        save_metadata=save_metadata,
        _t=_t,
    ) as pipeline:
        if (result := await pipeline.run()) is None:
            if pipeline.waited:
                logger.debug("Singleflight 等待完成, 重新检查缓存")
                if not bypass_cache and (cached := await persistent_cache.get(raw_url)):
                    await _send_cached(msg, cached, raw_url, user_config=user_config)
                else:
                    await handle_parse(
                        cli,
                        msg,
                        url=url,
                        mode=mode,
                        bypass_cache=bypass_cache,
                        _t=_t,
                        user_config=user_config,
                    )
                    return
            else:
                logger.debug("Pipeline 返回 None, 跳过后续处理")
            return

        parse_result = result.parse_result
        await parse_cache.set(raw_url, parse_result)

        # ── 富文本 → Telegraph ──
        if parse_result.type == PostType.RICHTEXT:
            logger.debug(f"富文本类型, 创建 Telegraph 页面: title={parse_result.title}")
            await msg.reply_chat_action(enums.ChatAction.TYPING)
            ph_url = await create_richtext_telegraph(cli, parse_result)
            logger.debug(f"Telegraph 页面创建完成: {ph_url}")
            caption = build_caption(parse_result, ph_url, hide_source=user_config.hide_source)
            await _send_tracked(
                lambda: msg.reply_text(
                    caption,
                    link_preview_options=LinkPreviewOptions(show_above_text=True),
                )
            )
            await persistent_cache.set(
                raw_url,
                CacheEntry(
                    parse_result=CacheParseResult(title=parse_result.title, content=parse_result.content),
                    telegraph_url=ph_url,
                ),
            )
            await reporter.dismiss()
            return

        caption = build_caption(parse_result, hide_source=user_config.hide_source)
        gif_only = all(isinstance(i, AniRef) for i in to_list(parse_result.media))
        if mode == "preview" and gif_only and len(to_list(parse_result.media)) > GIF_ONLY_SKIP_DOWNLOAD_COUNT_THRESHOLD:
            await _send_tracked(
                lambda: msg.reply_text(
                    caption,
                    reply_markup=_build_gif_button(to_list(parse_result.media)),
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                )
            )
            await reporter.dismiss()
            return

        if not result.processed_list:
            logger.debug("无媒体文件, 仅发送文本")
            await msg.reply_chat_action(enums.ChatAction.TYPING)
            await _send_tracked(
                lambda: msg.reply_text(
                    caption,
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                )
            )
            cache_entry = CacheEntry(
                parse_result=CacheParseResult(title=parse_result.title, content=parse_result.content)
            )
            await persistent_cache.set(raw_url, cache_entry)
            await reporter.dismiss()
            return

        if mode == "raw":
            await _send_raw(msg, result, reporter, _t=_t, user_config=user_config)
            return
        if mode == "zip":
            await _send_zip(msg, result, reporter, _t=_t, user_config=user_config)
            return

        # ── 上传媒体 ──
        logger.debug(f"开始上传媒体: media_count={len(result.processed_list)}")
        await reporter.report(_t("上 传 中..."))
        try:
            media_cache_entry = await _send_media(msg, parse_result, result.processed_list, caption, _t=_t)
            if media_cache_entry:
                await persistent_cache.set(raw_url, media_cache_entry)
            await reporter.dismiss()
        except Exception as e:
            logger.opt(exception=e).debug("详细堆栈")
            logger.error(f"上传失败: {e}")
            await reporter.report_error(_t("上传"), e)
            return


# ── 构建 InputMedia ──────────────────────────────────────────────────


def _build_input_media(
    media_refs: Sequence[AnyMediaRef],
    processed_list: list[ProcessedMedia],
) -> tuple[list[InputMediaPhoto | InputMediaVideo], list[InputMediaAnimation]]:
    """根据处理结果和媒体引用构建 Telegram InputMedia 列表。

    Returns:
        (photos_videos, animations) 两类媒体列表
    """
    photos_videos: list[InputMediaPhoto | InputMediaVideo] = []
    animations: list[InputMediaAnimation] = []

    for media_ref, processed in zip(media_refs, processed_list, strict=False):
        file_paths = processed.output_paths or [processed.source.path]
        for file_path in file_paths:
            file_path_str = str(file_path)
            width, height, duration = resolve_media_info(processed, file_path_str)

            match processed.source:
                case ImageFile():
                    photos_videos.append(InputMediaPhoto(media=file_path_str))
                case AniFile():
                    animations.append(InputMediaAnimation(media=file_path_str))
                case VideoFile():
                    photos_videos.append(
                        InputMediaVideo(
                            media=file_path_str,
                            video_cover=media_ref.thumb_url,
                            duration=duration,
                            width=width,
                            height=height,
                            supports_streaming=True,
                        )
                    )
                case LivePhotoFile():
                    photos_videos.append(
                        InputMediaVideo(
                            media=processed.source.video_path,
                            video_cover=file_path_str,
                            duration=duration,
                            width=width,
                            height=height,
                            supports_streaming=True,
                        )
                    )

    return photos_videos, animations


# ── 缓存条目构建 ─────────────────────────────────────────────────────


def _cache_media_from_message(m: Message) -> CacheMedia | None:
    """从已发送的 Telegram Message 提取 CacheMedia。"""
    if m.photo:
        return CacheMedia(type=CacheMediaType.PHOTO, file_id=m.photo.file_id)
    if m.video:
        return CacheMedia(
            type=CacheMediaType.VIDEO,
            file_id=m.video.file_id,
            cover_file_id=m.video.video_cover.file_id if m.video.video_cover else None,
        )
    if m.animation:
        return CacheMedia(type=CacheMediaType.ANIMATION, file_id=m.animation.file_id)
    if m.document:
        return CacheMedia(type=CacheMediaType.DOCUMENT, file_id=m.document.file_id)
    return None


def _make_cache_entry(parse_result: AnyParseResult, media_list: list[CacheMedia]) -> CacheEntry:
    return CacheEntry(
        parse_result=CacheParseResult(title=parse_result.title, content=parse_result.content),
        media=media_list,
    )


# ── Raw 模式上传 ──────────────────────────────────────────────────────


async def _send_raw(
    msg: Message,
    result: PipelineResult,
    reporter: MessageStatusReporter,
    *,
    _t: PreLocaleSelector,
    user_config: UserConfig,
) -> None:
    """Raw 模式：将文件以原始文档形式上传。"""
    logger.debug("Raw 模式, 直接上传文件")
    await reporter.report(_t("上 传 中..."))
    try:
        caption = build_caption(result.parse_result, hide_source=user_config.hide_source)
        docs: list[InputMediaDocument] = []
        gifs = []
        livephoto_videos: dict[int, InputMediaDocument] = {}

        for processed in result.processed_list:
            file_paths = processed.output_paths or [processed.source.path]
            file_path = file_paths[0]
            doc = InputMediaDocument(media=str(file_path))
            if isinstance(processed.source, AniFile):
                gifs.append(doc)
            elif isinstance(processed.source, LivePhotoFile):
                docs.append(doc)
                livephoto_videos[len(docs) - 1] = InputMediaDocument(media=str(processed.source.video_path))
            else:
                docs.append(doc)

        if len(docs + gifs) == 1:
            all_docs = docs + gifs
            await msg.reply_chat_action(enums.ChatAction.UPLOAD_DOCUMENT)
            sent_msg = await _send_tracked(
                lambda: msg.reply_document(_media_input(all_docs[0].media), caption=caption, force_document=True)
            )
            if livephoto_videos and sent_msg:
                await _send_tracked(
                    lambda: sent_msg.reply_document(_media_input(livephoto_videos[0].media), force_document=True)
                )
        else:
            msgs: list[Message] = []
            for batch in batched(docs, 10):
                await msg.reply_chat_action(enums.ChatAction.UPLOAD_DOCUMENT)
                # noinspection PyDefaultArgument
                mg = await _send_tracked(lambda b=list(batch): msg.reply_media_group(b))  # type: ignore
                msgs.extend(mg)
            if livephoto_videos:
                for idx, media_doc in livephoto_videos.items():
                    await msg.reply_chat_action(enums.ChatAction.UPLOAD_DOCUMENT)
                    await _send_tracked(
                        lambda m_=media_doc, idx_=idx: msgs[idx_].reply_document(  # type: ignore
                            _media_input(m_.media), force_document=True
                        )
                    )
            if gifs:
                await _send_tracked(
                    lambda: msg.reply_text(
                        _t("**▎GIF 下载链接**"), reply_markup=_build_gif_button(to_list(result.parse_result.media))
                    )
                )
            await _send_tracked(
                lambda: msg.reply_text(
                    caption,
                    link_preview_options=LinkPreviewOptions(is_disabled=True),
                )
            )

    except Exception as e:
        logger.opt(exception=e).debug("详细堆栈")
        logger.error(f"Raw 模式上传失败: {e}")
        await reporter.report_error(_t("上传"), e)
        return
    finally:
        result.cleanup()

    await reporter.dismiss()


async def _send_zip(
    msg: Message,
    result: PipelineResult,
    reporter: MessageStatusReporter,
    *,
    _t: PreLocaleSelector,
    user_config: UserConfig,
) -> None:
    logger.debug("Zip 模式, 开始打包")
    await reporter.report(_t("打 包 中..."))
    try:
        caption = build_caption(result.parse_result, hide_source=user_config.hide_source)
        if result.output_dir is None:
            raise ValueError("缺少打包目录")
        pack_path = await asyncio.to_thread(pack_dir_to_tar_gz, result.output_dir)
    except Exception as e:
        logger.opt(exception=e).debug("详细堆栈")
        logger.error(f"打包失败: {e}")
        await reporter.report_error(_t("打包"), Exception("..."))
        return
    finally:
        result.cleanup()

    await reporter.report(_t("上 传 中..."))
    try:
        await msg.reply_chat_action(enums.ChatAction.UPLOAD_DOCUMENT)
        await _send_tracked(lambda: msg.reply_document(str(pack_path), caption=caption))
    except Exception as e:
        logger.opt(exception=e).debug("详细堆栈")
        logger.error(f"上传失败: {e}")
        await reporter.report_error(_t("上传"), e)
        return
    finally:
        if not bs.debug_skip_cleanup:
            logger.debug("清理压缩包")
            os.remove(pack_path)

    await reporter.dismiss()


# ── 发送媒体 ─────────────────────────────────────────────────────────


async def _send_single(
    msg: Message,
    photos_videos: list[InputMediaPhoto | InputMediaVideo],
    animations: list[InputMediaAnimation],
    caption: str,
) -> list[CacheMedia] | None:
    """发送单个媒体，返回 CacheMedia 列表。上传失败时降级为 document。
    返回 None 表示不缓存
    """
    media_list: list[CacheMedia] = []
    all_media = animations + photos_videos

    try:
        sent: Message | None = None
        if animations:
            await msg.reply_chat_action(enums.ChatAction.UPLOAD_PHOTO)
            sent = await _send_tracked(
                lambda: msg.reply_animation(_media_input(animations[0].media), caption=caption)
            )
        else:
            single = photos_videos[0]
            match single:
                case InputMediaPhoto():
                    await msg.reply_chat_action(enums.ChatAction.UPLOAD_PHOTO)
                    sent = await _send_tracked(
                        lambda: msg.reply_photo(_media_input(single.media), caption=caption)
                    )
                case InputMediaVideo():
                    await msg.reply_chat_action(enums.ChatAction.UPLOAD_VIDEO)
                    try:
                        sent = await _send_tracked(
                            lambda: msg.reply_video(
                                _media_input(single.media),
                                caption=caption,
                                video_cover=single.video_cover,
                                duration=single.duration,
                                width=single.width,
                                height=single.height,
                                supports_streaming=True,
                            )
                        )
                    except (WebpageCurlFailed, WebpageMediaEmpty):
                        logger.warning("Tg 获取封面失败, 移除封面上传")
                        sent = await _send_tracked(
                            lambda: msg.reply_video(
                                _media_input(single.media),
                                caption=caption,
                                duration=single.duration,
                                width=single.width,
                                height=single.height,
                                supports_streaming=True,
                            )
                        )

        if sent and (cm := _cache_media_from_message(sent)):
            media_list.append(cm)
    except Exception as e:
        logger.warning(f"上传失败 {e}, 使用兼容模式上传")
        await msg.reply_chat_action(enums.ChatAction.UPLOAD_DOCUMENT)
        await _send_tracked(
            lambda: msg.reply_document(_media_input(all_media[0].media), caption=caption, force_document=True)
        )
        return None

    return media_list


def _build_gif_button(media_refs: Sequence[AnyMediaRef]) -> Ikm:
    buttons = []
    n = 5
    for i, v in enumerate(media_refs):
        if isinstance(v, AniRef):
            buttons.append(Ikb(f"{i + 1}", url=v.url))
    ikbs = [list(i) for i in batched(buttons, n)]
    return Ikm(ikbs)


async def _send_multi(
    msg: Message,
    photos_videos: list[InputMediaPhoto | InputMediaVideo],
    animations: list[InputMediaAnimation],
    caption: str,
    media_refs: Sequence[AnyMediaRef],
    *,
    _t: PreLocaleSelector,
) -> list[CacheMedia] | None:
    """发送多个媒体（动图逐条、图片视频分批），返回 CacheMedia 列表。
    返回 None 表示不缓存
    """
    media_list: list[CacheMedia] = []
    not_cache = False
    if len([i for i in media_refs if isinstance(i, AniRef)]) > GIF_ONLY_SKIP_DOWNLOAD_COUNT_THRESHOLD:
        not_cache = True
        await _send_tracked(
            lambda: msg.reply_text(_t("**▎GIF 过多跳过上传, 请自行下载**"), reply_markup=_build_gif_button(media_refs))
        )
    else:
        for ani in animations:
            await msg.reply_chat_action(enums.ChatAction.UPLOAD_PHOTO)
            caption_ = caption if ani == animations[-1] and not photos_videos else ""
            try:
                sent = await _send_tracked(
                    lambda a=ani, c=caption_: msg.reply_animation(  # type: ignore[misc]
                        _media_input(a.media),
                        caption=c,
                    )
                )
            except Exception as e:
                logger.warning(f"上传失败 {e}, 使用兼容模式上传")
                not_cache = True
                await msg.reply_chat_action(enums.ChatAction.UPLOAD_DOCUMENT)
                await _send_tracked(
                    lambda a=ani, c=caption_: msg.reply_document(_media_input(a.media), caption=c, force_document=True)  # type: ignore[misc]
                )
            else:
                # 过大的 GIF 会返回 document
                if sent and sent.document:
                    media_list.append(CacheMedia(type=CacheMediaType.DOCUMENT, file_id=sent.document.file_id))
                elif sent and sent.animation:
                    media_list.append(CacheMedia(type=CacheMediaType.ANIMATION, file_id=sent.animation.file_id))

    try:
        for batch in batched(photos_videos, 10):
            if batch[-1] == photos_videos[-1]:
                batch[0].caption = caption

            await msg.reply_chat_action(enums.ChatAction.UPLOAD_PHOTO)
            # noinspection PyDefaultArgument
            sent_msgs = await _send_tracked(lambda b=list(batch): msg.reply_media_group(media=b))  # type: ignore[misc]
            for m in sent_msgs:
                if cm := _cache_media_from_message(m):
                    media_list.append(cm)
    except Exception as e:
        logger.warning(f"上传失败 {e}, 使用兼容模式上传")
        input_documents: list[InputMediaDocument] = [
            InputMediaDocument(media=_media_input(item.media)) for item in photos_videos
        ]
        for document_batch in batched(input_documents, 10):
            if document_batch[-1] == input_documents[-1]:
                document_batch[-1].caption = caption

            await msg.reply_chat_action(enums.ChatAction.UPLOAD_DOCUMENT)
            # noinspection PyDefaultArgument
            await _send_tracked(lambda b=list(document_batch): msg.reply_media_group(media=b))  # type: ignore
        return None

    return None if not_cache else media_list


async def _send_media(
    msg: Message,
    parse_result: AnyParseResult,
    processed_list: list[ProcessedMedia],
    caption: str,
    *,
    _t: PreLocaleSelector,
) -> CacheEntry | None:
    """构建、发送媒体，并返回缓存条目。
    返回 None 表示不缓存
    """
    media_refs = to_list(parse_result.media)
    photos_videos, animations = _build_input_media(media_refs, processed_list)
    all_count = len(photos_videos) + len(animations)
    logger.debug(f"媒体分类完成: animations={len(animations)}, photos_videos={len(photos_videos)}")

    if all_count == 1:
        logger.debug("单媒体模式发送")
        media_list = await _send_single(msg, photos_videos, animations, caption)
    else:
        logger.debug(f"多媒体模式发送: total={all_count}")
        media_list = await _send_multi(msg, photos_videos, animations, caption, media_refs, _t=_t)

    if media_list is None:
        return None
    return _make_cache_entry(parse_result, media_list)


# ── 缓存发送 ─────────────────────────────────────────────────────────


async def _send_cached(msg: Message, entry: CacheEntry, url: str, *, user_config: UserConfig) -> None:
    """从 file_id 缓存直接发送，跳过解析/下载/转码"""
    logger.debug(f"缓存发送: media={entry.media}")
    caption = build_caption_by_str(
        entry.parse_result.title,
        entry.parse_result.content,
        url,
        entry.telegraph_url,
        hide_source=user_config.hide_source,
    )

    # 富文本类型
    if entry.telegraph_url:
        await _track_output(
            await msg.reply_text(
                caption,
                link_preview_options=LinkPreviewOptions(show_above_text=True),
            )
        )
        return

    if not entry.media:
        await _track_output(
            await msg.reply_text(
                caption,
                link_preview_options=LinkPreviewOptions(is_disabled=True),
            )
        )
        return

    if len(entry.media) == 1:
        await _send_cached_single(msg, entry.media[0], caption)
    else:
        await _send_cached_multi(msg, entry.media, caption)


async def _send_cached_single(msg: Message, m: CacheMedia, caption: str) -> None:
    """从缓存发送单个媒体。"""
    match m.type:
        case CacheMediaType.PHOTO:
            await msg.reply_chat_action(enums.ChatAction.UPLOAD_PHOTO)
            await _send_tracked(lambda: msg.reply_photo(m.file_id, caption=caption))
        case CacheMediaType.VIDEO:
            await msg.reply_chat_action(enums.ChatAction.UPLOAD_VIDEO)
            await _send_tracked(
                lambda: msg.reply_video(
                    m.file_id, caption=caption, supports_streaming=True, video_cover=m.cover_file_id
                )
            )
        case CacheMediaType.ANIMATION:
            await msg.reply_chat_action(enums.ChatAction.UPLOAD_PHOTO)
            await _send_tracked(lambda: msg.reply_animation(m.file_id, caption=caption))
        case CacheMediaType.DOCUMENT:
            await msg.reply_chat_action(enums.ChatAction.UPLOAD_DOCUMENT)
            await _send_tracked(lambda: msg.reply_document(m.file_id, caption=caption, force_document=True))


async def _send_cached_multi(msg: Message, media: list[CacheMedia], caption: str) -> None:
    """从缓存发送多个媒体。"""
    animations = [m for m in media if m.type == CacheMediaType.ANIMATION]
    others = [m for m in media if m.type != CacheMediaType.ANIMATION]

    for ani in animations:
        await msg.reply_chat_action(enums.ChatAction.UPLOAD_PHOTO)
        await _send_tracked(
            lambda a=ani: msg.reply_animation(  # type: ignore[misc]
                a.file_id,
                caption=caption if a == animations[-1] and not others else "",
            )
        )

    media_group = _build_cached_media_group(others)
    for batch in batched(media_group, 10):
        if batch[-1] == media_group[-1]:
            batch[0].caption = caption

        await msg.reply_chat_action(enums.ChatAction.UPLOAD_PHOTO)
        # noinspection PyDefaultArgument
        await _send_tracked(lambda m=list(batch): msg.reply_media_group(m))  # type: ignore[misc]


def _build_cached_media_group(
    media: list[CacheMedia],
) -> list[InputMediaPhoto | InputMediaVideo | InputMediaDocument]:
    """从 CacheMedia 列表构建 Telegram media group。"""
    group: list[InputMediaPhoto | InputMediaVideo | InputMediaDocument] = []
    for m in media:
        match m.type:
            case CacheMediaType.PHOTO:
                group.append(InputMediaPhoto(media=m.file_id))
            case CacheMediaType.VIDEO:
                if m.cover_file_id:
                    group.append(InputMediaVideo(media=m.file_id, supports_streaming=True, video_cover=m.cover_file_id))
                else:
                    group.append(InputMediaVideo(media=m.file_id, supports_streaming=True))
            case CacheMediaType.DOCUMENT:
                group.append(InputMediaDocument(media=m.file_id))
    return group
