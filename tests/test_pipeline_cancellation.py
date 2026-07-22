import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from easy_ai18n import PreLocaleSelector
from parsehub.types import AnyParseResult, PostType

from services import pipeline
from services.pipeline import ParsePipeline


class Reporter:
    async def report(self, _: str) -> None:
        return

    async def report_error(self, _: str, __: Exception) -> None:
        return

    async def dismiss(self) -> None:
        return


@pytest.mark.asyncio
async def test_pipeline_cancellation_removes_partial_download_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    download_started = asyncio.Event()

    class FakeParseResult:
        type = PostType.VIDEO
        media = SimpleNamespace()

        async def download(self, download_root: Path, **_: Any) -> None:
            partial_dir = download_root / "partial"
            partial_dir.mkdir(parents=True)
            (partial_dir / "partial.mp4").write_bytes(b"partial")
            download_started.set()
            await asyncio.Event().wait()

    class FakeParseService:
        parser = SimpleNamespace(get_platform=lambda _: SimpleNamespace(id="youtube"))

    monkeypatch.setattr(pipeline, "ParseService", FakeParseService)
    monkeypatch.setattr(pipeline.bs, "download_dir", tmp_path)
    monkeypatch.setattr(pipeline.bs, "debug_skip_cleanup", False)
    monkeypatch.setattr(pipeline, "pl_cfg", SimpleNamespace(roll_downloader_proxy=lambda _: None))
    parse_result = cast(AnyParseResult, FakeParseResult())

    async def run_pipeline() -> None:
        with ParsePipeline(
            "https://youtu.be/video",
            "https://youtu.be/video",
            Reporter(),
            parse_result=parse_result,
            singleflight=False,
            _t=cast(PreLocaleSelector, lambda text: text),
        ) as parse_pipeline:
            await parse_pipeline.run()

    task = asyncio.create_task(run_pipeline())
    await download_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert list(tmp_path.iterdir()) == []
