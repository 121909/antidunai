from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_compose_builds_the_antidunai_worktree() -> None:
    compose_path = ROOT / "docker-compose.yaml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    bot = compose["services"]["bot"]

    assert compose["name"] == "antidunai"
    assert bot["image"] == "antidunai:burst-video-guard"
    assert bot["build"] == {"context": ".", "dockerfile": "Dockerfile"}
    assert bot["env_file"] == [".env"]
    assert "z-mio/parse_hub_bot" not in compose_path.read_text(encoding="utf-8")


def test_documented_runtime_does_not_pull_the_upstream_bot_image() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "ghcr.io/z-mio/parse_hub_bot" not in readme
    assert "COPY . ." in dockerfile
    assert 'CMD ["python", "bot.py"]' in dockerfile
