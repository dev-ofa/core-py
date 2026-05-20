from dataclasses import dataclass, field

import pytest

from core_py import config


@dataclass
class HTTPConfig:
    port: int = 0


@dataclass
class DBConfig:
    uri: str = ""


@dataclass
class AppConfig:
    http: HTTPConfig = field(default_factory=HTTPConfig)
    db: DBConfig = field(default_factory=DBConfig)
    debug: bool = False


def test_load_merges_files_env_and_flags(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "config.yaml").write_text(
        "http:\n  port: 8080\ndb:\n  uri: sqlite://default\ndebug: false\n"
    )
    (cfg_dir / "config.local.yaml").write_text("http:\n  port: 8081\n")
    monkeypatch.setenv("APP.db.uri", "sqlite://env")

    out, meta = config.load(
        AppConfig,
        config.Options(
            default_config_path=str(cfg_dir / "config.yaml"),
            required_keys=["db.uri"],
            args=["--debug=true"],
            sensitive_keys=["uri"],
            log_enabled=False,
        ),
    )

    assert out.http.port == 8081
    assert out.db.uri == "sqlite://env"
    assert out.debug is True
    assert meta.sources == ["default", "env", "local", "flags"]
    assert meta.summary["db"]["uri"] == "***"
    assert len(meta.hash) == 64


def test_sensitive_file_value_must_be_overridden_by_env(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("db:\n  uri: sqlite://file\n")
    with pytest.raises(ValueError, match="sensitive config"):
        config.load(
            dict,
            config.Options(
                default_config_path=str(path), sensitive_keys=["uri"], log_enabled=False
            ),
        )
