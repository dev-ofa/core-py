from dataclasses import dataclass, field

import pytest

from core_py import config, data


@dataclass
class HTTPConfig:
    port: int = 0


@dataclass
class DBConfig:
    uri: str = ""


@dataclass
class AppConfig:
    app: dict[str, str] = field(default_factory=dict)
    http: HTTPConfig = field(default_factory=HTTPConfig)
    db: DBConfig = field(default_factory=DBConfig)
    debug: bool = False


@dataclass
class ServiceConfig:
    profile: str = ""


@dataclass
class CustomDeployEnvConfig:
    service: ServiceConfig = field(default_factory=ServiceConfig)
    http: HTTPConfig = field(default_factory=HTTPConfig)
    db: DBConfig = field(default_factory=DBConfig)


@dataclass
class FeatureConfig:
    ratio: float = 0.0
    enabled: bool = False


@dataclass
class StrictConfig:
    name: str = ""


def test_load_merges_files_env_and_flags(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "config.yaml").write_text(
        "http:\n  port: 8080\ndb:\n  uri: sqlite://default\ndebug: false\n"
    )
    (cfg_dir / "config.local.yaml").write_text("http:\n  port: 8081\n")
    monkeypatch.setenv("APP__ENV", "dev")
    monkeypatch.setenv("APP__DB__URI", "sqlite://env")
    monkeypatch.setenv("APP__HTTP__PORT", "9090")

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

    assert out.http.port == 9090
    assert out.db.uri == "sqlite://env"
    assert out.app["env"] == "dev"
    assert out.debug is True
    assert meta.sources == ["default", "local", "env", "flags"]
    assert meta.summary["db"]["uri"] == "***"
    assert len(meta.hash) == 64


def test_sensitive_default_file_value_must_be_overridden_by_env_or_local(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("db:\n  uri: sqlite://file\n")
    with pytest.raises(data.ValidationError, match="sensitive config"):
        config.load(
            dict,
            config.Options(
                default_config_path=str(path), sensitive_keys=["uri"], log_enabled=False
            ),
        )


def test_sensitive_local_file_value_is_allowed(tmp_path):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "config.yaml").write_text("db:\n  uri: ''\n")
    (cfg_dir / "config.local.yaml").write_text("db:\n  uri: sqlite://local\n")
    out, _ = config.load(
        dict,
        config.Options(
            default_config_path=str(cfg_dir / "config.yaml"),
            sensitive_keys=["uri"],
            log_enabled=False,
        ),
    )

    assert out == {"db": {"uri": "sqlite://local"}}


def test_sensitive_local_placeholder_is_rejected(tmp_path):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "config.yaml").write_text("db:\n  uri: ''\n")
    (cfg_dir / "config.local.yaml").write_text("db:\n  uri: '******'\n")

    with pytest.raises(data.ValidationError, match="placeholder"):
        config.load(
            dict,
            config.Options(
                default_config_path=str(cfg_dir / "config.yaml"),
                sensitive_keys=["uri"],
                log_enabled=False,
            ),
        )


def test_sensitive_env_placeholder_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text("db:\n  uri: ''\n")
    monkeypatch.setenv("APP__DB__URI", "******")

    with pytest.raises(data.ValidationError, match="placeholder"):
        config.load(
            dict,
            config.Options(
                default_config_path=str(path), sensitive_keys=["uri"], log_enabled=False
            ),
        )


def test_load_reads_env_file_and_runs_validators(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "config.yaml").write_text("feature:\n  ratio: 0.5\n  enabled: false\n")
    (cfg_dir / "config.dev.yaml").write_text("feature:\n  ratio: 0.75\n")
    seen: dict[str, object] = {}

    def validate_map(raw: dict[str, object]) -> None:
        seen["map"] = raw["feature"]

    def validate_config(cfg: dict[str, object]) -> None:
        seen["config"] = cfg["feature"]

    monkeypatch.setenv("APP__ENV", "DEV")
    out, meta = config.load(
        dict,
        config.Options(
            default_config_path=str(cfg_dir / "config.yaml"),
            validate_map=validate_map,
            validate_config=validate_config,
            sensitive_keys=[],
            log_enabled=False,
        ),
    )

    assert out == {"app": {"env": "DEV"}, "feature": {"ratio": 0.75, "enabled": False}}
    assert meta.sources == ["default", "env-file", "env"]
    assert seen == {
        "map": {"ratio": 0.75, "enabled": False},
        "config": {"ratio": 0.75, "enabled": False},
    }



@pytest.mark.parametrize(
    ("args", "message"),
    [
        pytest.param(["--debug=maybe"], "expected bool", id="invalid-bool"),
        pytest.param(["--http.port=oops"], "expected int", id="invalid-int"),
        pytest.param(["--feature.ratio=oops"], "expected float", id="invalid-float"),
    ],
)
def test_load_rejects_invalid_scalar_overrides(args, message):
    @dataclass
    class LocalConfig:
        http: HTTPConfig = field(default_factory=HTTPConfig)
        feature: FeatureConfig = field(default_factory=FeatureConfig)
        debug: bool = False

    with pytest.raises(data.ValidationError, match=message):
        config.load(
            LocalConfig,
            config.Options(args=args, sensitive_keys=[], log_enabled=False),
        )


def test_load_honors_strict_unknown_field_flag(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("name: demo\nextra: ignored\n")

    with pytest.raises(data.ValidationError, match="unknown config fields"):
        config.load(
            StrictConfig,
            config.Options(default_config_path=str(path), sensitive_keys=[], log_enabled=False),
        )

    out, _ = config.load(
        StrictConfig,
        config.Options(
            default_config_path=str(path),
            sensitive_keys=[],
            strict=False,
            log_enabled=False,
        ),
    )
    assert out == StrictConfig(name="demo")


def test_private_helpers_normalize_and_mask_values(monkeypatch):
    monkeypatch.setenv("APP__DB__URI", "postgres://env-user:env-pass@db.example/app")
    monkeypatch.setenv("APP__HTTP__PORT", "8080")

    assert config._env_to_map("APP", "__") == {
        "db": {"uri": "postgres://env-user:env-pass@db.example/app"},
        "http": {"port": "8080"},
    }
    assert config._args_to_map(["--feature.enabled=true", "--feature.ratio=0.8"]) == {
        "feature": {"enabled": "true", "ratio": "0.8"}
    }
    assert config._load_config_if_exists("")[1] is False
    assert (
        config._mask_uri("postgres://user:pass@db.example/app")
        == "postgres://user:***@db.example/app"
    )
    assert config._mask_map(
        {
            "db": {"uri": "postgres://user:pass@db.example/app"},
            "feature": {"webhook": "https://example.test/hook"},
        },
        ["uri"],
    ) == {
        "db": {"uri": "***"},
        "feature": {"webhook": "https://example.test/hook"},
    }


def test_env_to_map_ignores_invalid_env_names(monkeypatch):
    monkeypatch.setenv("APPTEST__HTTP__PORT", "8080")
    monkeypatch.setenv("APPTEST__db__uri", "sqlite://invalid")
    monkeypatch.setenv("APPTEST____BROKEN", "invalid")

    assert config._env_to_map("APPTEST", "__") == {"http": {"port": "8080"}}


def test_deploy_env_selector_maps_by_env_rules(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "config.yaml").write_text(
        "app:\n  name: demo\nhttp:\n  port: 8080\ndb:\n  uri: sqlite://default\n"
    )
    (cfg_dir / "config.dev.yaml").write_text("http:\n  port: 8081\n")
    monkeypatch.setenv("APP__ENV", "dev")
    monkeypatch.setenv("APP__DB__URI", "sqlite://env")

    out, meta = config.load(
        dict,
        config.Options(
            default_config_path=str(cfg_dir / "config.yaml"),
            required_keys=["db.uri"],
            sensitive_keys=["uri"],
            log_enabled=False,
        ),
    )

    assert out["app"]["env"] == "dev"
    assert out["http"]["port"] == 8081
    assert "env" not in out
    assert "env" not in meta.summary


def test_custom_deploy_env_key_changes_final_path(tmp_path, monkeypatch):
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    (cfg_dir / "config.yaml").write_text(
        "http:\n  port: 8080\ndb:\n  uri: sqlite://default\n"
    )
    (cfg_dir / "config.dev.yaml").write_text("http:\n  port: 8081\n")
    monkeypatch.setenv("SERVICE__PROFILE", "dev")
    monkeypatch.setenv("SERVICE__DB__URI", "sqlite://env")

    out, meta = config.load(
        CustomDeployEnvConfig,
        config.Options(
            default_config_path=str(cfg_dir / "config.yaml"),
            env_prefix="SERVICE",
            deploy_env_key="PROFILE",
            required_keys=["db.uri"],
            sensitive_keys=["uri"],
            log_enabled=False,
        ),
    )

    assert out.service.profile == "dev"
    assert out.http.port == 8081
    assert meta.sources == ["default", "env-file", "env"]
