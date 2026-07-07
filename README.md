# core-py

`core-py` is the Python foundation library implementing the OFA spec. It follows the recommended Python `src/` layout. The OFA spec under `docs/spec/` is the compatibility source of truth. `core-go` and other standard libraries implementing the OFA spec are useful references, but `core-py` does not aim to mechanically mirror any single implementation. Its public APIs should stay Pythonic while keeping semantics and recommended usage close to other standard OFA libraries.

## API Conventions

- `docs/spec` is the authority for cross-language behavior, protocol fields, storage formats, and runtime compatibility.
- `core-py` exposes Python-style APIs first, with public interfaces named in `snake_case`.
- There is no requirement to preserve Go-style aliases, case-compatible entry points, or a one-to-one migration layer mirroring Go package names.
- `core-go` is a useful reference implementation, but not the source of truth for `core-py`.
- When aligning with other OFA implementations, prioritize stable behavior, protocol fields, storage formats, runtime compatibility, and similar usage patterns over mechanically copying `core-go` naming or package layout.

## Concurrency and IO Conventions

- `core-py` uses an async-first design for runtime IO modules. The default public path should be awaitable so upstream async services do not accidentally block the event loop.
- Pure logic, pure data, and context-propagation modules stay synchronous. Do not turn non-IO capabilities into `async def` purely for consistency.
- New modules involving networks, databases, caches, distributed locks, resource handles, or long-lived background tasks should expose async APIs first instead of wrapping a synchronous implementation in an async shell.
- When there is no compatibility burden from upstream integrations, do not keep sync compatibility layers for new async-first designs. If a breaking change is needed, prefer unifying the public interface directly rather than maintaining dual entry points long term.
- Do not use `asyncio.to_thread()`, thread pools, or synchronous clients inside the library to disguise blocking implementations as async-first code paths. If bridging is necessary, scope it explicitly and keep it away from the default entry path.
- Constructors, property access, and lightweight helpers must not perform runtime IO implicitly. Expose connection setup, initialization, stream opening, and background task startup through explicit `await` or `async with`.
- Async resources must define explicit shutdown semantics such as `aclose()`, `async with`, and cleanup guarantees after cancellation. Timeouts, cancellation, retries, and resource release should all be part of the API contract.
- The async main path must not call `time.sleep()`, synchronous socket/HTTP/DB APIs, or unmanaged background threads. These block the event loop and should be replaced with coroutine sleep, async clients, and cancellable tasks.

## Modules

- `core_py.config`: config loading, merge precedence, required-key validation, sensitive-key validation, redacted summaries, and stable hashing
- `core_py.context`: cross-call context propagation with unified `ofa-pass-*`, `ofa-direct-*`, and `ofa-*` string keys
- `core_py.trace`: trace/request header constants and ID generation
- `core_py.logging`: unified logging facade carrying trace/request context
- `core_py.httpx`: HTTP client with trace propagation, timeout budgets, bounded retries, and pluggable service discovery
- `core_py.resource`: unified resource identifier parsing plus async open, download, and upload support
- `core_py.model`: entities, paging, audit fields, and context-driven audit injection
- `core_py.model.mongox`: async repository implementation for Mongo collections
- `core_py.data`: business error codes, error types, and paging/sort inputs
- `core_py.dkit`: distributed primitive protocols, mutex helpers, default ID generation, and in-memory/Redis/MongoDB backends

## Quick Start

```python
from dataclasses import dataclass
from core_py import config

@dataclass
class DBConfig:
    uri: str = ""

@dataclass
class AppConfig:
    db: DBConfig

cfg, meta = config.load(AppConfig, config.Options(required_keys=["db.uri"]))
```

## Config Behavior

- Hot reload is not supported. Config changes require a process restart.
- Standard config source precedence is: default config file < environment-specific config file < local override file < environment variables.
- The default config file is `configs/config.yaml`. When `APP__ENV=dev`, `configs/config.dev.yaml` is included as the environment-specific source. If `configs/config.local.yaml` exists, it participates as the local override source.
- `Options.default_config_path` is the path to the base config file. When passed explicitly, the current implementation uses that file as the base source and keeps looking for `config.{env}.yaml` and `config.local.yaml` in the same directory.
- Environment variables use the `APP` prefix and `__` as the hierarchy separator by default. For example, `APP__DB__URI` maps to the canonical path `db.uri`.
- The deployment environment selector also participates in final config overrides. With the defaults, `APP__ENV=dev` maps to `app.env=dev`. Customizing `Options.env_prefix`, `Options.env_separator`, or `Options.deploy_env_key` changes both the deployment environment variable name and the final config path, e.g. `SERVICE__PROFILE=dev` maps to `service.profile=dev`.
- Environment variable names must use uppercase ASCII letters, digits, and underscores. Names outside that rule are ignored.
- `Options.args` is a `core-py` implementation extension and not part of the standard config sources. The current implementation supports `--group.key=value` with precedence above environment variables, and it should only be used for local debugging, temporary diagnostics, and tests.
- Command-line flags must not be used for secrets, passwords, tokens, or other sensitive config. In shared environments, sensitive config must come from environment variables or secure storage. Local development may use `config.local.yaml` for sensitive values.
- Startup logs record `config sources`, a redacted summary, and a stable hash to help diagnose the final config source and effective differences.

```python
import asyncio

from core_py import context, httpx

async def main() -> None:
    payload = {}
    with context.use_context():
        context.set_trace_id("trace-1")
        context.set_operator("user-1")
        await httpx.get(
            "http://example.com/api",
            httpx.json_resp(payload),
        ).do()

    print(payload)

asyncio.run(main())
```

```python
import asyncio

from core_py import resource

async def main() -> None:
    manager = resource.Manager()
    async with await manager.open("ofa-res#data:text/plain;base64,aGVsbG8=") as stream:
        body = await stream.body.read()
        print(body.decode())

asyncio.run(main())
```

```python
import asyncio

from core_py import dkit

async def main() -> None:
    atomic = dkit.InMemoryAtomic(default_ttl=1)
    kit = await dkit.new_default_kit(atomic)

    async def critical_section() -> None:
        print(kit.get_snowflake_id())

    await kit.mutex_do("job", critical_section)
    await atomic.close()

asyncio.run(main())
```

```python
import asyncio
from dataclasses import dataclass

from core_py import context, model
from core_py.model import mongox

@dataclass
class Item(model.CreateAudit, model.UpdateAudit, model.DeleteAudit, model.TenantAudit):
    id: str = ""
    name: str = ""

async def main(collection: object) -> None:
    repo = mongox.CollectionRepository[str, Item](collection, Item).with_repo_opt(
        model.RepoOpt(data_isolation=model.DATA_ISOLATION_TENANT)
    )

    with context.use_context():
        context.set_operator("user-1")
        context.set_tenant_id("tenant-1")
        created = await repo.create(Item(id="i-1", name="first"))
        loaded = await repo.get(created.id)
        print(loaded.name)

asyncio.run(main(collection))
```

## Async API Notes

- In the current version, runtime IO capabilities follow an async-first design by default. The primary paths for `httpx`, `resource`, `dkit`, and `model.mongox` should be called with `await`.
- Pure-logic or startup-time modules such as `context`, `trace`, `logging`, `config`, and `data` remain synchronous and can be used directly from async functions.
- Objects that own resources should be closed explicitly, such as `resource.Stream` opened with `async with`, `httpx.StreamResponse`, and `dkit.Atomic` instances after use.
- The examples in this README are the recommended usage patterns. No sync-compatibility variants are provided here.

## Commands

This project uses `uv` to manage dependencies and the lockfile. Run the following when entering the project for the first time:

```bash
make sync
```

Common commands:

```bash
make lock   # generate or update uv.lock
make fmt    # format src and tests
make lint   # run ruff check
make type   # run mypy
make test   # run pytest
make build  # build sdist and wheel
```

Full local verification:

```bash
make sync fmt lint type test
```

## Packaging and Publishing

Packaging and publishing also use `uv`:

```bash
make build
```

This generates the source distribution and wheel under `dist/`.

Publish to PyPI:

```bash
make publish-pip
```

Before running this command, prepare the credentials required by `uv publish`, for example by setting `UV_PUBLISH_TOKEN` or by configuring authentication the way `uv` expects.

Publish to a local debugging environment:

```bash
make publish-local
```

This command builds the wheel first, then installs it into the current Python environment with `uv pip install --system --force-reinstall dist/*.whl`, which is convenient for local integration debugging.

Remove from the local environment:

```bash
make unpublish-local
```
