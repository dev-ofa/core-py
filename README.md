# core-py

`core-py` 是 `core-go` 能力在 Python 侧的实现，采用 Python 指南推荐的 `src/` 布局，`core-go` 目录可以参考，但实现和 API 以 Python 语言的最佳实践和规范为主。

## API 约定

- `core-py` 优先提供 Python 风格 API，公开接口使用 `snake_case` 命名。
- 不要求保留 Go-style alias、大小写兼容入口或按 Go 包名一比一镜像的迁移层。
- 与 `core-go` 对齐时，优先保证行为语义、协议字段、存储格式和运行时兼容，而不是逐个复刻 Go 命名。

## 并发与 IO 约定

- `core-py` 对运行期 IO 模块采用 async-first 设计；默认公开主路径应可 `await`，避免上游 async 服务在事件循环中误用阻塞 API。
- 纯逻辑、纯数据和上下文传播模块保持同步 API；不要为了形式统一把不涉及运行期 IO 的能力机械改成 `async def`。
- 新模块如果涉及网络、数据库、缓存、分布式锁、资源句柄或长生命周期后台任务，应优先提供异步 API，而不是先写同步实现再用异步外壳包装。
- 当前仓库没有上游接入包袱时，不为新的 async-first 设计保留 sync 兼容层；如需破坏性调整，优先直接统一公开接口，而不是长期维护双套入口。
- 库内部不要用 `asyncio.to_thread()`、线程池或同步客户端把阻塞实现伪装成异步主路径；确需桥接时，应明确限制场景，并避免成为默认入口。
- 构造函数、属性访问和轻量辅助函数不应隐式执行运行期 IO；需要连接、初始化、打开流或启动后台任务时，应通过显式 `await` 或 `async with` 暴露。
- 异步资源必须提供明确的关闭语义，例如 `aclose()`、`async with` 和取消后的清理保证；超时、取消、重试和资源释放应作为 API 语义的一部分设计。
- 在 async 主路径中禁止 `time.sleep()`、同步 socket/HTTP/DB 调用和不受控后台线程；这类实现会阻塞事件循环，应替换为协程 sleep、异步客户端和可取消任务。

## 模块概览

- `core_py.config`：配置加载、合并优先级、必填校验、敏感项校验、脱敏摘要与稳定哈希。
- `core_py.context`：跨调用上下文透传，统一 `OFA_PASS_*` 与 `OFA_DIRECT_*` Key。
- `core_py.trace`：trace/request header 常量与 ID 生成。
- `core_py.logging`：携带 trace/request 的统一日志门面。
- `core_py.httpx`：带 trace 透传、超时预算、有限重试与可插拔服务发现的 HTTP client。
- `core_py.resource`：统一资源标识解析、异步打开、下载与上传能力。
- `core_py.model`：实体、分页、审计字段与上下文审计注入。
- `core_py.model.mongox`：面向 Mongo collection 的异步仓储实现。
- `core_py.data`：业务错误码、错误类型、分页排序输入。
- `core_py.dkit`：分布式原语协议、锁辅助、默认 ID 生成，以及内存、Redis、MongoDB 后端入口。

## 快速使用

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

## 配置行为

- 不支持热更新，配置变更需要重启进程后生效。
- 标准配置来源优先级为：默认配置文件 < 环境对应配置文件 < 本地覆盖文件 < 环境变量。
- 默认配置文件为 `configs/config.yaml`；设置 `ENV=dev` 时，`configs/config.dev.yaml` 会作为环境对应配置文件参与最终配置计算；存在 `configs/config.local.yaml` 时会作为本地覆盖文件参与最终配置计算。
- `Options.default_config_path` 表示默认基础配置文件路径。显式传入该路径时，当前实现会以该文件作为基础配置来源，并继续在该文件所在目录查找 `config.{env}.yaml` 和 `config.local.yaml`。
- 环境变量默认使用 `APP` 前缀和 `__` 层级分隔符，例如 `APP__DB__URI` 对应规范路径 `db.uri`。
- 环境变量名必须使用大写 ASCII 字母、数字与下划线；不符合该规则的环境变量会被忽略。
- `Options.args` 是 `core-py` 的实现扩展，不属于标准配置来源。当前实现支持 `--group.key=value`，优先级高于环境变量，仅建议用于本地调试、临时诊断和测试场景。
- 命令行参数不得用于传入密钥、密码、Token 等敏感配置；共享环境中的敏感配置必须来自环境变量或安全存储，本地开发允许通过 `config.local.yaml` 提供敏感配置。
- 启动日志会记录 `config sources`、脱敏摘要和稳定哈希，用于排查最终配置来源与差异。

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

## Async API 说明

- 当前版本的运行期 IO 能力默认采用 async-first 设计，`httpx`、`resource`、`dkit`、`model.mongox` 的主路径都应在协程中通过 `await` 调用。
- `context`、`trace`、`logging`、`config`、`data` 这类纯逻辑或启动期能力仍保持同步接口，可直接在 async 函数中配合使用。
- 需要释放资源的对象应显式关闭，例如 `async with` 打开的 `resource.Stream`、`httpx.StreamResponse`，以及使用完成后的 `dkit.Atomic` 实例。
- README 中的示例就是当前推荐用法，不再提供同步兼容写法。

## 命令

本项目使用 `uv` 管理依赖和锁文件。首次进入项目后执行：

```bash
make sync
```

常用命令：

```bash
make lock   # 生成或更新 uv.lock
make fmt    # 格式化 src 和 tests
make lint   # 运行 ruff check
make type   # 运行 mypy
make test   # 运行 pytest
make build  # 构建 sdist 和 wheel
```

完整本地验证：

```bash
make sync fmt lint type test
```

## 打包与发布

打包发布同样基于 `uv`：

```bash
make build
```

会在 `dist/` 下生成源码包和 wheel。

发布到 PyPI：

```bash
make publish-pip
```

执行前需要先准备好 `uv publish` 所需凭据，例如设置 `UV_PUBLISH_TOKEN`，或按 `uv` 的发布配置方式准备认证信息。

发布到本地调试环境：

```bash
make publish-local
```

该命令会先构建 wheel，再通过 `uv pip install --system --force-reinstall dist/*.whl` 安装到当前 Python 环境，便于在本机做集成调试。

从本地环境移除：

```bash
make unpublish-local
```
