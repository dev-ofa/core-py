# core-py

`core-py` 是 `core-go` 能力在 Python 侧的实现，采用 Python 指南推荐的 `src/` 布局，避免照搬 Go 的目录结构。

## 模块概览

- `core_py.config`：配置加载、合并优先级、必填校验、敏感项校验、脱敏摘要与稳定哈希。
- `core_py.context`：跨调用上下文透传，统一 `OFA_PASS_*` 与 `OFA_DIRECT_*` Key。
- `core_py.trace`：trace/request header 常量与 ID 生成。
- `core_py.logging`：携带 trace/request 的统一日志门面。
- `core_py.httpx`：带 trace 透传、超时预算、有限重试与可插拔服务发现的 HTTP client。
- `core_py.model`：实体、分页、审计字段与上下文审计注入。
- `core_py.data`：业务错误码、错误类型、分页排序输入。
- `core_py.dkit`：分布式原语协议、锁辅助、默认 ID 生成与内存实现，外部后端通过协议注入。

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

```python
from core_py import context, httpx

ctx = context.ctx_set_trace_id(context.empty_context(), "trace-1")
ctx = context.ctx_set_operator(ctx, "user-1")

payload = {}
httpx.get("http://example.com/api", httpx.Context(ctx), httpx.JSONResp(payload)).do()
```

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
