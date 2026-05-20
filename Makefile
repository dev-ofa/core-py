.PHONY: sync lock fmt lint type test build publish publish-pip publish-local unpublish-local

PACKAGE_NAME := ofa-core-py

sync:
	uv sync --extra dev

lock:
	uv lock

fmt:
	uv run ruff format src tests

lint:
	uv run ruff check src tests

type:
	uv run mypy src

test:
	uv run pytest

build:
	uv build

publish: publish-pip

publish-pip:
	uv build
	uv publish

publish-local:
	uv build
	uv pip install --system --force-reinstall dist/*.whl

unpublish-local:
	-uv pip uninstall --system $(PACKAGE_NAME)
