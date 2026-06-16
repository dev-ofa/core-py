from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from core_py import data, model
from core_py.model.mongox.codec import decode_document, to_document


@dataclass
class SnowflakeItem(model.Entity[model.SnowflakeID]):
    name: str = ""


def test_snowflake_id_is_json_string_and_numeric_convertible() -> None:
    id_ = model.SnowflakeID(623949464310157351)

    assert id_ == "623949464310157351"
    assert int(id_) == 623949464310157351
    assert json.dumps({"id": id_}) == '{"id": "623949464310157351"}'


def test_snowflake_id_codec_uses_numeric_storage_and_string_entity_value() -> None:
    item = SnowflakeItem(id=model.SnowflakeID("623949464310157351"), name="item")

    doc = to_document(item, "_id")
    assert doc["_id"] == 623949464310157351
    assert isinstance(doc["_id"], int)

    decoded = decode_document(SnowflakeItem, doc, "_id")
    assert isinstance(decoded, SnowflakeItem)
    assert decoded.id == "623949464310157351"
    assert isinstance(decoded.id, model.SnowflakeID)


def test_snowflake_id_parse_errors_are_validation_errors() -> None:
    with pytest.raises(data.ValidationError) as exc_info:
        model.SnowflakeID("bad")

    assert data.code_of(exc_info.value) == data.ERR_CODE_VALIDATE
