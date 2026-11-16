"""Map Singer JSON Schema to Arrow types, and Singer record values to Arrow arrays.

Singer taps describe every stream with a JSON Schema. The mapping used here:

============================================  ==================================
JSON Schema                                   Arrow / DuckDB
============================================  ==================================
``integer``                                   ``int64`` / BIGINT
``number``                                    ``float64`` / DOUBLE
``boolean``                                   ``bool`` / BOOLEAN
``string``                                    ``string`` / VARCHAR
``string`` + ``format: date-time``            ``timestamp[us, UTC]`` / TIMESTAMPTZ
``string`` + ``format: date``                 ``date32`` / DATE
``string`` + ``format: time``                 ``time64[us]`` / TIME
``object`` with ``properties``                ``struct`` / STRUCT (if stable)
``array`` with ``items``                      ``list`` / LIST
anything open or ambiguous                    ``arrow.json`` / JSON
============================================  ==================================

``null`` in a ``type`` list, or a ``{"type": "null"}`` branch of ``anyOf`` /
``oneOf``, only makes the column nullable (every column is nullable anyway:
Singer does not guarantee a property is present). An object is "stable" when
it declares ``properties`` and does not allow arbitrary extra keys
(``additionalProperties`` / ``patternProperties``), and is no deeper than
``MAX_STRUCT_DEPTH``; otherwise it becomes JSON so no data is lost.

Values that do not fit the declared type (``"abc"`` in an integer column, an
unparseable timestamp) become NULL rather than failing the scan; the count of
such values is logged to stderr.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
from collections.abc import Callable, Iterable
from typing import Any

import pyarrow as pa

log = logging.getLogger(__name__)

MAX_STRUCT_DEPTH = 4
JSON_TYPE: pa.DataType = pa.json_()
TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")

_SCALAR_TYPES = {"integer", "number", "boolean", "string"}


def _branches(schema: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the non-null alternatives of ``schema`` (``type`` lists and ``anyOf``/``oneOf``)."""
    for key in ("anyOf", "oneOf"):
        alts = schema.get(key)
        if isinstance(alts, list) and alts:
            out: list[dict[str, Any]] = []
            for alt in alts:
                if isinstance(alt, dict):
                    out.extend(_branches({**{k: v for k, v in schema.items() if k != key}, **alt}))
            return out
    types = schema.get("type")
    if isinstance(types, list):
        return [{**schema, "type": t} for t in types if t != "null"]
    if types == "null":
        return []
    return [schema]


def _merge_numeric(branches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """``integer`` + ``number`` is just ``number``."""
    kinds = {b.get("type") for b in branches}
    if kinds == {"integer", "number"}:
        return [b for b in branches if b.get("type") == "number"][:1]
    return branches


def _string_type(fmt: Any) -> pa.DataType:
    if fmt == "date-time":
        return TIMESTAMP_TYPE
    if fmt == "date":
        return pa.date32()
    if fmt == "time":
        return pa.time64("us")
    return pa.string()


def arrow_type(schema: Any, depth: int = 0) -> pa.DataType:
    """Return the Arrow type for one JSON Schema (sub)schema."""
    if not isinstance(schema, dict):
        return JSON_TYPE
    branches = _merge_numeric(_branches(schema))
    if len(branches) != 1:
        # Several distinct non-null types (or none at all): keep the raw JSON value.
        return JSON_TYPE
    branch = branches[0]
    kind = branch.get("type")
    if kind is None:
        if "properties" in branch:
            kind = "object"
        elif "items" in branch:
            kind = "array"
        elif "format" in branch:
            kind = "string"
    if kind == "integer":
        return pa.int64()
    if kind == "number":
        return pa.float64()
    if kind == "boolean":
        return pa.bool_()
    if kind == "string":
        return _string_type(branch.get("format"))
    if kind == "array":
        items = branch.get("items")
        if not isinstance(items, dict) or depth >= MAX_STRUCT_DEPTH:
            return pa.list_(JSON_TYPE)
        return pa.list_(arrow_type(items, depth + 1))
    if kind == "object":
        props = branch.get("properties")
        open_shape = bool(branch.get("patternProperties")) or branch.get("additionalProperties") not in (None, False)
        if not isinstance(props, dict) or not props or open_shape or depth >= MAX_STRUCT_DEPTH:
            return JSON_TYPE
        return pa.struct([pa.field(str(name), arrow_type(sub, depth + 1)) for name, sub in props.items()])
    return JSON_TYPE


def stream_properties(schema: dict[str, Any]) -> dict[str, Any]:
    """Return the top-level properties of a stream schema (empty when it declares none)."""
    props = schema.get("properties")
    return props if isinstance(props, dict) else {}


def arrow_fields(schema: dict[str, Any], comments: dict[str, str] | None = None) -> list[pa.Field[Any]]:
    """One nullable Arrow field per top-level property of a stream schema."""
    fields: list[pa.Field[Any]] = []
    for name, sub in stream_properties(schema).items():
        comment = (comments or {}).get(name) or _describe(sub)
        meta = {b"comment": comment.encode()} if comment else None
        fields.append(pa.field(str(name), arrow_type(sub), nullable=True, metadata=meta))
    return fields


def _describe(sub: Any) -> str:
    if isinstance(sub, dict):
        for key in ("description", "title"):
            value = sub.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def duckdb_type_name(arrow: pa.DataType) -> str:
    """Return a DuckDB type name for an Arrow type produced by :func:`arrow_type`."""
    if arrow == JSON_TYPE:
        return "JSON"
    if pa.types.is_int64(arrow):
        return "BIGINT"
    if pa.types.is_float64(arrow):
        return "DOUBLE"
    if pa.types.is_boolean(arrow):
        return "BOOLEAN"
    if pa.types.is_string(arrow):
        return "VARCHAR"
    if pa.types.is_timestamp(arrow):
        return "TIMESTAMP WITH TIME ZONE"
    if pa.types.is_date32(arrow):
        return "DATE"
    if pa.types.is_time64(arrow):
        return "TIME"
    if pa.types.is_list(arrow):
        return f"{duckdb_type_name(arrow.value_type)}[]"
    if pa.types.is_struct(arrow):
        inner = ", ".join(f'"{f.name}" {duckdb_type_name(f.type)}' for f in arrow)
        return f"STRUCT({inner})"
    return "JSON"


# --------------------------------------------------------------- value coercion


class Coercer:
    """Convert decoded JSON values to Python values Arrow accepts for one type.

    Counts values that could not be converted (they become NULL).
    """

    def __init__(self) -> None:
        """Start with no rejected values."""
        self.rejected = 0

    def converter(self, arrow: pa.DataType) -> Callable[[Any], Any]:
        """Return a function mapping one JSON value to a Python value of ``arrow`` (or None)."""
        if arrow == JSON_TYPE:
            return _to_json
        if pa.types.is_int64(arrow):
            return self._guard(_to_int)
        if pa.types.is_float64(arrow):
            return self._guard(_to_float)
        if pa.types.is_boolean(arrow):
            return self._guard(_to_bool)
        if pa.types.is_string(arrow):
            return _to_str
        if pa.types.is_timestamp(arrow):
            return self._guard(_to_timestamp)
        if pa.types.is_date32(arrow):
            return self._guard(_to_date)
        if pa.types.is_time64(arrow):
            return self._guard(_to_time)
        if pa.types.is_list(arrow):
            item = self.converter(arrow.value_type)

            def to_list(v: Any) -> Any:
                if v is None:
                    return None
                if not isinstance(v, list):
                    self.rejected += 1
                    return None
                return [item(x) for x in v]

            return to_list
        if pa.types.is_struct(arrow):
            subs = [(f.name, self.converter(f.type)) for f in arrow]

            def to_struct(v: Any) -> Any:
                if v is None:
                    return None
                if not isinstance(v, dict):
                    self.rejected += 1
                    return None
                return {name: conv(v.get(name)) for name, conv in subs}

            return to_struct
        return _to_json

    def _guard(self, fn: Callable[[Any], Any]) -> Callable[[Any], Any]:
        def wrapped(v: Any) -> Any:
            if v is None:
                return None
            try:
                return fn(v)
            except TypeError, ValueError, OverflowError:
                self.rejected += 1
                return None

        return wrapped


def _to_json(v: Any) -> Any:
    return None if v is None else json.dumps(v, separators=(",", ":"), ensure_ascii=False)


def _to_str(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int | float):
        return str(v)
    return json.dumps(v, separators=(",", ":"), ensure_ascii=False)


def _to_int(v: Any) -> int:
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        if not -(2**63) <= v < 2**63:
            raise OverflowError(v)
        return v
    if isinstance(v, float):
        if not math.isfinite(v) or v != int(v):
            raise ValueError(v)
        return int(v)
    if isinstance(v, str) and v.strip():
        return _to_int(int(v.strip()))
    raise ValueError(v)


def _to_float(v: Any) -> float:
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, int | float):
        return float(v)
    if isinstance(v, str) and v.strip():
        return float(v)
    raise ValueError(v)


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, int) and v in (0, 1):
        return bool(v)
    if isinstance(v, str) and v.strip().lower() in ("true", "false", "1", "0", "t", "f", "yes", "no"):
        return v.strip().lower() in ("true", "1", "t", "yes")
    raise ValueError(v)


def _to_timestamp(v: Any) -> dt.datetime:
    if isinstance(v, int | float) and not isinstance(v, bool):
        return dt.datetime.fromtimestamp(v, tz=dt.UTC)
    if not isinstance(v, str):
        raise ValueError(v)
    text = v.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    value = dt.datetime.fromisoformat(text)
    if value.tzinfo is None:
        # Singer date-times without an offset are UTC by convention.
        value = value.replace(tzinfo=dt.UTC)
    return value.astimezone(dt.UTC)


def _to_date(v: Any) -> dt.date:
    if not isinstance(v, str):
        raise ValueError(v)
    text = v.strip()
    if len(text) > 10:
        # Some taps send a full date-time for a "date" property.
        return _to_timestamp(text).date()
    return dt.date.fromisoformat(text)


def _to_time(v: Any) -> dt.time:
    if not isinstance(v, str):
        raise ValueError(v)
    value = dt.time.fromisoformat(v.strip().removesuffix("Z"))
    return value.replace(tzinfo=None)


def records_to_batch(records: Iterable[dict[str, Any]], schema: pa.Schema, coercer: Coercer) -> pa.RecordBatch:
    """Build one record batch with ``schema``'s columns from decoded Singer records."""
    rows = list(records)
    arrays: list[pa.Array[Any]] = []
    for field in schema:
        conv = coercer.converter(field.type)
        values = [conv(row.get(field.name)) for row in rows]
        arrays.append(pa.array(values, type=field.type))
    return pa.RecordBatch.from_arrays(arrays, schema=schema)
