"""Scalar ``json_schema_type(schema)``: the DuckDB type a JSON Schema maps to."""

from __future__ import annotations

import json
from typing import Annotated

import pyarrow as pa
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from .jsonschema import arrow_type, duckdb_type_name
from .meta import object_tags


def schema_type_names(values: list[str | None]) -> list[str | None]:
    """DuckDB type names for JSON Schema texts; NULL (or invalid JSON) gives NULL."""
    out: list[str | None] = []
    for text in values:
        if text is None:
            out.append(None)
            continue
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            out.append(None)
            continue
        out.append(duckdb_type_name(arrow_type(parsed)))
    return out


class JsonSchemaType(ScalarFunction):
    """``json_schema_type(schema)``: how this worker types a JSON Schema."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "json_schema_type"
        description = "The DuckDB type this worker maps a Singer JSON Schema to"
        tags = {
            **object_tags(
                title="JSON Schema To DuckDB Type",
                category="debugging",
                keywords="json schema, type mapping, duckdb type, singer schema, nullable, anyOf",
                doc_llm=(
                    "Given a JSON Schema as text (one property's schema from a Singer SCHEMA message or "
                    "catalog), return the DuckDB type read_stream gives that column: integer -> `BIGINT`, "
                    "number -> `DOUBLE`, boolean -> `BOOLEAN`, string -> `VARCHAR`, format date-time -> "
                    "`TIMESTAMP WITH TIME ZONE`, date -> `DATE`, time -> `TIME`, object with properties -> "
                    "`STRUCT(...)`, array -> `T[]`, and `JSON` for open or mixed shapes. 'null' in a type list "
                    "or anyOf only makes it nullable. NULL or invalid JSON gives NULL."
                ),
                doc_md=(
                    "## json_schema_type(schema)\n\n"
                    '`json_schema_type(\'{"type": ["null", "string"], "format": "date-time"}\')` '
                    "returns `TIMESTAMP WITH TIME ZONE`."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Nullable date-time and integer properties",
                        "sql": (
                            "SELECT singer.main.json_schema_type(s) AS t FROM (VALUES "
                            '(\'{"type": ["null", "string"], "format": "date-time"}\'), '
                            '(\'{"anyOf": [{"type": "integer"}, {"type": "null"}]}\')) v(s) ORDER BY t'
                        ),
                        "expected_result": [["BIGINT"], ["TIMESTAMP WITH TIME ZONE"]],
                    },
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT singer.main.json_schema_type("
                    '\'{"type": "object", "properties": {"id": {"type": "integer"}}}\') AS duckdb_type'
                ),
                description="An object with declared properties becomes a STRUCT",
            ),
        ]

    @classmethod
    def compute(
        cls,
        schema: Annotated[pa.StringArray, Param(doc="A JSON Schema as JSON text; NULL in, NULL out.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Map every schema in the column."""
        return pa.array(schema_type_names(schema.to_pylist()), type=pa.string())
