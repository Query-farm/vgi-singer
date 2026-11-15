"""Discovery metadata: the one file to edit when you rename or extend the worker.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Keep every object documented and ``make lint`` stays at 100.

* Catalog-level tags (title, license, author, links, long description) live in
  ``CATALOG_TAGS`` below.
* Every function and table carries ``object_tags(...)``: a title, a category
  from ``SCHEMA_CATEGORIES``, search keywords, and LLM- and human-facing docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "singer"

SOURCE_URL = "https://github.com/Query-farm/vgi-singer"

# Categories group functions in docs and agent tools. Every object's
# ``vgi.category`` must name one of these.
SCHEMA_CATEGORIES = [
    {"name": "taps", "title": "Taps", "description": "Run Singer taps: discover streams and read them as tables."},
    {"name": "files", "title": "Singer Files", "description": "Read Singer JSONL output that is already on disk."},
    {"name": "debugging", "title": "Debugging", "description": "Inspect raw messages and the type mapping."},
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def result_columns(schema: pa.Schema) -> str:
    """``vgi.result_columns_schema`` JSON for a fixed output schema."""
    from .jsonschema import duckdb_type_name

    return json.dumps(
        [
            {
                "name": f.name,
                "type": duckdb_type_name(f.type),
                "description": (f.metadata or {}).get(b"comment", b"").decode(),
            }
            for f in schema
        ]
    )


CATALOG_COMMENT = (
    "Run any Singer tap (the ~600 extractors on Meltano Hub) from DuckDB and query its streams as typed "
    "tables, or read saved Singer JSONL output."
)

CATALOG_DOC_LLM = (
    "Runs Singer taps (command line extractors such as tap-github, tap-postgres or tap-csv; about 600 are "
    "listed on Meltano Hub) and returns their streams as DuckDB tables with real types. A tap is installed "
    "on first use into its own cached virtual environment from a pip requirement (`pip_url`), never into "
    "the worker. Its config JSON comes only from a DuckDB secret of type `singer`, bound to one tap with "
    "`SCOPE 'singer://<tap-name>'`; the worker writes it to a private temporary file while the tap runs. "
    "JSON Schema maps to `BIGINT`, `DOUBLE`, `BOOLEAN`, `VARCHAR`, `TIMESTAMPTZ`, `DATE`, `STRUCT`, `LIST` "
    "or `JSON`. Singer STATE messages are the scan cursor, so long scans resume, and incremental syncs start "
    "from a state you pass in. Saved Singer JSONL output can be read without running a tap. ATTACH with "
    "`tap` and `catalog` options exposes every stream of a tap as a table."
)

CATALOG_DOC_MD = (
    "# Singer taps in DuckDB\n\n"
    "Run any [Singer](https://hub.meltano.com/singer/spec) tap and query its streams with SQL.\n\n"
    "Taps are installed into isolated, cached environments; their configs come from DuckDB secrets; "
    "JSON Schema becomes real DuckDB types; and Singer STATE drives resumable, incremental scans.\n\n"
    "Attach a tap together with its Singer catalog to see every stream as a table, or read saved "
    "Singer JSONL output directly."
)

SCHEMA_COMMENT = "Functions that run Singer taps and read Singer output; attached tap streams appear here as tables."

SCHEMA_DOC_LLM = (
    "`main` holds the worker's functions and, when the catalog is attached with `tap` and `catalog` options, "
    "one table per stream of that tap. Functions that run a tap take the tap executable name first and the "
    "named arguments `pip_url`, `executable`, `python`, `secret_name` and `timeout_seconds`; the ones that "
    "sync also take `state` (Singer state JSON) and `catalog_json` (skip discovery). File readers take a "
    "local path, glob or http(s) URL of Singer JSONL output and need no tap or secret."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Functions that discover and sync Singer taps, inspect their raw messages and read saved output.\n\n"
    "When a tap is attached with its catalog, each stream is also a table here, and scanning it runs the tap."
)

DYNAMIC_COLUMNS_MD = (
    "One column per top-level property of the stream's JSON Schema, in schema order, all nullable. "
    "The type depends on the property's schema:\n\n"
    "| Name | Type | Description |\n"
    "| --- | --- | --- |\n"
    "| integer property | BIGINT | `type: integer` |\n"
    "| number property | DOUBLE | `type: number` (or integer and number) |\n"
    "| boolean property | BOOLEAN | `type: boolean` |\n"
    "| string property | VARCHAR | `type: string` without a known format |\n"
    "| date-time property | TIMESTAMP WITH TIME ZONE | `format: date-time`; offset-less values are UTC |\n"
    "| date property | DATE | `format: date` |\n"
    "| time property | TIME | `format: time` |\n"
    "| object property | STRUCT(city VARCHAR, zip VARCHAR) | "
    "e.g. `properties` city and zip declared, no extra keys allowed, up to 4 levels deep |\n"
    "| array property | VARCHAR[] | e.g. `items: {type: string}`; items are mapped by the same rules |\n"
    "| open property | JSON | open objects, mixed types, or no type |\n"
)

FIXTURE_URL = "https://raw.githubusercontent.com/Query-farm/vgi-singer/v0.1.0/test/fixtures/fake-output.jsonl"

AGENT_TEST_TASKS = [
    {
        "name": "saved_output_message_types",
        "prompt": (f"Saved Singer tap output is at {FIXTURE_URL}. How many messages of each type does it contain?"),
    },
    {
        "name": "saved_output_events",
        "prompt": f"In the Singer output at {FIXTURE_URL}, how many records does the events stream have?",
    },
    {
        "name": "schema_type_of_datetime",
        "prompt": (
            "Using the singer worker, which DuckDB type does a Singer property with JSON Schema "
            '{"type": ["null", "string"], "format": "date-time"} become?'
        ),
    },
]

CATALOG_TAGS = {
    "vgi.title": "Singer Taps",
    "vgi.subtitle": "Run any Singer tap and query its streams as typed DuckDB tables.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array("singer, meltano, tap, elt, extract, connectors, jsonl, json schema, state"),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    # Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
