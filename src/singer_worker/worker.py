"""Assemble the functions into a catalog and run the worker.

To add a function: write it, then add the class to ``FUNCTIONS`` below.

Run it:

    vgi-singer-worker                      # stdio: what DuckDB launches by default
    vgi-singer-worker --http --port 8000   # HTTP: for Docker / hosted deploys
    vgi-singer-worker --help

Attach it from DuckDB:

    ATTACH 'singer' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-singer-worker');
    ATTACH 'singer' AS csv (TYPE vgi, LOCATION '...', tap 'tap-csv', catalog '/path/catalog.json');
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from vgi import Worker
from vgi.catalog import Catalog, Schema
from vgi.catalog.attach_option import AttachOption

from .catalog import TapCatalog
from .common import SINGER_SECRET_TYPE
from .discover import CatalogJson, Discover
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_COMMENT,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .raw import Messages, ReadMessages, ReadRecords
from .read_stream import FinalState, ReadStream
from .schema_type import JsonSchemaType
from .tap import DEFAULT_TIMEOUT_SECONDS

FUNCTIONS: list[type[Any]] = [
    Discover,
    CatalogJson,
    ReadStream,
    FinalState,
    Messages,
    ReadMessages,
    ReadRecords,
    JsonSchemaType,
]


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries`` (with descriptions).

    Overloads share one SQL name, so their examples are merged per name.
    Returns every example, for the schema-level list.
    """
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entry for entries in by_name.values() for entry in entries]


_ALL_EXAMPLES = _stamp_example_queries(FUNCTIONS)


class SingerCatalog(TapCatalog):
    """The ``singer`` catalog: the functions, plus one table per stream of an attached tap."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment=SCHEMA_COMMENT,
                functions=FUNCTIONS,
                tags={
                    "vgi.title": "Singer Tap Functions",
                    "domain": "data integration",
                    "vgi.keywords": keywords_array("singer, tap, meltano, streams, discover, state, jsonl"),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_ALL_EXAMPLES),
                },
            )
        ],
    )


class SingerWorker(Worker):
    """The worker process: the ``singer`` catalog, its secret type and ATTACH options."""

    catalog_interface = SingerCatalog
    catalog_name = CATALOG_NAME
    secret_types = [SINGER_SECRET_TYPE]

    class AttachOptions:
        """Options for ``ATTACH 'singer' AS name (TYPE vgi, LOCATION ..., tap '...', catalog '...')``."""

        tap: Annotated[
            str,
            AttachOption(desc="Tap executable name, e.g. 'tap-csv'. With catalog, every stream becomes a table."),
        ] = ""
        catalog: Annotated[
            str,
            AttachOption(desc="Singer catalog for the tap: a file path or the catalog JSON itself."),
        ] = ""
        pip_url: Annotated[
            str,
            AttachOption(desc="pip requirement the tap is installed from into an isolated cached environment."),
        ] = ""
        executable: Annotated[
            str, AttachOption(desc="Path to an already installed tap executable; skips installation.")
        ] = ""
        python: Annotated[str, AttachOption(desc="Python version for the tap's environment, e.g. '3.12'.")] = ""
        secret_name: Annotated[
            str,
            AttachOption(desc="Name of the singer secret with the tap config; empty uses the 'singer://<tap>' scope."),
        ] = ""
        timeout_seconds: Annotated[
            int, AttachOption(desc="Stop a tap that prints nothing for this many seconds; 0 waits forever.")
        ] = DEFAULT_TIMEOUT_SECONDS


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    SingerWorker.main()


if __name__ == "__main__":
    main()
