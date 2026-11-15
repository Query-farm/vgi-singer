"""Attach a Singer tap as a database: one table per stream.

::

    ATTACH 'singer' AS csv (TYPE vgi, LOCATION '/path/to/vgi-singer-worker',
                            tap 'tap-csv', pip_url 'git+https://github.com/MeltanoLabs/tap-csv.git',
                            catalog '/path/to/catalog.json');
    SELECT * FROM csv.customers;

Catalog hooks never see DuckDB secrets, and most taps need their config to
discover, so the stream list comes from a Singer catalog given at ATTACH: a
file path, or the JSON text itself (``catalog getvariable('cat')`` after
``SET VARIABLE cat = (SELECT catalog FROM singer.main.catalog('tap-csv'))``).
Every scan is redirected to ``read_stream`` with the attach's tap settings and
that stream's catalog entry, so the scan resolves the ``singer`` secret,
prunes columns and resumes exactly like a direct ``read_stream`` call.

Without a ``tap`` the catalog holds only the functions.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from vgi.catalog import (
    AttachOpaqueData,
    CatalogAttachResult,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionOpaqueData,
)

from . import singer_catalog as sc
from .tap import DEFAULT_TIMEOUT_SECONDS, TapSpec

if TYPE_CHECKING:
    from vgi import SchemaPath
    from vgi.catalog.catalog_interface import FunctionInfo, IndexInfo, MacroInfo, ViewInfo
    from vgi_rpc.rpc import CallContext

MAIN = ["main"]
_ATTACH_PREFIX = b"vgi-singer-tap:"


@dataclass(frozen=True, slots=True)
class TapAttach:
    """The tap settings of one ATTACH (no credentials: those stay in the secret)."""

    tap: str
    pip_url: str
    executable: str
    python: str
    secret_name: str
    timeout_seconds: int
    catalog: dict[str, Any]


def _load_catalog_option(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if not text.startswith("{"):
        path = Path(text).expanduser()
        if not path.is_file():
            raise ValueError(f"catalog {raw!r} is neither catalog JSON nor an existing file")
        text = path.read_text()
    return sc.load_catalog(text)


def tap_attach(options: Mapping[str, Any]) -> TapAttach | None:
    """Read and validate the tap options; None when no ``tap`` was given."""
    tap = str(options.get("tap") or "").strip()
    if not tap:
        return None
    raw_catalog = str(options.get("catalog") or "").strip()
    if not raw_catalog:
        raise ValueError(
            "ATTACH with a tap needs a catalog: a Singer catalog file path or its JSON, e.g. "
            "catalog getvariable('cat') after SET VARIABLE cat = (SELECT catalog FROM singer.main.catalog('<tap>'))"
        )
    timeout = int(options.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS)
    if timeout < 0:
        raise ValueError(f"timeout_seconds must be 0 or more, got {timeout}")
    attach = TapAttach(
        tap=tap,
        pip_url=str(options.get("pip_url") or ""),
        executable=str(options.get("executable") or ""),
        python=str(options.get("python") or ""),
        secret_name=str(options.get("secret_name") or ""),
        timeout_seconds=timeout,
        catalog=_load_catalog_option(raw_catalog),
    )
    TapSpec(tap=attach.tap, executable=attach.executable)  # validates the tap name
    return attach


def _table_comment(stream: dict[str, Any], attach: TapAttach) -> str:
    info = sc.stream_info(stream)
    parts = [f"Stream `{sc.stream_id(stream)}` of the Singer tap {attach.tap}."]
    if info["key_properties"]:
        parts.append(f"Key: {', '.join(info['key_properties'])}.")
    if info["replication_method"]:
        parts.append(f"Replication: {info['replication_method']}.")
    return " ".join(parts)


def _table_info(name: str, stream: dict[str, Any], attach: TapAttach) -> TableInfo:
    schema = sc.stream_schema(stream)
    info = sc.stream_info(stream)
    return TableInfo(
        name=name,
        schema_path=list(MAIN),
        columns=SerializedSchema(schema.serialize().to_pybytes()),
        constraints=[],
        comment=_table_comment(stream, attach),
        tags={
            "vgi.title": f"Stream {sc.stream_name(stream)}",
            "singer.tap": attach.tap,
            "singer.tap_stream_id": sc.stream_id(stream),
            "singer.key_properties": json.dumps(info["key_properties"]),
            "singer.replication_method": str(info["replication_method"] or ""),
        },
    )


class TapCatalog(ReadOnlyCatalogInterface):
    """The ``singer`` catalog: functions, plus one table per stream when a tap is attached."""

    # Tables depend on the ATTACH options, so the worker must not share one
    # catalog_contents snapshot between attaches.
    catalog_contents_attach_independent = False

    def loggable_attach_options(self, options: Mapping[str, Any]) -> Mapping[str, Any]:
        """Log the tap settings (the catalog JSON is long, and no credential is an attach option)."""
        keys = ("tap", "pip_url", "executable", "python", "secret_name", "timeout_seconds")
        return {k: options[k] for k in keys if k in options}

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        client_capabilities: Any = None,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        """Attach, failing early when the catalog option is unusable."""
        attach = tap_attach(options)
        if attach is not None and not sc.streams(attach.catalog):
            raise ValueError("the catalog lists no streams, so there are no tables to attach")
        return super().catalog_attach(
            name=name,
            options=options,
            data_version_spec=data_version_spec,
            implementation_version=implementation_version,
            client_capabilities=client_capabilities,
            ctx=ctx,
        )

    def attach_opaque_data_for(self, *, name: str, options: dict[str, Any]) -> AttachOpaqueData:
        """Carry the tap settings and catalog to every hook of this attach (no secrets in them)."""
        attach = tap_attach(options)
        if attach is None:
            return super().attach_opaque_data_for(name=name, options=options)
        payload = {"v": 1, **dataclasses.asdict(attach)}
        return AttachOpaqueData(_ATTACH_PREFIX + json.dumps(payload, separators=(",", ":")).encode())

    @staticmethod
    def _attach(attach_opaque_data: AttachOpaqueData) -> TapAttach | None:
        raw = bytes(attach_opaque_data)
        if not raw.startswith(_ATTACH_PREFIX):
            return None
        d = json.loads(raw[len(_ATTACH_PREFIX) :])
        d.pop("v", None)
        return TapAttach(**d)

    def _tables(self, attach_opaque_data: AttachOpaqueData) -> list[tuple[TableInfo, dict[str, Any]]]:
        attach = self._attach(attach_opaque_data)
        if attach is None:
            return []
        return [(_table_info(n, s, attach), s) for n, s in sc.table_names(attach.catalog).items()]

    def _with_table_count(self, info: SchemaInfo, attach_opaque_data: AttachOpaqueData) -> SchemaInfo:
        """Count the stream tables in ``main`` (a declared count of 0 would hide them)."""
        if [p.lower() for p in info.path] != MAIN:
            return info
        counts = dict(info.estimated_object_count or {})
        counts["table"] = len(self._tables(attach_opaque_data))
        return dataclasses.replace(info, estimated_object_count=counts)

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None
    ) -> list[SchemaInfo]:
        """List the schemas, with ``main`` counting the stream tables."""
        return [
            self._with_table_count(info, attach_opaque_data)
            for info in super().schemas(
                attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data
            )
        ]

    def schema_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: SchemaPath,
    ) -> SchemaInfo | None:
        """Look up a schema, with ``main`` counting the stream tables."""
        info = super().schema_get(
            attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data, path=path
        )
        return None if info is None else self._with_table_count(info, attach_opaque_data)

    def schema_contents(  # type: ignore[override]
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: SchemaPath,
        type: SchemaObjectType,  # noqa: A002 - SDK keyword
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo]:
        """List the functions, plus one table per stream in ``main``."""
        base = list(
            super().schema_contents(  # type: ignore[call-overload]
                attach_opaque_data=attach_opaque_data,
                transaction_opaque_data=transaction_opaque_data,
                path=path,
                type=type,
            )
        )
        kind = type if isinstance(type, SchemaObjectType) else SchemaObjectType(type)
        if kind == SchemaObjectType.TABLE and [p.lower() for p in path] == MAIN:
            base.extend(info for info, _ in self._tables(attach_opaque_data))
        return base

    def _find(self, attach_opaque_data: AttachOpaqueData, schema_path: SchemaPath, name: str) -> Any:
        if [p.lower() for p in schema_path] != MAIN:
            return None
        for info, stream in self._tables(attach_opaque_data):
            if info.name.lower() == name.lower():
                return info, stream
        return None

    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: SchemaPath,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Look up a stream table by name (case-insensitive)."""
        if at_unit:
            raise ValueError("Singer streams do not support time travel (AT ...)")
        hit = self._find(attach_opaque_data, schema_path, name)
        return None if hit is None else hit[0]

    def table_scan_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: SchemaPath,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        """Redirect a stream table scan to ``read_stream`` with the attach's tap settings."""
        attach = self._attach(attach_opaque_data)
        hit = self._find(attach_opaque_data, schema_path, name)
        if attach is None or hit is None:
            raise ValueError(f"no stream table named {name!r} in this tap")
        _, stream = hit
        named: dict[str, pa.Scalar[Any]] = {
            "catalog_json": pa.scalar(json.dumps({"streams": [stream]})),
            "timeout_seconds": pa.scalar(attach.timeout_seconds, pa.int64()),
        }
        for key in ("pip_url", "executable", "python", "secret_name"):
            value = getattr(attach, key)
            if value:
                named[key] = pa.scalar(value)
        return ScanFunctionResult(
            function_name="read_stream",
            positional_arguments=[pa.scalar(attach.tap), pa.scalar(sc.stream_id(stream))],
            named_arguments=named,
            required_extensions=[],
            schema_path=list(MAIN),
        )
