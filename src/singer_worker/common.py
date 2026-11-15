"""Arguments, secrets and the resumable scan engine shared by the tap functions.

**Secrets.** A tap's config JSON comes from a DuckDB secret of type
``singer``::

    CREATE SECRET csv_tap (TYPE singer, CONFIG '{"files": [...]}', SCOPE 'singer://tap-csv');

The worker asks DuckDB for the ``singer`` secret whose scope best matches
``singer://<tap>``; a secret with no scope applies to every tap. Name one
explicitly with ``secret_name := 'csv_tap'``. Without any secret the tap gets
an empty config ``{}``.

**Resumable scans.** The scan cursor (``TapCursor``) holds the last Singer
STATE value the tap emitted and how many items were read after it, never the
config. In one worker process the live tap process simply continues between
batches. When a batch lands on another process (HTTP), or the process is gone,
the tap is restarted with ``--state <last STATE>`` and the items already
returned after that state are skipped, so the scan carries on where it left
off. This relies on the tap replaying the same records from the same state,
which is the Singer contract for incremental streams.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Arg
from vgi.catalog import SecretTypeSpec
from vgi_rpc import ArrowSerializableDataclass

from . import tap as tapmod
from .tap import TapError, TapRun, TapSpec

SECRET_TYPE = "singer"
SCOPE_PREFIX = "singer://"
BATCH_ROWS = 1_000
FLUSH_SECONDS = 2.0
DOC_URL = "https://github.com/Query-farm/vgi-singer#secrets"

SINGER_SECRET_TYPE = SecretTypeSpec(
    name=SECRET_TYPE,
    description="Config JSON for a Singer tap, written to a private temporary file while the tap runs.",
    schema=pa.schema(
        [
            pa.field(
                "config",
                pa.string(),
                metadata={
                    "redact": "true",
                    "vgi_required": "true",
                    "vgi_doc": (
                        "The tap's config as a JSON object, exactly what the tap reads from --config. "
                        "Bind it to one tap with SCOPE 'singer://<tap-name>'."
                    ),
                },
            )
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)


@dataclass(slots=True, frozen=True, kw_only=True)
class TapArgs:
    """Arguments every tap-running function takes."""

    tap: Annotated[
        str,
        Arg(0, doc="Tap executable name, e.g. 'tap-csv' or 'tap-github'.", pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$"),
    ]
    pip_url: Annotated[
        str,
        Arg(
            "pip_url",
            default="",
            doc="pip requirement to install the tap from, e.g. 'tap-github==1.4.0' or "
            "'git+https://github.com/MeltanoLabs/tap-csv.git'; empty installs the package named like the tap.",
        ),
    ]
    executable: Annotated[
        str,
        Arg("executable", default="", doc="Path to an already installed tap executable; skips installation."),
    ]
    python: Annotated[
        str, Arg("python", default="", doc="Python version for the tap's environment, e.g. '3.12'; empty = default.")
    ]
    secret_name: Annotated[
        str,
        Arg(
            "secret_name",
            default="",
            doc="Name of the singer secret holding the config; empty picks the one scoped to 'singer://<tap>'.",
        ),
    ]
    timeout_seconds: Annotated[
        int,
        Arg(
            "timeout_seconds",
            default=tapmod.DEFAULT_TIMEOUT_SECONDS,
            doc="Stop the tap if it prints nothing for this many seconds; 0 waits forever.",
            ge=0,
        ),
    ]

    def spec(self) -> TapSpec:
        """Return the tap to run."""
        return TapSpec(tap=self.tap, pip_url=self.pip_url, executable=self.executable, python=self.python)


def _config_value(secret: dict[str, Any] | None) -> str:
    if secret is None:
        return "{}"
    value = secret.get("config")
    text = value.as_py() if value is not None and hasattr(value, "as_py") else value
    if not text:
        return "{}"
    try:
        parsed = json.loads(str(text))
    except json.JSONDecodeError as exc:
        # Never echo the value: it is a credential.
        raise ValueError(f"the singer secret's CONFIG is not valid JSON ({exc.msg} at position {exc.pos})") from None
    if not isinstance(parsed, dict):
        raise ValueError("the singer secret's CONFIG must be a JSON object")
    return str(text)


def bind_config(secrets: Any, args: TapArgs) -> str | None:
    """Config JSON at bind time, or None while DuckDB still has to resolve the secret."""
    if args.secret_name:
        secret = secrets.get(SECRET_TYPE, name=args.secret_name)
        if secret is None and secrets.all_resolved:
            raise ValueError(f"no singer secret named {args.secret_name!r}")
    else:
        secret = secrets.get(SECRET_TYPE, scope=SCOPE_PREFIX + args.tap)
    if not secrets.all_resolved:
        return None
    return _config_value(secret)


def process_config(secrets: Any, args: TapArgs) -> str:
    """Config JSON while scanning (the secrets resolved at bind)."""
    if args.secret_name:
        return _config_value(secrets.lookup(SECRET_TYPE, name=args.secret_name))
    return _config_value(secrets.lookup(SECRET_TYPE, scope=SCOPE_PREFIX + args.tap))


def parse_json_arg(text: str, what: str) -> Any:
    """Decode a JSON argument, naming it in the error."""
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{what} is not valid JSON: {exc}") from exc


# ------------------------------------------------------------------ scan engine


@dataclass(kw_only=True)
class TapCursor(ArrowSerializableDataclass):
    """Externalized scan position: no secrets, only Singer state and counters."""

    token: str = ""
    singer_state: str = ""
    since_state: int = 0
    done: bool = False


# Turns one message into the items it contributes (records, or the message itself).
ItemFn = Callable[[dict[str, Any]], list[dict[str, Any]]]


def read_items(
    cursor: TapCursor,
    *,
    spec: TapSpec,
    config: str,
    catalog: dict[str, Any] | None,
    timeout_seconds: int,
    items_of: ItemFn,
    max_items: int = BATCH_ROWS,
) -> list[dict[str, Any]]:
    """Read the next items of a scan, resuming or restarting the tap as needed.

    Sets ``cursor.done`` once the tap has exited cleanly. Raises TapError when it fails.
    """
    if not cursor.token:
        cursor.token = uuid.uuid4().hex
    run = tapmod.take_run(cursor.token)
    if run is not None and (run.last_state != cursor.singer_state or run.since_state != cursor.since_state):
        tapmod.drop_run(cursor.token)
        run = None
    skip = 0
    if run is None:
        run = TapRun.start(spec, config, catalog, cursor.singer_state or None, timeout_seconds=timeout_seconds)
        run.last_state = cursor.singer_state
        skip = cursor.since_state
        run.since_state = 0
        tapmod.keep_run(cursor.token, run)
    items: list[dict[str, Any]] = []
    started = time.monotonic()
    try:
        while len(items) < max_items:
            wait = 1.0 if not items else max(0.05, FLUSH_SECONDS - (time.monotonic() - started))
            line = run.poll_line(wait)
            if line is None:
                cursor.done = True
                break
            if line is False:
                if items and time.monotonic() - started >= FLUSH_SECONDS:
                    break
                continue
            assert isinstance(line, str)
            msg = tapmod.parse_message(line)
            if msg is None:
                continue
            for item in items_of(msg):
                run.since_state += 1
                if skip:
                    skip -= 1
                else:
                    items.append(item)
            if msg["type"] == "STATE":
                run.last_state = json.dumps(msg.get("value"), separators=(",", ":"))
                run.since_state = 0
    except BaseException:
        tapmod.drop_run(cursor.token)
        raise
    if cursor.done:
        tapmod.drop_run(cursor.token)
        if skip:
            raise TapError(
                f"{spec.tap} produced fewer records when resumed from its state than before; "
                "the scan cannot continue consistently"
            )
    cursor.singer_state = run.last_state
    cursor.since_state = run.since_state
    return items


def full_catalog(catalog: dict[str, Any]) -> dict[str, Any]:
    """Return the catalog with every stream (and property) selected."""
    from .singer_catalog import select, streams

    return {"streams": [select(s, None)["streams"][0] for s in streams(catalog)]}
