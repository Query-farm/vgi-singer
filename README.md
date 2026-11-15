<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Run Any Singer Tap and Query Its Streams in DuckDB

> **vgi-singer** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-singer/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-singer/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[Singer](https://hub.meltano.com/singer/spec) taps are command line extractors (about
600 of them are listed on [Meltano Hub](https://hub.meltano.com/extractors/)): GitHub,
Postgres, Salesforce, Stripe, CSV files and many more. This worker runs any of them from
DuckDB and returns their streams as typed tables.

- **Isolated installs.** Each tap is installed on first use into its own cached virtual
  environment from a pip requirement (`pip_url`), never into the worker's environment.
- **Configs from secrets only.** The tap's config JSON lives in a DuckDB secret of type
  `singer`; it is written to a private (0600) temporary file while the tap runs and
  deleted afterwards.
- **Real types.** JSON Schema becomes `BIGINT`, `DOUBLE`, `BOOLEAN`, `VARCHAR`,
  `TIMESTAMPTZ`, `DATE`, `TIME`, `STRUCT`, `LIST`, or `JSON` for open shapes.
- **Streaming and resumable.** Rows stream back in Arrow batches as the tap prints them.
  Singer STATE is the scan cursor, so a scan can continue in another worker process.
- **Incremental.** Start a sync from a Singer state and get the final state back.

## Quick start

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'singer' (TYPE vgi, LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-singer@v0.1.0 vgi-singer-worker');

-- The tap's config: exactly what the tap reads from --config.
CREATE SECRET csv (TYPE singer, SCOPE 'singer://tap-csv', CONFIG '{
  "files": [{"entity": "customers", "path": "/data/customers.csv", "keys": ["id"]}]}');

-- What does the tap offer?
SELECT stream, key_properties, columns
FROM singer.main.discover('tap-csv', pip_url := 'git+https://github.com/MeltanoLabs/tap-csv.git@v1.3.2');

-- Read a stream.
SELECT * FROM singer.main.read_stream('tap-csv', 'customers',
    pip_url := 'git+https://github.com/MeltanoLabs/tap-csv.git@v1.3.2');
```

From a source checkout: `make build`, then `LOCATION '/abs/path/to/.venv/bin/vgi-singer-worker'`.

## Functions

All in `singer.main`. Tap-running functions take the tap executable name first plus the
named arguments `pip_url`, `executable`, `python`, `secret_name` and `timeout_seconds`.

| Function | Returns |
|---|---|
| `discover(tap, ...)` | One row per stream: id, name, key properties, replication method/key, columns with DuckDB types, JSON Schema, metadata. |
| `catalog(tap, ...)` | The whole Singer catalog as `JSON` (one row). |
| `read_stream(tap, stream, state :=, catalog_json :=, ...)` | The stream as a typed table. |
| `final_state(tap, stream :=, state :=, ...)` | Runs a sync and returns the final STATE, record and state-message counts. |
| `messages(tap, stream :=, state :=, ...)` | Every raw message (SCHEMA, RECORD, STATE, ACTIVATE_VERSION, BATCH), one row each. |
| `read_messages(path)` | Raw messages from saved Singer JSONL output (file, glob or http(s) URL; plain or gzip). |
| `read_records(path, stream)` | One stream's records from saved output, typed from its SCHEMA message. |
| `json_schema_type(schema)` | Scalar: the DuckDB type a JSON Schema maps to. |

### Installing taps

| Argument | Meaning |
|---|---|
| `tap` | The tap's executable name, e.g. `tap-github`. Without `pip_url` it is also the package installed. |
| `pip_url` | A pip requirement: `tap-github==1.4.0`, `git+https://github.com/MeltanoLabs/tap-csv.git@v1.3.2`, a local path. |
| `python` | Python version for the tap's environment (`'3.11'`), for taps that do not support the default. |
| `executable` | Path to an already installed tap; nothing is installed. |

Environments live under `~/Library/Caches/vgi-singer` (macOS) or
`$XDG_CACHE_HOME/vgi-singer`; set `VGI_SINGER_CACHE_DIR` to move them. They are created
with `uv` when it is on `PATH`, otherwise with `python -m venv` and `pip`.

### Secrets

```sql
-- Bound to one tap by scope.
CREATE SECRET gh (TYPE singer, SCOPE 'singer://tap-github', CONFIG '{"access_token": "...", "repository": "duckdb/duckdb", "start_date": "2026-01-01"}');
-- Or chosen by name.
SELECT * FROM singer.main.read_stream('tap-github', 'issues', secret_name := 'gh');
```

The worker asks DuckDB for the `singer` secret whose scope best matches
`singer://<tap>`; a secret without a scope applies to every tap. Without any secret the
tap gets `{}`. The config never appears in scan state, logs or error messages, and
`duckdb_secrets()` shows it redacted.

### Attach a tap as a database

Catalog discovery needs the tap's config, which catalog listing cannot see, so you pass
a Singer catalog at ATTACH (a file path, or the JSON itself):

```sql
SET VARIABLE cat = (SELECT catalog FROM singer.main.catalog('tap-csv', pip_url := '...'));
ATTACH 'singer' AS csv (TYPE vgi, LOCATION '...', tap 'tap-csv', pip_url '...', catalog getvariable('cat'));
SELECT * FROM csv.customers;
```

Options: `tap`, `catalog`, `pip_url`, `executable`, `python`, `secret_name`,
`timeout_seconds`. Each stream becomes a table in `main`; a scan runs `read_stream` with
the attach's settings, so it uses the same secret lookup, pushdown and resume behaviour.

### Incremental syncs

```sql
SELECT final_state FROM singer.main.final_state('tap-github', stream := 'issues',
    state := '{"bookmarks": {"issues": {"since": "2026-11-01T00:00:00Z"}}}');
SELECT * FROM singer.main.read_stream('tap-github', 'issues', state := '<the state from last time>');
```

## What pushes down

- **Projection.** `read_stream` (and attached stream tables) marks only the queried
  columns `selected` in the catalog given to the tap, plus the stream's key and
  `inclusion: automatic` properties. Taps that honour property selection fetch less.
- **Stream selection.** Only the requested stream is selected.
- **LIMIT** stops reading early: once DuckDB stops pulling, the tap blocks on its pipe and
  is stopped after 10 idle minutes or when the worker exits.
- **Filters** do not push down: Singer has no filter protocol. Use `state :=` to start
  an incremental stream from a bookmark instead.

## Type mapping

| JSON Schema | DuckDB |
|---|---|
| `integer` | `BIGINT` |
| `number` (or integer + number) | `DOUBLE` |
| `boolean` | `BOOLEAN` |
| `string` | `VARCHAR` |
| `string`, `format: date-time` | `TIMESTAMP WITH TIME ZONE` (offset-less values are UTC) |
| `string`, `format: date` / `time` | `DATE` / `TIME` |
| `object` with `properties`, no extra keys | `STRUCT(...)`, up to 4 levels deep |
| `array` with `items` | `LIST` of the item type |
| open objects, several types, no type | `JSON` |

`null` in a `type` list or an `anyOf`/`oneOf` branch only makes the column nullable;
every column is nullable. A value that does not fit its column (text in an integer
column, an unparseable timestamp) becomes NULL and the count is logged to stderr. Keys
not declared in a STRUCT are dropped; use a `JSON` property to keep everything.
Properties with `inclusion: unsupported` are not columns.

## How scans run and resume

The tap runs as a subprocess. A reader thread moves its stdout into a bounded queue (the
tap blocks on its pipe rather than being buffered), and rows go to DuckDB in batches of
up to 1,000, or sooner when the tap is slow. stderr is kept in a small ring buffer and
quoted when the tap exits non-zero. A tap that prints nothing for `timeout_seconds`
(default 300; 0 disables) is stopped. BATCH messages with local `jsonl` manifests (plain
or gzip) are expanded into records.

The scan cursor holds the last STATE value and the number of rows read after it, never
the config. In one worker process the tap simply keeps running between batches. If a
batch lands on another process (HTTP deployments), the tap is restarted with
`--state <last STATE>` and the rows already returned after that state are skipped. This
relies on the tap replaying the same records from the same state, which is the Singer
contract; a tap that returns fewer records on resume fails the scan rather than silently
dropping rows.

## Testing

`make check` runs ruff, mypy, pydoclint, the unit tests, the SQL suite over stdio,
launch and HTTP transports, and the metadata lint.

- `test/fake_tap/tap-fake` is a hermetic tap (standard library only): discovery, every
  JSON Schema shape, an incremental stream with STATE, ACTIVATE_VERSION, a BATCH
  message, property selection, failures, hangs and an auth check.
- `test/sql/tap_csv.test` installs [MeltanoLabs tap-csv](https://github.com/MeltanoLabs/tap-csv)
  v1.3.2 through `pip_url` and reads local CSVs. It is skipped (an expected skip) when
  the tap cannot be installed, e.g. offline with a cold cache.
- The metadata lint runs with execution disabled (`vgi-lint.toml`): every tap-running
  example needs a third-party tap and its credentials.

## Support

Open an issue at https://github.com/Query-farm/vgi-singer/issues.

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Licensed under the BSD Zero Clause License (0BSD); see [LICENSE](LICENSE).
