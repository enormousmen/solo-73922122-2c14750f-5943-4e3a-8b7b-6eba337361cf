# Edge stable release API

A FastAPI service that releases **one architecture at a time** into a
dual-architecture (linux/amd64 + linux/arm64) stable OCI index. Publishing an
amd64 fix never requires re-pushing arm64, and vice versa.

Image content is verified from a **local, read-only OCI image layout** — no
registry access, no network fetches.

When the amd64 and arm64 images share a resource-layer blob, concurrent
releases verify that layer with **one full disk scan per overlapping cohort**,
across uvicorn worker processes. See
[Shared resource layers](#shared-resource-layers) and
[`examples/shared-layers.md`](examples/shared-layers.md).

## Architecture

```
                 read-only mount                 writable volume
┌──────────────┐   ┌────────────────────┐   ┌──────────────────────┐
│ layout-init  │──▶│ /oci-layout        │   │ /state               │
│ (one-shot)   │   │  oci-layout        │   │  stable-index.json   │
└──────────────┘   │  index.json        │   │  stable-index.lock   │
                   │  blobs/sha256/…    │   └──────────┬───────────┘
                   └────────┬───────────┘              │
                            │ verify (chunked hashing) │ CAS commit
                   ┌────────▼───────────────────────────▼──────────┐
                   │ release-api (uvicorn, 2 workers)              │
                   │ GET  /v1/stable   POST /v1/releases           │
                   └───────────────────────────────────────────────┘
```

### Request flow for `POST /v1/releases`

1. Validate `platform` is `linux/amd64` or `linux/arm64` and both digests are
   well-formed `sha256:…`.
2. **Outside any lock**, verify the candidate manifest:
   - manifest blob exists and its bytes hash to the submitted digest,
     `schemaVersion` is 2, media type allowed;
   - config descriptor: media type, size, digest, and the config's own
     `os` / `architecture` must equal `linux` / the requested architecture;
   - every layer descriptor: media type, `size` (checked against the blob by
     `stat`, again while streaming, and on the final byte count) and digest,
     hashed in bounded chunks (`LAYER_CHUNK_BYTES`).
3. **Under an `flock` + compare-and-swap**, re-read the persisted index; if the
   target platform's digest differs from `expectedPreviousDigest` → `409` with
   the current digest, writing nothing. Otherwise replace only that platform's
   descriptor, keep the other exactly as it is, bump the revision, and persist.
4. Persist atomically (temp file → `fsync` → `rename` → directory `fsync`), so a
   reader or a crash only ever sees the whole old or whole new index.

Consequences:

* Layer scanning never holds the commit lock, so `GET /v1/stable` stays
  responsive and the other architecture can commit mid-scan.
* A layer digest is physically read/hashed at most once per concurrent cohort,
  even with two uvicorn workers; waiters reuse atomically published scan facts.
  Every request still validates its own manifest, config and descriptor
  size/digest against those facts. Results are **not** cached after the cohort
  ends, so a later release re-reads disk. A dead scan leader is detected via
  `flock` release and a waiter rescans (`app/scans.py`).
* Failed verification (422) or a conflict (409) leaves the persisted index
  untouched; after a restart the last successful index is served.
* Two uvicorn workers (or two containers) sharing `/state` serialise commits
  through the file lock and each read the latest committed state.

## Shared resource layers

The Compose layout is built with `build_layout.py --shared`: for each of v1,
v2 and v3 the amd64 and arm64 images share one identical resource-layer blob.
A dedicated demo fires the concurrent scenes and prints the real scan counts:

```console
docker compose up -d --build
docker compose exec release-api \
  python /srv/examples/demo_shared_layers.py
curl -s localhost:8000/v1/scans | jq .stats
```

Full API examples, measured two-worker output, the wrong-size scene and the
crash-takeover reproduction are in
[`examples/shared-layers.md`](examples/shared-layers.md). The older
[`examples/demo_concurrent.py`](examples/demo_concurrent.py) overlap/polling
demo still runs against the same layout.

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/v1/stable` | Full OCI index (`schemaVersion`, `mediaType`, both `manifests`). `ETag`/`304`. |
| `GET` | `/v1/stable/{arch}` | One platform descriptor (`amd64` or `arm64`). |
| `POST` | `/v1/releases` | Release one architecture. |
| `GET` | `/v1/scans` | Cross-worker layer scan counters, local active scans and on-disk sessions. |
| `GET` | `/v1/releases` | Per-architecture last publication + currently active scans + scan totals. |
| `GET` | `/healthz` | Readiness, revision, persistence status, in-flight scans. |
| `GET` | `/docs` | OpenAPI UI. |

## Run with Docker Compose

```console
docker compose up -d --build
curl -s localhost:8000/v1/stable | jq .
# shared-layer scan-count / wrong-size / late-rescan scenes:
docker compose exec release-api python /srv/examples/demo_shared_layers.py
# original slow-scan vs reads/other-architecture demo:
docker compose exec release-api python /srv/examples/demo_concurrent.py
```

`docker-compose.yml`:

* `layout-init` builds a deterministic **shared-layer** demo layout
  (`build_layout.py --shared`) into the `oci-layout` named volume: preloaded
  stable **v1** on both architectures, **v2** and **v3** candidate manifests,
  one byte-identical resource layer shared by amd64/arm64 per version, plus a
  wrong-layer-size manifest candidate. It writes `demo-digests.json` beside it.
* `release-api` mounts `oci-layout` **read-only** and `release-state`
  read-write. It runs two uvicorn workers to exercise the cross-process commit
  and scan-coalescing paths. `LAYER_CHUNK_DELAY_MS=2` is a demo-only pause per
  64 KiB chunk so the concurrency is visible on small demo layers; set it to
  `0` in production.

Reset the demo (returns to the v1 baseline):

```console
docker compose down -v && docker compose up -d
```

## Local run without Docker

Requires Python 3.11+.

```console
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt

# add --shared for the amd64/arm64 shared-resource-layer layout
python scripts/build_layout.py --out /tmp/oci-layout --meta /tmp/oci-layout/demo-digests.json

OCI_LAYOUT_DIR=/tmp/oci-layout STATE_DIR=/tmp/state \
  uvicorn app.main:create_app --factory --port 8000 --workers 2

# in another shell
python examples/demo_concurrent.py --meta /tmp/oci-layout/demo-digests.json
# or, with a --shared layout:
python examples/demo_shared_layers.py --meta /tmp/oci-layout/demo-digests.json
```

Tests (66 cases: validation, conflicts, durability, in-process and
cross-process concurrency, scan coalescing/crash takeover, endpoints):

```console
python -m pytest -q
```

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `OCI_LAYOUT_DIR` | `/oci-layout` | OCI image layout root (read-only). |
| `STATE_DIR` | `/state` | Writable directory for `stable-index.json`. |
| `OCI_INDEX_REF` | `registry.example.com/edge/stable:latest` | Reference recorded on the index. |
| `OCI_ARCHITECTURES` | `amd64,arm64` | Platforms the index must contain. |
| `LAYER_CHUNK_BYTES` | `65536` | Blob hashing chunk size (1 B – 16 MiB). |
| `LAYER_CHUNK_DELAY_MS` | `0` | Demo-only per-chunk pause (0–1000 ms). |
| `LAYER_SCAN_COALESCE` | `true` | Cross-process single-flight sharing of identical layer scans. |
| `LAYER_SCAN_POLL_MS` | `20` | Waiter poll interval for the leader result / lock release (1–60000 ms). |
| `LAYER_SCAN_WAIT_TIMEOUT_S` | `120` | Max wait for another process's layer scan before `503 scan_unavailable`. |

## Input layout expectations

`<layout>/oci-layout` must contain `imageLayoutVersion: 1.0.0`.
`<layout>/index.json` may either list platform descriptors directly, or hold a
single descriptor whose `mediaType` is an OCI/Docker **index** and whose blob is
the nested multi-arch index (the demo does the latter). The resolved index must:

* have `schemaVersion: 2` and an OCI/Docker index media type,
* contain exactly one `linux/<arch>` descriptor per configured architecture,
* reference manifest blobs that exist and whose configs declare the matching
  platform.

Anything else fails startup with `invalid_layout` rather than serving an index
the service cannot honour.

## What was exercised

All 66 pytest cases pass in this environment, including the original 54
(validation, conflicts, durability, in-process and cross-process commit
concurrency, endpoints) and 12 new cases in
`tests/test_shared_scans.py` (in-process and real-OS-process scan coalescing,
wrong-size isolation, late re-scan, independent layers, disable switch, and
`kill`-style leader crash takeover with a single CAS commit).

A live `uvicorn ... --workers 2` process was also run against a generated
`--shared` layout with the Compose-sized payloads and
`LAYER_CHUNK_DELAY_MS=2`. Both example scripts pass; in
`demo_shared_layers.py` the concurrent v3 requests were served by two
different worker PIDs yet physically scanned the shared layer once, and the
wrong-size scene returned `422 size_mismatch` for only that request while the
other architecture committed. A standalone `scripts/scan_worker.py`
orchestration reproduced leader exit code 9 followed by one `takeover`, one
`shared`, and exactly one committed revision.

Docker itself is not installed in this environment, so `docker compose up` was
not executed here; `docker-compose.yml` was validated by YAML parsing and the
exact image command/env were exercised via local two-worker uvicorn. See
[`examples/shared-layers.md`](examples/shared-layers.md) for the captured
output.

### Not done / possible follow-ups

* No Prometheus/OpenMetrics exposition; scan metrics are JSON at
  `GET /v1/scans` and in `layer-scan-stats.json`.
* Scan session artefacts are tiny JSON/lock files under
  `STATE_DIR/layer-scans/`; aged files for a layer are swept when a new cohort
  starts, but there is no separate global GC for layers never released again.
* Docker Compose was validated statically, not launched by the Docker CLI.
