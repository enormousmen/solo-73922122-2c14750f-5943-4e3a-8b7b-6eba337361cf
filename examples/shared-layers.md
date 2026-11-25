# Shared resource layers: scan-once across workers

This walkthrough uses the `--shared` demo layout that Docker Compose builds by
default. In it, the amd64 and arm64 images for each candidate version share the
same byte-identical **resource layer** (layer 0), plus one architecture
specific layer.

The key idea:

* manifest, config and every layer **descriptor** are still checked
  independently by every request;
* the expensive full chunked read/hash of a **layer blob** is single-flighted
  per layer digest across threads, uvicorn workers and plain processes using
  `flock` plus a session/result directory under `STATE_DIR/layer-scans/`;
* only requests that overlap one live scan cohort share its result. Once the
  scan has finished, the next request starts a fresh cohort and reads disk
  again (no persistent result cache);
* if the elected scanner dies mid-read, its lock is released by the kernel and
  a waiter takes over with a full rescan;
* layers have independent locks, so a release that only uses other layers
  never waits.

## Run it

```console
docker compose up -d --build
docker compose exec release-api \
  python /srv/examples/demo_shared_layers.py
```

Locally (no Docker needed):

```console
python scripts/build_layout.py --shared \
  --out /tmp/shared-layout --meta /tmp/shared-layout/demo-digests.json

OCI_LAYOUT_DIR=/tmp/shared-layout STATE_DIR=/tmp/shared-state \
LAYER_CHUNK_BYTES=65536 LAYER_CHUNK_DELAY_MS=2 \
  uvicorn app.main:create_app --factory --port 8000 --workers 2

python examples/demo_shared_layers.py \
  --base http://127.0.0.1:8000 \
  --meta /tmp/shared-layout/demo-digests.json
```

## Observe the counters

`GET /v1/scans` reports process-wide and cross-worker counters (the
authoritative totals live in `STATE_DIR/layer-scan-stats.json`):

```console
$ curl -s localhost:8000/v1/scans | jq .
{
  "pid": 651,
  "coalescingEnabled": true,
  "pollMs": 20.0,
  "waitTimeoutS": 120.0,
  "statsFile": "/state/layer-scan-stats.json",
  "stats": {
    "scans": 0,          # completed physical full-layer scans
    "shared": 0,         # callers served from a concurrent leader's scan
    "takeovers": 0,      # scans re-run after a leader died
    "scanBytes": 0,
    "layers": {}         # per-digest scans/shared/takeovers/chunks/bytes
  },
  "activeLocal": [],
  "sessions": []
}
```

Each successful release response also reports the role this worker played for
each layer: `leader` (ran the scan), `shared` (reused a concurrent scan),
`takeover` (rescanned after a dead leader) or `direct` (coalescing disabled).

```json
"verified": {
  "layers": 2,
  "chunks": 195,
  "layersScannedHere": 1,
  "layersSharedHere": 1,
  "layerScans": [
    {"digest": "sha256:732efc00…", "role": "shared",   "chunks": 64, "bytes": 4194304},
    {"digest": "sha256:a9965ac3…", "role": "leader",   "chunks": 129, "bytes": 8388…}
  ]
}
```

## Measured output (two uvicorn workers)

The following is the real output of `demo_shared_layers.py` against
`uvicorn --workers 2` with the Compose-sized layout and
`LAYER_CHUNK_DELAY_MS=2`. The two scene-3 requests were served by
**different worker PIDs (650 and 651)** yet the shared v3 layer was physically
scanned exactly once:

```text
1) concurrent wrong-size amd64 manifest + good arm64 v2
   bad manifest declares shared layer size 4195855, actual 4195848
   bad-amd64 : HTTP 422 in 0.18s pid=650 code=size_mismatch
   good-arm64: HTTP 200 in 0.18s pid=651 roles[890e1773510d=leader, 33aceaf67a85=leader]
   physical scans of shared v2 layer: 1 (want 1)
   stable: amd64 still v1, arm64 moved to v2

2) good amd64 v2 after the shared scan ended (must re-read disk)
   amd64 v2: HTTP 200 in 0.46s roles[890e1773510d=leader, 509bb0ded66c=leader]
   physical scans of shared v2 layer: 1 (want 1, fresh cohort)

3) concurrent amd64 v3 + arm64 v3
   amd64-v3: HTTP 200 in 0.48s pid=650 roles[732efc004df8=shared, a9965ac32d51=leader]
   arm64-v3: HTTP 200 in 0.17s pid=651 roles[732efc004df8=leader, 91a1418452f1=leader]
   physical scans of shared v3 layer: 1 (want 1)

4) late duplicate amd64 v3 (re-reads layers, then CAS conflict)
   duplicate: HTTP 409 code=conflict
   physical scans of shared v3 layer: 1 (want 1, then 409)

final /v1/scans totals (all uvicorn workers):
   scans=9 shared=2 takeovers=0 scanBytes=41974611
   shared layer counters:
     v2 890e1773510d: scans=2 shared=1 bytes=8391696
     v3 732efc004df8: scans=2 shared=1 bytes=8391688
```

Accounting for `scans=9`: scene 1 scans the shared v2 layer and the arm64
layer (2); scene 2 rescans shared v2 plus the amd64 layer (2); scene 3 scans
shared v3 once plus both arch layers (3); scene 4 rescans shared v3 and the
amd64 layer before its 409 (2).

## Wrong layer size rejects one request

`demo-digests.json` contains `mutations.amd64V2WrongSharedLayerSize`: a clone
of the amd64 v2 manifest whose shared-layer `size` was changed by +7 while
the digest is unchanged. Run it at the same time as the good arm64 release:

```console
$ curl -s -o /tmp/bad.json -w '%{http_code}\n' -X POST localhost:8000/v1/releases \
    -H 'content-type: application/json' \
    -d "{\"platform\":\"linux/amd64\",
         \"manifestDigest\":\"$(jq -r .mutations.amd64V2WrongSharedLayerSize.digest /oci-layout/demo-digests.json)\",
         \"expectedPreviousDigest\":\"$(jq -r .mutations.amd64V2WrongSharedLayerSize.expectedPreviousDigest /oci-layout/demo-digests.json)\"}"
422
$ jq .error.code /tmp/bad.json
"size_mismatch"
```

The shared blob is still scanned only once for the concurrent cohort; the good
arm64 release compares the same facts against its own correct descriptor and
commits. The wrong amd64 request writes nothing.

## Crash mid-scan and takeover

`scripts/scan_worker.py` drives the same code path as the API in separate OS
processes. `--mode crash-leader` becomes the layer leader, sleeps inside the
physical read, then exits with `os._exit(9)` (equivalent to `SIGKILL`). Two
waiter processes then take over/share and commit through the normal CAS:

```text
leader exit code: 9 (9 = crashed as designed)
waiter: {'status': 409, 'roles': ['shared', 'shared'],   'commit': 'conflict'}
waiter: {'status': 200, 'roles': ['takeover', 'leader'], 'commit': 'ok', 'revision': 1}
shared layer stats: {"scans": 1, "shared": 1, "takeovers": 1, ...}
```

Exactly one waiter rescans (`takeover`); the other shares that rescan; only
the CAS winner moves `stable`. This is covered continuously by
`tests/test_shared_scans.py`, including a three-process one-scan assertion and
the kill/takeover/commit scenario.

## Reset

```console
docker compose down -v && docker compose up -d --build
```
