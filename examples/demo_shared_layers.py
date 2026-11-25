#!/usr/bin/env python3
"""Shared-layer scan coalescing demo over HTTP.

Run against the Docker Compose stack:

    docker compose exec release-api python /srv/examples/demo_shared_layers.py

or locally:

    python examples/demo_shared_layers.py \
        --meta /tmp/shared-layout/demo-digests.json

The layout must be the ``--shared`` layout produced by
``scripts/build_layout.py --shared``: amd64 and arm64 v2/v3 images share one
identical resource layer.  The script prints the *actual* cross-process scan
counters exposed by ``GET /v1/scans`` around four scenes:

  1. a wrong-size amd64 manifest and a good arm64 release verify the shared
     layer concurrently: exactly one physical scan; only the wrong request is
     rejected (422 size_mismatch);
  2. the later good amd64 release starts a new cohort and re-reads the layer;
  3. concurrent amd64+arm64 v3 releases scan the v3 shared layer once;
  4. a late duplicate re-verifies (re-reads disk) and then conflicts (409).
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_META = Path("/oci-layout/demo-digests.json")


def call(base: str, method: str, path: str, body: dict[str, Any] | None = None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"content-type": "application/json"},
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = json.loads(response.read() or b"null")
            status = response.status
    except urllib.error.HTTPError as exc:
        payload = json.loads(exc.read() or b"null")
        status = exc.code
    return status, payload, time.monotonic() - started


def by_arch(index: dict[str, Any]) -> dict[str, str]:
    return {
        manifest["platform"]["architecture"]: manifest["digest"]
        for manifest in index["manifests"]
    }


def release_body(meta: dict[str, Any], version: str, arch: str, previous: str) -> dict:
    return {
        "platform": f"linux/{arch}",
        "manifestDigest": meta["versions"][version][arch]["digest"],
        "expectedPreviousDigest": meta["versions"][previous][arch]["digest"],
    }


def layer_scan_count(base: str, digest: str) -> int:
    _status, payload, _ = call(base, "GET", "/v1/scans")
    return int(payload.get("stats", {}).get("layers", {}).get(digest, {}).get("scans", 0))


def short(digest: str) -> str:
    return digest.split(":", 1)[1][:12]


def print_roles(payload: dict[str, Any]) -> str:
    verified = payload.get("released", {}).get("verified", {})
    parts = []
    for item in verified.get("layerScans", []):
        parts.append(f"{short(item['digest'])}={item['role']}")
    return ", ".join(parts)


def concurrent_posts(base: str, jobs: dict[str, dict[str, Any]]):
    barrier = threading.Barrier(len(jobs))
    results: dict[str, tuple[int, Any, float]] = {}

    def worker(name: str, body: dict[str, Any]) -> None:
        barrier.wait()
        results[name] = call(base, "POST", "/v1/releases", body)

    threads = [threading.Thread(target=worker, args=(name, body)) for name, body in jobs.items()]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:8000")
    parser.add_argument("--meta", type=Path, default=DEFAULT_META)
    args = parser.parse_args()

    meta = json.loads(args.meta.read_text())
    if not meta.get("sharedLayerLayout"):
        print(
            "warning: metadata does not look like a --shared layout; "
            "scan-count assertions may fail"
        )

    status, stable, _ = call(args.base, "GET", "/v1/stable")
    assert status == 200, (status, stable)
    expected_baseline = {
        arch: meta["versions"]["v1"][arch]["digest"] for arch in ("amd64", "arm64")
    }
    if by_arch(stable) != expected_baseline:
        print(
            "stable is not at the v1 baseline; reset first with\n"
            "  docker compose down -v && docker compose up -d --build\n"
            "or use a fresh STATE_DIR for a local run."
        )
        return 2

    shared_v2 = meta["sharedLayers"]["v2"]["digest"]
    shared_v3 = meta["sharedLayers"]["v3"]["digest"]
    mutation = meta["mutations"]["amd64V2WrongSharedLayerSize"]

    print("baseline stable:")
    for arch, digest in by_arch(stable).items():
        print(f"  {arch}: {digest}")
    print(f"shared v2 layer: {shared_v2}")
    print(f"shared v3 layer: {shared_v3}")

    # -- Scene 1: wrong-size amd64 + good arm64 scan together --------------
    print("\n1) concurrent wrong-size amd64 manifest + good arm64 v2")
    print(
        f"   bad manifest declares shared layer size {mutation['wrongSize']}, "
        f"actual {mutation['actualSize']}"
    )
    before = layer_scan_count(args.base, shared_v2)
    results = concurrent_posts(
        args.base,
        {
            "bad-amd64": {
                "platform": mutation["platform"],
                "manifestDigest": mutation["digest"],
                "expectedPreviousDigest": mutation["expectedPreviousDigest"],
            },
            "good-arm64": release_body(meta, "v2", "arm64", "v1"),
        },
    )
    after = layer_scan_count(args.base, shared_v2)
    bad_status, bad_payload, bad_latency = results["bad-amd64"]
    arm_status, arm_payload, arm_latency = results["good-arm64"]
    print(f"   bad-amd64 : HTTP {bad_status} in {bad_latency:.2f}s "
          f"pid={bad_payload.get('workerPid')} "
          f"code={bad_payload.get('error', {}).get('code')}")
    print(f"   good-arm64: HTTP {arm_status} in {arm_latency:.2f}s "
          f"pid={arm_payload.get('workerPid')} "
          f"roles[{print_roles(arm_payload)}]")
    print(f"   physical scans of shared v2 layer: {after - before} (want 1)")
    assert bad_status == 422, bad_payload
    assert bad_payload["error"]["code"] == "size_mismatch", bad_payload
    assert arm_status == 200, arm_payload
    assert after - before == 1, (before, after)

    stable = call(args.base, "GET", "/v1/stable")[1]
    assert by_arch(stable)["amd64"] == meta["versions"]["v1"]["amd64"]["digest"]
    assert by_arch(stable)["arm64"] == meta["versions"]["v2"]["arm64"]["digest"]
    print("   stable: amd64 still v1, arm64 moved to v2")

    # -- Scene 2: late good amd64 re-reads disk -----------------------------
    print("\n2) good amd64 v2 after the shared scan ended (must re-read disk)")
    before = layer_scan_count(args.base, shared_v2)
    amd_status, amd_payload, amd_latency = call(
        args.base, "POST", "/v1/releases", release_body(meta, "v2", "amd64", "v1")
    )
    after = layer_scan_count(args.base, shared_v2)
    print(f"   amd64 v2: HTTP {amd_status} in {amd_latency:.2f}s "
          f"roles[{print_roles(amd_payload)}]")
    print(f"   physical scans of shared v2 layer: {after - before} (want 1, fresh cohort)")
    assert amd_status == 200, amd_payload
    assert after - before == 1, (before, after)

    # -- Scene 3: concurrent v3 both arches share v3 layer ------------------
    print("\n3) concurrent amd64 v3 + arm64 v3")
    before = layer_scan_count(args.base, shared_v3)
    results = concurrent_posts(
        args.base,
        {
            "amd64-v3": release_body(meta, "v3", "amd64", "v2"),
            "arm64-v3": release_body(meta, "v3", "arm64", "v2"),
        },
    )
    after = layer_scan_count(args.base, shared_v3)
    for name in ("amd64-v3", "arm64-v3"):
        status, payload, latency = results[name]
        print(f"   {name}: HTTP {status} in {latency:.2f}s "
              f"pid={payload.get('workerPid')} roles[{print_roles(payload)}]")
        assert status == 200, payload
    print(f"   physical scans of shared v3 layer: {after - before} (want 1)")
    assert after - before == 1, (before, after)

    # -- Scene 4: late duplicate rescans then conflicts ---------------------
    print("\n4) late duplicate amd64 v3 (re-reads layers, then CAS conflict)")
    before = layer_scan_count(args.base, shared_v3)
    dup_status, dup_payload, _ = call(
        args.base, "POST", "/v1/releases", release_body(meta, "v3", "amd64", "v2")
    )
    after = layer_scan_count(args.base, shared_v3)
    print(f"   duplicate: HTTP {dup_status} code={dup_payload.get('error', {}).get('code')}")
    print(f"   physical scans of shared v3 layer: {after - before} (want 1, then 409)")
    assert dup_status == 409, dup_payload
    assert after - before == 1, (before, after)

    # -- totals --------------------------------------------------------------
    _status, scans_payload, _ = call(args.base, "GET", "/v1/scans")
    stats = scans_payload["stats"]
    print("\nfinal /v1/scans totals (all uvicorn workers):")
    print(f"   scans={stats['scans']} shared={stats['shared']} "
          f"takeovers={stats['takeovers']} scanBytes={stats['scanBytes']}")
    print("   shared layer counters:")
    for label, digest in (("v2", shared_v2), ("v3", shared_v3)):
        layer = stats["layers"][digest]
        print(f"     {label} {short(digest)}: scans={layer['scans']} "
              f"shared={layer['shared']} bytes={layer['bytes']}")

    final = by_arch(call(args.base, "GET", "/v1/stable")[1])
    assert final == {arch: meta["versions"]["v3"][arch]["digest"] for arch in ("amd64", "arm64")}
    print("\ndemo complete: one physical scan per concurrent shared-layer cohort")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
