"""Cross-process per-layer scan coalescing and crash takeover.

These tests encode the shared-resource-layer contract:

* concurrent releases verifying the same layer digest share one complete scan;
* every request still checks manifest/config/descriptors itself, so a wrong
  descriptor size rejects only that request;
* once a scan cohort has ended, later requests read the layer again;
* distinct layers never wait on each other;
* a scan leader that dies mid-read is replaced by a waiting process which
  rescans, and only verified releases reach the usual CAS commit.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from app.config import Settings
from app.oci import OciLayout, ReleaseValidationError, verify_release
from app.scans import LayerScanCoordinator, ScanStats
from app.store import StableStore
from tests.conftest import make_client, stable_by_arch

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "scripts" / "scan_worker.py"

# Re-exported here so the shared-layout fixture reads top-to-bottom.
from scripts.build_layout import (  # noqa: E402
    OCI_LAYER_GZIP,
    LayoutBuilder,
    build_layer,
)


def make_shared_layout(
    tmp_path: Path,
    *,
    shared_bytes: int = 128 * 1024,
    arch_bytes: int = 8 * 1024,
    amd_version: str = "v2",
) -> dict:
    """v1 baseline images plus v2 candidates sharing layer 0 across arches."""
    layout_dir = tmp_path / "layout"
    builder = LayoutBuilder(layout_dir)

    amd_v1 = builder.add_image(arch="amd64", version="v1", payload_bytes=4096)
    arm_v1 = builder.add_image(arch="arm64", version="v1", payload_bytes=4096)

    shared_blob, shared_diff = build_layer("shared-resources", amd_version, 0, shared_bytes)
    shared_digest, shared_size = builder.write_blob(shared_blob)
    shared_desc = {
        "mediaType": OCI_LAYER_GZIP,
        "digest": shared_digest,
        "size": shared_size,
    }

    descriptors = {}
    for arch in ("amd64", "arm64"):
        arch_blob, arch_diff = build_layer(arch, amd_version, 1, arch_bytes)
        arch_digest, arch_size = builder.write_blob(arch_blob)
        arch_desc = {
            "mediaType": OCI_LAYER_GZIP,
            "digest": arch_digest,
            "size": arch_size,
        }
        descriptors[arch] = builder.add_image_from_layers(
            arch=arch,
            version=amd_version,
            layer_descs=[dict(shared_desc), arch_desc],
            diff_ids=[shared_diff, arch_diff],
            variant="v8" if arch == "arm64" else None,
        )

    builder.write_index(
        preloaded=[amd_v1, arm_v1],
        ref_name="registry.example.com/edge/stable:latest",
    )

    wrong_size_digest = builder.rewrite_manifest(
        descriptors["amd64"]["digest"],
        lambda manifest: manifest["layers"][0].__setitem__("size", shared_size + 7),
    )

    return {
        "dir": layout_dir,
        "amd_v1": amd_v1,
        "arm_v1": arm_v1,
        "amd_v2": descriptors["amd64"],
        "arm_v2": descriptors["arm64"],
        "shared": shared_desc,
        "wrong_size_amd_v2": wrong_size_digest,
    }


def make_settings(tmp_path: Path, layout_dir: Path, **overrides: str) -> Settings:
    env = {
        "OCI_LAYOUT_DIR": str(layout_dir),
        "STATE_DIR": str(tmp_path / "state"),
        "OCI_ARCHITECTURES": "amd64,arm64",
        "LAYER_CHUNK_BYTES": "1024",
        "LAYER_CHUNK_DELAY_MS": "2",
        "LAYER_SCAN_POLL_MS": "10",
        "LAYER_SCAN_WAIT_TIMEOUT_S": "60",
    }
    env.update(overrides)
    return Settings.from_env(env)


@pytest.fixture
def shared_setup(tmp_path: Path) -> dict:
    built = make_shared_layout(tmp_path)
    settings = make_settings(tmp_path, built["dir"])
    built["settings"] = settings
    return built


def counting_scans(monkeypatch):
    calls: list[str] = []
    original = OciLayout.scan_blob

    def wrapper(self: OciLayout, digest: str, **kwargs):
        calls.append(digest)
        return original(self, digest, **kwargs)

    monkeypatch.setattr(OciLayout, "scan_blob", wrapper)
    return calls


# ---------------------------------------------------------------------------
# In-process coordination
# ---------------------------------------------------------------------------


def test_concurrent_same_layer_is_scanned_once(shared_setup, monkeypatch):
    setup = shared_setup
    settings = setup["settings"]
    layout = OciLayout(settings.oci_layout)
    coordinator = LayerScanCoordinator(settings)
    calls = counting_scans(monkeypatch)
    barrier = threading.Barrier(2)
    outcomes: dict[str, object] = {}

    def verify(name: str, arch: str, digest: str) -> None:
        barrier.wait()
        verified = verify_release(
            layout, settings, arch, digest, layer_scanner=coordinator
        )
        outcomes[name] = [report.role for report in verified.layer_scans]

    threads = [
        threading.Thread(
            target=verify, args=("amd", "amd64", setup["amd_v2"]["digest"])
        ),
        threading.Thread(
            target=verify, args=("arm", "arm64", setup["arm_v2"]["digest"])
        ),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive()

    shared_digest = setup["shared"]["digest"]
    assert calls.count(shared_digest) == 1, calls
    roles = outcomes["amd"][0], outcomes["arm"][0]
    assert sorted(roles) == ["leader", "shared"], roles

    layer_stats = coordinator.stats.snapshot()["layers"][shared_digest]
    assert layer_stats["scans"] == 1
    assert layer_stats["shared"] == 1


def test_wrong_layer_size_rejects_only_that_request(shared_setup, monkeypatch):
    setup = shared_setup
    settings = setup["settings"]
    layout = OciLayout(settings.oci_layout)
    coordinator = LayerScanCoordinator(settings)
    calls = counting_scans(monkeypatch)
    barrier = threading.Barrier(2)
    results: dict[str, object] = {}

    def bad_size() -> None:
        barrier.wait()
        try:
            verify_release(
                layout,
                settings,
                "amd64",
                setup["wrong_size_amd_v2"],
                layer_scanner=coordinator,
            )
            results["bad"] = "accepted"
        except ReleaseValidationError as exc:
            results["bad"] = exc.code

    def good_arm() -> None:
        barrier.wait()
        verified = verify_release(
            layout,
            settings,
            "arm64",
            setup["arm_v2"]["digest"],
            layer_scanner=coordinator,
        )
        results["arm"] = [report.role for report in verified.layer_scans]

    threads = [threading.Thread(target=bad_size), threading.Thread(target=good_arm)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert results["bad"] == "size_mismatch", results
    assert results["arm"][0] in ("leader", "shared")
    shared_digest = setup["shared"]["digest"]
    # Even if the wrong-size request led, the blob was scanned exactly once
    # and its own descriptor comparison is what rejected it.
    assert calls.count(shared_digest) == 1, calls
    assert coordinator.stats.snapshot()["layers"][shared_digest]["scans"] == 1


def test_scan_after_cohort_ended_reads_disk_again(shared_setup, monkeypatch):
    setup = shared_setup
    settings = setup["settings"]
    layout = OciLayout(settings.oci_layout)
    coordinator = LayerScanCoordinator(settings)
    calls = counting_scans(monkeypatch)
    digest = setup["shared"]["digest"]

    first = coordinator.scan_layer(layout, digest)
    second = coordinator.scan_layer(layout, digest)

    assert first.role == "leader"
    # The finished session must not serve a later caller.
    assert second.role == "leader"
    assert calls.count(digest) == 2
    assert coordinator.stats.snapshot()["layers"][digest]["scans"] == 2


def test_distinct_layers_scan_in_parallel(shared_setup, monkeypatch):
    setup = shared_setup
    settings = setup["settings"]
    layout = OciLayout(settings.oci_layout)
    coordinator = LayerScanCoordinator(settings)
    calls = counting_scans(monkeypatch)

    import json as _json

    manifest = _json.loads(
        (settings.oci_layout / "blobs" / "sha256" / setup["amd_v2"]["digest"].split(":")[1]).read_text()
    )
    arch_layer_digest = manifest["layers"][1]["digest"]

    barrier = threading.Barrier(2)
    roles: dict[str, str] = {}

    def scan(name: str, digest: str) -> None:
        barrier.wait()
        roles[name] = coordinator.scan_layer(layout, digest).role

    started = time.monotonic()
    threads = [
        threading.Thread(target=scan, args=("shared", setup["shared"]["digest"])),
        threading.Thread(target=scan, args=("arch", arch_layer_digest)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    elapsed = time.monotonic() - started

    assert roles == {"shared": "leader", "arch": "leader"}, roles
    assert calls.count(setup["shared"]["digest"]) == 1
    assert calls.count(arch_layer_digest) == 1
    # Both layers have a 256-chunk-ish delay; serial would be roughly 2x.
    assert elapsed < 1.0, elapsed


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------


def test_api_concurrent_shared_release_reports_one_scan(shared_setup):
    setup = shared_setup
    settings = setup["settings"]
    shared_digest = setup["shared"]["digest"]
    amd_body = {
        "platform": "linux/amd64",
        "manifestDigest": setup["amd_v2"]["digest"],
        "expectedPreviousDigest": setup["amd_v1"]["digest"],
    }
    arm_body = {
        "platform": "linux/arm64",
        "manifestDigest": setup["arm_v2"]["digest"],
        "expectedPreviousDigest": setup["arm_v1"]["digest"],
    }

    with make_client(settings) as client:
        barrier = threading.Barrier(2)
        responses: dict[str, int] = {}
        details: dict[str, dict] = {}

        def post(name: str, body: dict) -> None:
            barrier.wait()
            response = client.post("/v1/releases", json=body)
            responses[name] = response.status_code
            details[name] = response.json()

        threads = [
            threading.Thread(target=post, args=("amd", amd_body)),
            threading.Thread(target=post, args=("arm", arm_body)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert responses == {"amd": 200, "arm": 200}, responses
        scans = client.get("/v1/scans").json()
        layer_stats = scans["stats"]["layers"][shared_digest]
        assert layer_stats["scans"] == 1, layer_stats
        assert layer_stats["shared"] == 1, layer_stats

        # Per-response role reporting is present and sums sensibly.
        for name in ("amd", "arm"):
            verified = details[name]["released"]["verified"]
            assert len(verified["layerScans"]) == 2
            roles = {item["digest"]: item["role"] for item in verified["layerScans"]}
            assert roles[shared_digest] in ("leader", "shared")

        stable = stable_by_arch(client.get("/v1/stable").json())
        assert stable["amd64"]["digest"] == setup["amd_v2"]["digest"]
        assert stable["arm64"]["digest"] == setup["arm_v2"]["digest"]


def test_api_wrong_size_concurrent_only_rejects_that_release(shared_setup):
    setup = shared_setup
    settings = setup["settings"]
    shared_digest = setup["shared"]["digest"]
    bad_body = {
        "platform": "linux/amd64",
        "manifestDigest": setup["wrong_size_amd_v2"],
        "expectedPreviousDigest": setup["amd_v1"]["digest"],
    }
    arm_body = {
        "platform": "linux/arm64",
        "manifestDigest": setup["arm_v2"]["digest"],
        "expectedPreviousDigest": setup["arm_v1"]["digest"],
    }

    with make_client(settings) as client:
        barrier = threading.Barrier(2)
        results: dict[str, tuple[int, str]] = {}

        def post(name: str, body: dict) -> None:
            barrier.wait()
            response = client.post("/v1/releases", json=body)
            code = response.json().get("error", {}).get("code", "")
            results[name] = (response.status_code, code)

        threads = [
            threading.Thread(target=post, args=("bad", bad_body)),
            threading.Thread(target=post, args=("arm", arm_body)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert results["bad"] == (422, "size_mismatch"), results
        assert results["arm"][0] == 200, results

        layer_stats = client.get("/v1/scans").json()["stats"]["layers"][shared_digest]
        assert layer_stats["scans"] == 1, layer_stats

        stable = stable_by_arch(client.get("/v1/stable").json())
        # The wrong-size amd release wrote nothing; arm landed.
        assert stable["amd64"]["digest"] == setup["amd_v1"]["digest"]
        assert stable["arm64"]["digest"] == setup["arm_v2"]["digest"]


def test_api_late_duplicate_release_rescans_then_conflicts(shared_setup):
    setup = shared_setup
    settings = setup["settings"]
    shared_digest = setup["shared"]["digest"]
    body = {
        "platform": "linux/amd64",
        "manifestDigest": setup["amd_v2"]["digest"],
        "expectedPreviousDigest": setup["amd_v1"]["digest"],
    }

    with make_client(settings) as client:
        assert client.post("/v1/releases", json=body).status_code == 200
        before = client.get("/v1/scans").json()["stats"]["layers"][shared_digest]["scans"]

        # Cohort ended: the late request re-reads every layer, then loses CAS.
        second = client.post("/v1/releases", json=body)
        assert second.status_code == 409

        after = client.get("/v1/scans").json()["stats"]["layers"][shared_digest]["scans"]
        assert after == before + 1, (before, after)


def test_legacy_hash_blob_is_the_single_physical_scan_point(shared_setup, monkeypatch):
    """Instrumentation wrapping OciLayout.hash_blob sees one call per cohort."""
    setup = shared_setup
    settings = setup["settings"]
    layout = OciLayout(settings.oci_layout)
    coordinator = LayerScanCoordinator(settings)

    calls: list[str] = []
    original = OciLayout.hash_blob

    def wrapper(self, digest, **kwargs):
        calls.append(digest)
        # Shared scans ask for raw facts (no descriptor size pre-check).
        assert kwargs["expected_size"] is None
        return original(self, digest, **kwargs)

    monkeypatch.setattr(OciLayout, "hash_blob", wrapper)
    barrier = threading.Barrier(2)

    def scan():
        barrier.wait()
        coordinator.scan_layer(layout, setup["shared"]["digest"])

    threads = [threading.Thread(target=scan) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert calls == [setup["shared"]["digest"]], calls


def test_coalescing_can_be_disabled(tmp_path, shared_setup):
    settings = make_settings(tmp_path, shared_setup["dir"], LAYER_SCAN_COALESCE="false")
    assert settings.layer_scan_coalesce is False
    with make_client(settings) as client:
        response = client.post(
            "/v1/releases",
            json={
                "platform": "linux/amd64",
                "manifestDigest": shared_setup["amd_v2"]["digest"],
                "expectedPreviousDigest": shared_setup["amd_v1"]["digest"],
            },
        )
        assert response.status_code == 200, response.text
        scans = client.get("/v1/scans").json()
        assert scans["coalescingEnabled"] is False


def test_scans_endpoint_reports_global_counters(shared_setup):
    settings = shared_setup["settings"]
    with make_client(settings) as client:
        response = client.get("/v1/scans")
        assert response.status_code == 200
        body = response.json()
        assert body["coalescingEnabled"] is True
        assert body["stats"]["scans"] == 0
        assert "activeLocal" in body and "sessions" in body


# ---------------------------------------------------------------------------
# Real OS processes
# ---------------------------------------------------------------------------


def _settings_json(settings: Settings, **overrides: str) -> str:
    env = {
        "OCI_LAYOUT_DIR": str(settings.oci_layout),
        "STATE_DIR": str(settings.state_dir),
        "LAYER_CHUNK_BYTES": str(settings.layer_chunk_bytes),
        "LAYER_CHUNK_DELAY_MS": str(settings.layer_chunk_delay * 1000),
        "LAYER_SCAN_POLL_MS": "10",
        "LAYER_SCAN_WAIT_TIMEOUT_S": "60",
        "PYTHONPATH": str(ROOT),
    }
    env.update(overrides)
    return json.dumps(env)


def _run_worker(settings_json: str, *extra: str) -> subprocess.Popen:
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    return subprocess.Popen(
        [sys.executable, str(WORKER), "--settings-json", settings_json, *extra],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def test_three_os_processes_share_one_scan(tmp_path):
    built = make_shared_layout(tmp_path, shared_bytes=512 * 1024)
    settings = make_settings(
        tmp_path, built["dir"], LAYER_CHUNK_BYTES="1024", LAYER_CHUNK_DELAY_MS="2"
    )
    payload = _settings_json(settings)
    digest = built["shared"]["digest"]

    procs = [
        _run_worker(payload, "--mode", "scan", "--digest", digest) for _ in range(3)
    ]
    rows = []
    for proc in procs:
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, err
        rows.append(json.loads(out.strip().splitlines()[-1]))

    roles = sorted(row["role"] for row in rows)
    assert roles == ["leader", "shared", "shared"], roles

    stats = ScanStats(settings.state_dir).snapshot()
    layer_stats = stats["layers"][digest]
    assert layer_stats["scans"] == 1, layer_stats
    assert layer_stats["shared"] == 2, layer_stats


def test_crash_leader_is_replaced_by_waiter_and_commits_once(tmp_path):
    built = make_shared_layout(tmp_path, shared_bytes=64 * 1024)
    settings = make_settings(
        tmp_path, built["dir"], LAYER_CHUNK_BYTES="1024", LAYER_CHUNK_DELAY_MS="1"
    )
    payload = _settings_json(settings)
    shared_digest = built["shared"]["digest"]
    release_args = (
        "--arch",
        "amd64",
        "--manifest",
        built["amd_v2"]["digest"],
        "--previous",
        built["amd_v1"]["digest"],
    )

    leader = _run_worker(
        payload,
        "--mode",
        "crash-leader",
        "--digest",
        shared_digest,
        "--crash-delay",
        "0.6",
        *release_args,
    )

    # Wait until the leader has elected itself and published its session.
    scan_dir = settings.state_dir / "layer-scans"
    deadline = time.monotonic() + 20
    session_files = []
    while time.monotonic() < deadline:
        session_files = list(scan_dir.glob("*.session.json")) if scan_dir.exists() else []
        if session_files:
            break
        time.sleep(0.02)
    assert session_files, "leader never created a scan session"
    # Give it time to enter the slow physical read.
    time.sleep(0.2)

    waiters = [
        _run_worker(payload, "--mode", "release", *release_args),
        _run_worker(payload, "--mode", "release", *release_args),
    ]

    out, err = leader.communicate(timeout=30)
    assert leader.returncode == 9, (leader.returncode, out, err)

    rows = []
    for proc in waiters:
        out, err = proc.communicate(timeout=60)
        assert proc.returncode == 0, err
        rows.append(json.loads(out.strip().splitlines()[-1]))

    statuses = sorted(row["status"] for row in rows)
    assert statuses == [200, 409], rows
    first_layer_roles = sorted(row["roles"][0] for row in rows)
    assert first_layer_roles == ["shared", "takeover"], rows

    layer_stats = ScanStats(settings.state_dir).snapshot()["layers"][shared_digest]
    assert layer_stats["scans"] == 1, layer_stats
    assert layer_stats["takeovers"] == 1, layer_stats

    # Verified takeover committed through the original compare-and-swap rules.
    store = StableStore(settings, OciLayout(settings.oci_layout))
    store.initialize()
    snapshot = store.snapshot()
    by_arch = stable_by_arch(snapshot.index)
    assert by_arch["amd64"]["digest"] == built["amd_v2"]["digest"]
    assert snapshot.revision == 1
