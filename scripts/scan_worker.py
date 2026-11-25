#!/usr/bin/env python3
"""Subprocess worker used by the cross-process scan tests and demos.

It exercises the same public stack as the API:

    Settings -> OciLayout -> LayerScanCoordinator -> verify_release -> StableStore

Modes
-----
scan
    Coordinate one layer digest; print its role/facts.
release
    Verify a manifest with the shared scanner and optionally CAS-commit it.
crash-leader
    Become the scan leader, sleep inside the physical layer read, then exit
    immediately (``os._exit``) without releasing any lock -- the exact
    SIGKILL-style failure a waiting process must recover from.

The script prints exactly one JSON object on stdout on completion.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

# Allow running directly from a checkout without installation.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import Settings  # noqa: E402
from app.oci import OciLayout, ReleaseValidationError, verify_release  # noqa: E402
from app.scans import LayerScanCoordinator, ScanStats  # noqa: E402
from app.store import ConflictError, StableStore  # noqa: E402


def build(settings_json: str | None) -> tuple[Settings, OciLayout, LayerScanCoordinator, StableStore]:
    env: dict[str, str] = dict(os.environ)
    if settings_json:
        env.update(json.loads(settings_json))
    settings = Settings.from_env(env)
    layout = OciLayout(settings.oci_layout)
    stats = ScanStats(settings.state_dir)
    coordinator = LayerScanCoordinator(settings, stats)
    store = StableStore(settings, layout)
    store.initialize()
    return settings, layout, coordinator, store


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings-json", default=None)
    parser.add_argument("--mode", choices=("scan", "release", "crash-leader"), required=True)
    parser.add_argument("--digest", default=None)
    parser.add_argument("--arch", default=None)
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--previous", default=None)
    parser.add_argument("--crash-delay", type=float, default=0.5)
    args = parser.parse_args(argv)

    settings, layout, coordinator, store = build(args.settings_json)

    if args.mode == "scan":
        assert args.digest
        outcome = coordinator.scan_layer(layout, args.digest)
        print(
            json.dumps(
                {
                    "role": outcome.role,
                    "session": outcome.session,
                    "size": outcome.scan.size,
                    "chunks": outcome.scan.chunks,
                    "pid": os.getpid(),
                }
            )
        )
        return 0

    if args.mode == "crash-leader":
        target_digest = args.digest
        original_scan_blob = OciLayout.scan_blob

        def crashing_scan_blob(self: OciLayout, digest: str, **kwargs: Any):
            if target_digest is None or digest == target_digest:
                print(
                    json.dumps({"event": "crash-leader-inside-scan", "pid": os.getpid()}),
                    flush=True,
                )
                time.sleep(args.crash_delay)
                # Immediate process death: no finally blocks, flock released
                # by the kernel when the process vanishes.
                os._exit(9)
            return original_scan_blob(self, digest, **kwargs)

        OciLayout.scan_blob = crashing_scan_blob  # type: ignore[assignment]

    assert args.arch and args.manifest
    try:
        verified = verify_release(
            layout,
            settings,
            args.arch,
            args.manifest,
            layer_scanner=coordinator,
        )
    except ReleaseValidationError as exc:
        print(json.dumps({"status": 422, "code": exc.code, "message": str(exc)}))
        return 0

    roles = [item.role for item in verified.layer_scans]
    result: dict[str, Any] = {"status": 200, "roles": roles, "pid": os.getpid()}

    if args.previous is not None:
        try:
            snapshot, _record = store.commit(verified, args.previous)
            result["commit"] = "ok"
            result["revision"] = snapshot.revision
        except ConflictError as exc:
            result["status"] = 409
            result["commit"] = "conflict"
            result["currentDigest"] = exc.current
    else:
        result["commit"] = "none"

    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
