"""Cross-process single-flight sharing of full layer scans.

When an amd64 and an arm64 manifest reference the same layer blob (a shared
resource layer), concurrent ``POST /v1/releases`` calls should read and hash
that blob once per concurrent cohort instead of once per request.

Coordination uses only the shared writable state directory and ``flock``, so it
works between uvicorn worker processes, plain subprocesses and threads:

``<state>/layer-scans/<digest-hex>.meta.lock``
    Brief exclusive lock covering session election.

``<state>/layer-scans/<digest-hex>.session.json``
    Points at the current session id.

``<state>/layer-scans/<digest-hex>.<session>.leader``
    The elected leader holds an ``flock`` exclusive lock on this file for the
    whole scan.  If the leader process exits -- including ``SIGKILL`` -- the
    kernel releases the lock and a waiter's non-blocking lock attempt succeeds,
    which is the crash/takeover signal.

``<state>/layer-scans/<digest-hex>.<session>.result.json``
    Atomically published scan facts (size, chunk count, computed digest).

Lifecycle semantics deliberately match the release API contract:

* requests that arrive while a session is running wait for and reuse its one
  complete scan;
* a request that arrives after the session finished never reuses the old
  result: electing against a free leader lock with a result present starts a
  fresh session, so the layer is read from disk again;
* a leader that disappears without publishing a result is replaced by a
  waiting process which rescans;
* coordination is per layer digest, so layers unique to another architecture
  never wait on an unrelated scan.

Descriptor size/digest comparisons stay in :mod:`app.oci` for every request.
"""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import Settings
from .oci import BlobScan, LayerScanOutcome, OciLayout, validate_digest

SCAN_DIRNAME = "layer-scans"
STATS_FILENAME = "layer-scan-stats.json"
STATS_LOCK_FILENAME = "layer-scan-stats.lock"
STATS_SCHEMA_VERSION = 1
# Old session artefacts of a previous cohort for the same layer are swept only
# past this age; live scans are far shorter, so an active waiter can never
# lose its files.
SESSION_FILE_MAX_AGE_SECONDS = 3600


class ScanCoordinationError(Exception):
    """A shared scan result did not appear before the wait timeout."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _blank_stats() -> dict[str, Any]:
    return {
        "schemaVersion": STATS_SCHEMA_VERSION,
        "updatedAt": None,
        "scans": 0,
        "shared": 0,
        "takeovers": 0,
        "scanBytes": 0,
        "layers": {},
    }


def _read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError:
        return None
    try:
        data = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(
                json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
                    "utf-8"
                )
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


class ScanStats:
    """Cross-process cumulative scan counters.

    The authoritative copy is a small JSON file in ``STATE_DIR`` updated under
    an ``flock``.  An in-memory copy keeps counters usable when the state
    directory is read-only.
    """

    def __init__(self, state_dir: Path) -> None:
        self.state_dir = Path(state_dir)
        self.path = self.state_dir / STATS_FILENAME
        self.lock_path = self.state_dir / STATS_LOCK_FILENAME
        self._memory_lock = threading.Lock()
        self._memory = _blank_stats()

    def record(
        self,
        digest: str,
        *,
        scans: int = 0,
        shared: int = 0,
        takeovers: int = 0,
        chunks: int = 0,
        scan_bytes: int = 0,
    ) -> None:
        with self._memory_lock:
            self._memory["scans"] += scans
            self._memory["shared"] += shared
            self._memory["takeovers"] += takeovers
            self._memory["scanBytes"] += scan_bytes
            layer = self._memory["layers"].setdefault(
                digest, {"scans": 0, "shared": 0, "takeovers": 0, "chunks": 0, "bytes": 0}
            )
            layer["scans"] += scans
            layer["shared"] += shared
            layer["takeovers"] += takeovers
            if scans:
                layer["chunks"] += chunks
                layer["bytes"] += scan_bytes

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._locked():
                data = _read_json_file(self.path) or _blank_stats()
                data["scans"] = int(data.get("scans", 0)) + scans
                data["shared"] = int(data.get("shared", 0)) + shared
                data["takeovers"] = int(data.get("takeovers", 0)) + takeovers
                data["scanBytes"] = int(data.get("scanBytes", 0)) + scan_bytes
                layers = data.setdefault("layers", {})
                layer = layers.setdefault(
                    digest,
                    {"scans": 0, "shared": 0, "takeovers": 0, "chunks": 0, "bytes": 0},
                )
                for key, value in (
                    ("scans", scans),
                    ("shared", shared),
                    ("takeovers", takeovers),
                ):
                    layer[key] = int(layer.get(key, 0)) + value
                if scans:
                    layer["chunks"] = int(layer.get("chunks", 0)) + chunks
                    layer["bytes"] = int(layer.get("bytes", 0)) + scan_bytes
                data["updatedAt"] = _utc_now()
                _atomic_write_json(self.path, data)
        except OSError:
            # Read-only/unwritable state: process-local counters still work.
            pass

    @contextmanager
    def _locked(self) -> Iterator[None]:
        handle = open(self.lock_path, "a+b")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            handle.close()

    def snapshot(self) -> dict[str, Any]:
        data = _read_json_file(self.path)
        if data is not None:
            return data
        with self._memory_lock:
            return json.loads(json.dumps(self._memory))


class LayerScanCoordinator:
    """Per-layer single-flight scanner shared by processes and threads."""

    def __init__(self, settings: Settings, stats: ScanStats | None = None) -> None:
        self.settings = settings
        self.state_dir = Path(settings.state_dir)
        self.dir = self.state_dir / SCAN_DIRNAME
        self.poll_seconds = settings.scan_poll_seconds
        self.wait_timeout = settings.scan_wait_timeout_seconds
        self.stats = stats or ScanStats(self.state_dir)
        self.enabled = True
        # In-process bookkeeping for diagnostics (/v1/scans "activeLocal").
        self._active_lock = threading.Lock()
        self._active: dict[str, dict[str, Any]] = {}
        try:
            self.dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            # Read-only state dir: degrade to independent direct scans.
            self.enabled = False

    # -- paths -------------------------------------------------------------
    def _key(self, digest: str) -> str:
        validate_digest(digest)
        return digest.split(":", 1)[1]

    def _meta_lock_path(self, key: str) -> Path:
        return self.dir / f"{key}.meta.lock"

    def _session_path(self, key: str) -> Path:
        return self.dir / f"{key}.session.json"

    def _leader_path(self, key: str, session: str) -> Path:
        return self.dir / f"{key}.{session}.leader"

    def _result_path(self, key: str, session: str) -> Path:
        return self.dir / f"{key}.{session}.result.json"

    # -- low-level flock helpers -------------------------------------------
    @contextmanager
    def _meta_lock(self, key: str) -> Iterator[None]:
        path = self._meta_lock_path(key)
        handle = open(path, "a+b")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            handle.close()

    @staticmethod
    def _open_lock_fd(path: Path) -> int:
        return os.open(path, os.O_RDWR | os.O_CREAT, 0o600)

    @staticmethod
    def _try_exclusive(fd: int) -> bool:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False
        except OSError:
            return False

    @staticmethod
    def _close_fd(fd: int) -> None:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass

    # -- local active registry ---------------------------------------------
    def _begin_active(self, digest: str, role: str, session: str) -> None:
        with self._active_lock:
            self._active[digest] = {
                "digest": digest,
                "role": role,
                "session": session,
                "startedAt": _utc_now(),
                "pid": os.getpid(),
            }

    def _end_active(self, digest: str) -> None:
        with self._active_lock:
            self._active.pop(digest, None)

    def active_local(self) -> list[dict[str, Any]]:
        with self._active_lock:
            return [dict(item) for item in self._active.values()]

    def active_sessions(self) -> list[dict[str, Any]]:
        """Cross-process view of session pointer files and leader liveness."""
        sessions: list[dict[str, Any]] = []
        try:
            paths = sorted(self.dir.glob("*.session.json"))
        except OSError:
            return sessions
        for path in paths:
            data = _read_json_file(path)
            if not data:
                continue
            key = path.name.split(".", 1)[0]
            session = str(data.get("session", ""))
            leader_alive: bool | None = None
            if session:
                leader_path = self._leader_path(key, session)
                try:
                    fd = os.open(leader_path, os.O_RDONLY)
                except OSError:
                    fd = -1
                if fd >= 0:
                    try:
                        if self._try_exclusive(fd):
                            leader_alive = False
                        else:
                            leader_alive = True
                    finally:
                        self._close_fd(fd)
            sessions.append(
                {
                    "digest": "sha256:" + key,
                    "session": session,
                    "pid": data.get("pid"),
                    "startedAt": data.get("startedAt"),
                    "leaderAlive": leader_alive,
                    "hasResult": self._result_path(key, session).exists() if session else False,
                }
            )
        return sessions

    # -- result (de)serialisation ------------------------------------------
    def _read_result(self, path: Path) -> BlobScan | None:
        data = _read_json_file(path)
        if not data or data.get("ok") is not True:
            return None
        facts = data.get("facts")
        if not isinstance(facts, dict):
            return None
        try:
            return BlobScan(
                digest=str(facts["digest"]),
                size=int(facts["size"]),
                chunks=int(facts["chunks"]),
                computed_digest=str(facts["computedDigest"]),
            )
        except (KeyError, TypeError, ValueError):
            return None

    def _publish_result(self, key: str, session: str, facts: BlobScan) -> None:
        payload = {
            "ok": True,
            "publishedAt": _utc_now(),
            "facts": {
                "digest": facts.digest,
                "size": facts.size,
                "chunks": facts.chunks,
                "computedDigest": facts.computed_digest,
            },
        }
        try:
            _atomic_write_json(self._result_path(key, session), payload)
        except OSError:
            # If waiters cannot read our result they will safely rescan.
            pass

    # -- election -----------------------------------------------------------
    def _write_session(self, key: str, session: str) -> None:
        try:
            _atomic_write_json(
                self._session_path(key),
                {"session": session, "pid": os.getpid(), "startedAt": _utc_now()},
            )
        except OSError:
            pass

    def _new_session(self, key: str) -> tuple[str, int]:
        """Create and lock a fresh leader session. Caller holds meta lock."""
        session = uuid.uuid4().hex
        fd = self._open_lock_fd(self._leader_path(key, session))
        if not self._try_exclusive(fd):
            # Cannot happen while the meta lock serialises election, but keep
            # the guarantee explicit rather than scanning unlocked.
            self._close_fd(fd)
            raise OSError(f"fresh layer scan leader lock for {key} was busy")
        self._write_session(key, session)
        self._sweep_old_sessions(key, keep=session)
        return session, fd

    def _elect(self, key: str) -> tuple[str, str, int | None]:
        """Return (session id, role, held leader fd or None for a waiter)."""
        with self._meta_lock(key):
            pointer = _read_json_file(self._session_path(key))
            session = pointer.get("session") if pointer else None
            if isinstance(session, str) and session:
                fd = self._open_lock_fd(self._leader_path(key, session))
                if self._try_exclusive(fd):
                    # No live leader.  A result means the cohort finished:
                    # this is a late arrival and must start a fresh scan.
                    if self._result_path(key, session).exists():
                        self._close_fd(fd)
                        session, fd = self._new_session(key)
                        return session, "leader", fd
                    # No result: the leader died mid-scan. Take it over.
                    return session, "takeover", fd
                self._close_fd(fd)
                return session, "waiter", None
            session, fd = self._new_session(key)
            return session, "leader", fd

    def _sweep_old_sessions(self, key: str, *, keep: str) -> None:
        """Best-effort removal of aged session artefacts for this layer."""
        cutoff = time.time() - SESSION_FILE_MAX_AGE_SECONDS
        try:
            for path in self.dir.glob(f"{key}.*"):
                if path.name in (f"{key}.meta.lock",) or f".{keep}." in path.name:
                    continue
                try:
                    if path.stat().st_mtime < cutoff:
                        path.unlink()
                except OSError:
                    continue
        except OSError:
            pass

    # -- scanning / waiting -------------------------------------------------
    def _produce(self, layout: OciLayout, digest: str) -> BlobScan:
        return layout.scan_blob(
            digest,
            chunk_size=self.settings.layer_chunk_bytes,
            chunk_delay=self.settings.layer_chunk_delay,
        )

    def _await_result_or_takeover(
        self, key: str, session: str, layout: OciLayout, digest: str
    ) -> tuple[BlobScan, str]:
        result_path = self._result_path(key, session)
        leader_path = self._leader_path(key, session)
        deadline = time.monotonic() + self.wait_timeout
        fd = self._open_lock_fd(leader_path)
        try:
            while True:
                facts = self._read_result(result_path)
                if facts is not None:
                    return facts, "shared"

                if self._try_exclusive(fd):
                    # We hold the leader lock. Re-check the result first: the
                    # leader may have published between our read and lock.
                    facts = self._read_result(result_path)
                    if facts is not None:
                        return facts, "shared"
                    # Lock free + no result => leader vanished mid-scan.
                    facts = self._produce(layout, digest)
                    self._publish_result(key, session, facts)
                    return facts, "takeover"

                if time.monotonic() >= deadline:
                    raise ScanCoordinationError(
                        f"timed out after {self.wait_timeout:.0f}s waiting for layer scan {digest}"
                    )
                time.sleep(self.poll_seconds)
        finally:
            self._close_fd(fd)

    def scan_layer(self, layout: OciLayout, digest: str) -> LayerScanOutcome:
        """Return layer facts, coalescing concurrent scans for one digest."""
        if not self.enabled:
            facts = self._produce(layout, digest)
            self.stats.record(
                digest,
                scans=1,
                chunks=facts.chunks,
                scan_bytes=facts.size,
            )
            return LayerScanOutcome(scan=facts, role="direct")

        key = self._key(digest)
        try:
            session, role, leader_fd = self._elect(key)
        except OSError:
            # Coordination files unusable: correctness first, scan directly.
            facts = self._produce(layout, digest)
            self.stats.record(digest, scans=1, chunks=facts.chunks, scan_bytes=facts.size)
            return LayerScanOutcome(scan=facts, role="direct")

        self._begin_active(digest, role if role != "waiter" else "waiting", session)
        try:
            if leader_fd is None:
                try:
                    facts, final_role = self._await_result_or_takeover(
                        key, session, layout, digest
                    )
                except OSError:
                    # Shared directory hiccup: do not fail the release, scan.
                    facts = self._produce(layout, digest)
                    self.stats.record(
                        digest, scans=1, chunks=facts.chunks, scan_bytes=facts.size
                    )
                    return LayerScanOutcome(scan=facts, role="direct")
                if final_role == "shared":
                    self.stats.record(digest, shared=1)
                else:
                    self.stats.record(
                        digest,
                        scans=1,
                        takeovers=1,
                        chunks=facts.chunks,
                        scan_bytes=facts.size,
                    )
                return LayerScanOutcome(scan=facts, role=final_role, session=session)

            # Elected leader or immediate takeover: fd stays locked until the
            # result is durably published, so a waiter can never acquire the
            # lock in the gap between "scan done" and "result visible".
            try:
                try:
                    facts = self._produce(layout, digest)
                    self._publish_result(key, session, facts)
                finally:
                    self._close_fd(leader_fd)
            except BaseException:
                # Lock released without a result; waiting processes take over.
                raise
            self.stats.record(
                digest,
                scans=1,
                takeovers=1 if role == "takeover" else 0,
                chunks=facts.chunks,
                scan_bytes=facts.size,
            )
            return LayerScanOutcome(scan=facts, role=role, session=session)
        finally:
            self._end_active(digest)
