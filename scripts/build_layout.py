"""Build deterministic OCI image layouts for demos and tests.

Produces a real ``oci-layout`` directory:

    <root>/oci-layout
    <root>/index.json          -> points at a nested multi-arch index blob
    <root>/blobs/sha256/<hex>  -> configs, layers, manifests, index

Everything is deterministic for a given architecture/version/seed so the demo
metadata file always matches the blobs on disk.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import random
import tarfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
OCI_LAYER_GZIP = "application/vnd.oci.image.layer.v1.tar+gzip"


def sha256_digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def build_layer(arch: str, version: str, index: int, payload_bytes: int) -> tuple[bytes, str]:
    """Return (gzipped layer bytes, uncompressed diff_id)."""
    rng = random.Random(f"{arch}/{version}/{index}")
    payload = rng.randbytes(payload_bytes)

    tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=tar_buffer, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for name, data in (
            ("etc/os-release", f"ID=edge\nVERSION={version}\nARCH={arch}\n".encode()),
            ("payload.bin", payload),
        ):
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mtime = 0
            info.mode = 0o644
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            tar.addfile(info, io.BytesIO(data))
    tar_bytes = tar_buffer.getvalue()

    gz_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gz_buffer, mode="wb", mtime=0, filename="") as gz:
        gz.write(tar_bytes)
    return gz_buffer.getvalue(), sha256_digest(tar_bytes)


@dataclass
class LayoutBuilder:
    root: Path
    manifests: dict[str, dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.root = Path(self.root)
        (self.root / "blobs" / "sha256").mkdir(parents=True, exist_ok=True)

    def write_blob(self, data: bytes) -> tuple[str, int]:
        digest = sha256_digest(data)
        path = self.root / "blobs" / "sha256" / digest.split(":", 1)[1]
        if not path.exists():
            path.write_bytes(data)
        return digest, len(data)

    def add_image(
        self,
        *,
        arch: str,
        version: str,
        layers: int = 2,
        payload_bytes: int = 512 * 1024,
        variant: str | None = None,
    ) -> dict[str, Any]:
        layer_descs: list[dict[str, Any]] = []
        diff_ids: list[str] = []
        for position in range(layers):
            blob, diff_id = build_layer(arch, version, position, payload_bytes)
            digest, size = self.write_blob(blob)
            layer_descs.append(
                {"mediaType": OCI_LAYER_GZIP, "digest": digest, "size": size}
            )
            diff_ids.append(diff_id)
        return self.add_image_from_layers(
            arch=arch,
            version=version,
            layer_descs=layer_descs,
            diff_ids=diff_ids,
            variant=variant,
        )

    def add_image_from_layers(
        self,
        *,
        arch: str,
        version: str,
        layer_descs: list[dict[str, Any]],
        diff_ids: list[str],
        variant: str | None = None,
        annotation_version: str | None = None,
    ) -> dict[str, Any]:
        """Assemble config/manifest/descriptor from pre-built layers.

        Sharing the same ``layer_descs[0]`` between two calls is exactly how a
        cross-architecture image reuses a resource layer.
        """
        version_label = annotation_version or version
        config: dict[str, Any] = {
            "architecture": arch,
            "os": "linux",
            "created": "1970-01-01T00:00:00Z",
            "config": {
                "Env": ["PATH=/usr/local/bin:/usr/bin:/bin"],
                "Labels": {"org.opencontainers.image.version": version_label},
                "Cmd": ["/usr/local/bin/edge-agent"],
            },
            "rootfs": {"type": "layers", "diff_ids": diff_ids},
            "history": [
                {"created": "1970-01-01T00:00:00Z", "created_by": f"demo {version}"}
            ],
        }
        if variant:
            config["variant"] = variant
        config_bytes = canonical_json(config)
        config_digest, config_size = self.write_blob(config_bytes)

        manifest = {
            "schemaVersion": 2,
            "mediaType": OCI_MANIFEST,
            "config": {
                "mediaType": OCI_CONFIG,
                "digest": config_digest,
                "size": config_size,
            },
            "layers": layer_descs,
            "annotations": {
                "org.opencontainers.image.version": version_label,
                "org.opencontainers.image.ref.name": f"{arch}-{version}",
            },
        }
        manifest_bytes = canonical_json(manifest)
        manifest_digest, manifest_size = self.write_blob(manifest_bytes)

        platform: dict[str, str] = {"architecture": arch, "os": "linux"}
        if variant:
            platform["variant"] = variant
        descriptor = {
            "mediaType": OCI_MANIFEST,
            "digest": manifest_digest,
            "size": manifest_size,
            "platform": platform,
            "annotations": {
                "org.opencontainers.image.version": version_label,
            },
        }
        self.manifests[f"{arch}/{version}"] = descriptor
        return descriptor

    def rewrite_manifest(
        self, manifest_digest: str, mutate: Any, *, annotation_version: str | None = None
    ) -> str:
        """Clone a manifest blob through ``mutate(manifest)`` and write it."""
        path = self.root / "blobs" / "sha256" / manifest_digest.split(":", 1)[1]
        manifest = json.loads(path.read_bytes())
        mutate(manifest)
        if annotation_version is not None:
            manifest.setdefault("annotations", {})[
                "org.opencontainers.image.version"
            ] = annotation_version
        digest, _ = self.write_blob(canonical_json(manifest))
        return digest

    def write_index(
        self,
        *,
        preloaded: list[dict[str, Any]],
        ref_name: str,
        nested: bool = True,
    ) -> dict[str, Any]:
        index = {
            "schemaVersion": 2,
            "mediaType": OCI_INDEX,
            "manifests": preloaded,
            "annotations": {
                "org.opencontainers.image.ref.name": ref_name,
                "org.opencontainers.image.version": "stable",
            },
        }
        index_bytes = canonical_json(index)
        index_digest, index_size = self.write_blob(index_bytes)

        (self.root / "oci-layout").write_text(
            json.dumps({"imageLayoutVersion": "1.0.0"}, indent=2) + "\n",
            encoding="utf-8",
        )
        if nested:
            layout_index = {
                "schemaVersion": 2,
                "mediaType": OCI_INDEX,
                "manifests": [
                    {
                        "mediaType": OCI_INDEX,
                        "digest": index_digest,
                        "size": index_size,
                        "annotations": {"org.opencontainers.image.ref.name": ref_name},
                    }
                ],
            }
        else:
            layout_index = index
        (self.root / "index.json").write_text(
            json.dumps(layout_index, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return {
            "indexDigest": index_digest,
            "indexSize": index_size,
            "index": index,
        }


def build_demo_layout(
    out_dir: Path,
    meta_path: Path,
    *,
    amd64_payload_bytes: int = 512 * 1024,
    arm64_payload_bytes: int = 512 * 1024,
    layers: int = 2,
) -> dict[str, Any]:
    """Layout with stable v1 preloaded plus candidate v2/v3 manifests.

    v1 is what the service starts with on both architectures.  v2 and v3
    manifests are already present as blobs so the API can verify them without
    a registry, exactly like an air-gapped edge rollout.

    amd64 layers can be made deliberately larger than arm64 so the concurrent
    demo has a slow scan on one architecture and a fast one on the other.
    """
    builder = LayoutBuilder(out_dir)
    payloads = {"amd64": amd64_payload_bytes, "arm64": arm64_payload_bytes}
    descriptors: dict[str, dict[str, dict[str, Any]]] = {}
    versions = {
        "v1": {"amd64": "v1", "arm64": "v1"},
        "v2": {"amd64": "v2", "arm64": "v2"},
        "v3": {"amd64": "v3", "arm64": "v3"},
    }
    variants = {"amd64": None, "arm64": "v8"}
    for version in ("v1", "v2", "v3"):
        descriptors[version] = {}
        for arch in ("amd64", "arm64"):
            descriptors[version][arch] = builder.add_image(
                arch=arch,
                version=version,
                variant=variants[arch],
                layers=layers,
                payload_bytes=payloads[arch],
            )

    ref = "registry.example.com/edge/stable:latest"
    preloaded = [descriptors["v1"]["amd64"], descriptors["v1"]["arm64"]]
    written = builder.write_index(preloaded=preloaded, ref_name=ref, nested=True)

    meta = {
        "ref": ref,
        "preloadedVersion": "v1",
        "versions": descriptors,
        "indexDigest": written["indexDigest"],
        "layout": str(out_dir),
    }
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return meta


def build_shared_demo_layout(
    out_dir: Path,
    meta_path: Path,
    *,
    shared_payload_bytes: int = 4 * 1024 * 1024,
    amd64_payload_bytes: int = 2 * 1024 * 1024,
    arm64_payload_bytes: int = 8 * 1024,
) -> dict[str, Any]:
    """Layout where amd64 and arm64 share one resource layer per version.

    Each image has two layers:

    * layer 0 is the *same* blob (same digest/size) for both architectures of
      a given version -- the shared "resource" layer;
    * layer 1 is architecture specific and keeps the slow-amd64/fast-arm64
      shape used by the concurrency demo.

    Also writes a deliberately wrong-size amd64 v2 manifest candidate under
    ``mutations`` for the "wrong layer size rejects only that request" scene.
    """
    builder = LayoutBuilder(out_dir)
    payloads = {"amd64": amd64_payload_bytes, "arm64": arm64_payload_bytes}
    variants = {"amd64": None, "arm64": "v8"}
    descriptors: dict[str, dict[str, dict[str, Any]]] = {}
    shared_layers: dict[str, dict[str, Any]] = {}

    for version in ("v1", "v2", "v3"):
        descriptors[version] = {}
        # Architecture-independent seed => byte-identical layer for both arches.
        shared_blob, shared_diff_id = build_layer(
            "shared-resources", version, 0, shared_payload_bytes
        )
        shared_digest, shared_size = builder.write_blob(shared_blob)
        shared_desc = {
            "mediaType": OCI_LAYER_GZIP,
            "digest": shared_digest,
            "size": shared_size,
        }
        shared_layers[version] = shared_desc

        for arch in ("amd64", "arm64"):
            arch_blob, arch_diff_id = build_layer(
                arch, version, 1, payloads[arch]
            )
            arch_digest, arch_size = builder.write_blob(arch_blob)
            arch_desc = {
                "mediaType": OCI_LAYER_GZIP,
                "digest": arch_digest,
                "size": arch_size,
            }
            descriptors[version][arch] = builder.add_image_from_layers(
                arch=arch,
                version=version,
                layer_descs=[dict(shared_desc), arch_desc],
                diff_ids=[shared_diff_id, arch_diff_id],
                variant=variants[arch],
            )

    # Wrong-size clone of the amd64 v2 manifest: same digests, bad layer[0].size.
    good_amd_v2 = descriptors["v2"]["amd64"]["digest"]
    actual_shared_size = shared_layers["v2"]["size"]
    wrong_digest = builder.rewrite_manifest(
        good_amd_v2,
        lambda manifest: manifest["layers"][0].__setitem__(
            "size", actual_shared_size + 7
        ),
        annotation_version="v2-wrong-size",
    )

    ref = "registry.example.com/edge/stable:latest"
    preloaded = [descriptors["v1"]["amd64"], descriptors["v1"]["arm64"]]
    written = builder.write_index(preloaded=preloaded, ref_name=ref, nested=True)

    meta = {
        "ref": ref,
        "preloadedVersion": "v1",
        "sharedLayers": shared_layers,
        "versions": descriptors,
        "mutations": {
            "amd64V2WrongSharedLayerSize": {
                "platform": "linux/amd64",
                "digest": wrong_digest,
                "expectedPreviousDigest": descriptors["v1"]["amd64"]["digest"],
                "layerDigest": shared_layers["v2"]["digest"],
                "actualSize": actual_shared_size,
                "wrongSize": actual_shared_size + 7,
            }
        },
        "indexDigest": written["indexDigest"],
        "layout": str(out_dir),
        "sharedLayerLayout": True,
    }
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return meta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="oci-layout", help="output layout directory")
    parser.add_argument(
        "--meta", default="examples/demo-digests.json", help="where to write digest metadata"
    )
    parser.add_argument(
        "--shared",
        action="store_true",
        help="build the shared-resource-layer layout used by Docker Compose",
    )
    parser.add_argument(
        "--shared-payload-bytes",
        type=int,
        default=4 * 1024 * 1024,
        help="identical shared layer payload per version (--shared only)",
    )
    parser.add_argument(
        "--amd64-payload-bytes",
        type=int,
        default=512 * 1024,
        help="random payload per amd64 layer (bigger = slower verification)",
    )
    parser.add_argument(
        "--arm64-payload-bytes",
        type=int,
        default=512 * 1024,
        help="random payload per arm64 layer",
    )
    parser.add_argument("--layers", type=int, default=2, help="layers per image")
    args = parser.parse_args(argv)
    if args.shared:
        meta = build_shared_demo_layout(
            Path(args.out),
            Path(args.meta),
            shared_payload_bytes=args.shared_payload_bytes,
            amd64_payload_bytes=args.amd64_payload_bytes,
            arm64_payload_bytes=args.arm64_payload_bytes,
        )
    else:
        meta = build_demo_layout(
            Path(args.out),
            Path(args.meta),
            amd64_payload_bytes=args.amd64_payload_bytes,
            arm64_payload_bytes=args.arm64_payload_bytes,
            layers=args.layers,
        )
    print(json.dumps(meta["versions"], indent=2, sort_keys=True))
    print(f"wrote layout to {args.out} and metadata to {args.meta}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
