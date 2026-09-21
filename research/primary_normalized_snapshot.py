#!/usr/bin/env python3
"""Archive one verified primary normalized day in OCI Object Storage.

The raw OCI evidence remains in its own bucket prefix.  This helper only handles
the normalizer's derived files, and will evict those files only after an
authenticated, streaming readback of every immutable object succeeds.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

from verify_primary_oci_day import BUCKET, NAMESPACE, REGION

PREFIX = "research-primary-normalized"
SCHEMA = "polyedge.primary_normalized_snapshot.v1"
SHA_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
CHUNK = 1024 * 1024
_KINDS = ("runtime_provenance", "market", "reference", "book", "fair_value", "decision",
          "execution_report", "paper_settlement", "feed_error", "raw_market_event",
          "price_change", "last_trade", "book_snapshot", "level_change", "other")


class SnapshotError(ValueError):
    pass


def _sha_file(path: Path) -> tuple[str, int]:
    digest, size = hashlib.sha256(), 0
    with path.open("rb") as source:
        while block := source.read(CHUNK):
            digest.update(block)
            size += len(block)
    return "sha256:" + digest.hexdigest(), size


def _safe_path(value: str) -> Path:
    path = PurePosixPath(value)
    if not value or path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        raise SnapshotError(f"unsafe normalized snapshot path: {value}")
    return Path(*path.parts)


def _sha_value(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SHA_RE.fullmatch(value):
        raise SnapshotError(f"{label} is not a sha256 digest")
    return value


def _expected_payload(manifest: dict[str, Any], root: Path) -> list[Path]:
    fmt = manifest.get("format")
    suffix = "" if fmt == "jsonl-indexed" else ".gz"
    if fmt not in ("jsonl-indexed", "jsonl-indexed-gzip", "jsonl-indexed-gzip-sharded"):
        raise SnapshotError("unsupported native normalized format")
    names = {
        "runtime_provenance": "runtime_provenance.jsonl", "market": "markets.jsonl",
        "reference": "references.jsonl", "book": "books.jsonl", "fair_value": "fair_values.jsonl",
        "decision": "decisions.jsonl", "execution_report": "execution_reports.jsonl",
        "paper_settlement": "paper_settlements.jsonl", "feed_error": "feed_errors.jsonl",
        "raw_market_event": "raw_market_events.jsonl", "price_change": "price_changes.jsonl",
        "last_trade": "last_trades.jsonl", "book_snapshot": "book_snapshots.jsonl",
        "level_change": "level_changes.jsonl", "other": "other.jsonl",
    }
    expected = {name + suffix for name in names.values()}
    if fmt != "jsonl-indexed-gzip-sharded":
        expected.add("events.jsonl" + suffix)
    files = manifest.get("files")
    if not isinstance(files, dict) or set(files) != {"events", *_KINDS}:
        raise SnapshotError("native events manifest file inventory is incomplete")
    for kind in _KINDS:
        row = files[kind]
        if not isinstance(row, dict) or not isinstance(row.get("path"), str) or not isinstance(row.get("rows"), int):
            raise SnapshotError("native events manifest file inventory is malformed")
        if not _native_path_name(row["path"], names[kind] + suffix):
            raise SnapshotError("native events manifest payload path differs from normalized directory")
    event = files["events"]
    if fmt == "jsonl-indexed-gzip-sharded":
        if event is not None:
            raise SnapshotError("sharded native manifest must omit events file")
    elif not isinstance(event, str) or not _native_path_name(event, "events.jsonl" + suffix):
        raise SnapshotError("native events manifest events path differs from normalized directory")
    return [Path(name) for name in sorted(expected)]


def _native_path_name(value: str, expected_name: str) -> bool:
    """Validate the normalizer's recorded container path without resolving it locally."""
    path = PurePosixPath(value.replace("\\", "/"))
    return (path.name == expected_name and path.parent.name == "normalized"
            and not any(part in ("", ".", "..") for part in path.parts))


def _files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if path.is_symlink():
            raise SnapshotError(f"snapshot refuses symbolic link: {path}")
        if path.is_file():
            yield path.relative_to(root)


def _local_binding(normalized: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    root = normalized.resolve()
    if not root.is_dir() or normalized.is_symlink():
        raise SnapshotError("normalized input is not a real directory")
    manifest_path = root / "events_manifest.json"
    marker_path = root / ".polyedge-daily-complete.json"
    if any(not path.is_file() or path.is_symlink() for path in (manifest_path, marker_path)):
        raise SnapshotError("normalized snapshot requires manifest and completion marker")
    try:
        manifest, marker = json.loads(manifest_path.read_text()), json.loads(marker_path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise SnapshotError("normalized manifest or completion marker is invalid JSON") from error
    if not isinstance(manifest, dict) or not isinstance(marker, dict):
        raise SnapshotError("normalized manifest or completion marker is not an object")
    manifest_sha, _ = _sha_file(manifest_path)
    inventory = _sha_value(manifest.get("raw_source_inventory", {}).get("canonical_sha256"), "source inventory")
    if marker.get("events_manifest_sha256") != manifest_sha or marker.get("raw_source_inventory_sha256") != inventory:
        raise SnapshotError("completion marker is not bound to native manifest and source inventory")
    return dict(manifest_sha256=manifest_sha, source_inventory_sha256=inventory, root=root), manifest


def local_snapshot(normalized: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    local, manifest = _local_binding(normalized)
    root = local["root"]
    payload = _expected_payload(manifest, root)
    allowed = set(payload) | {Path("events_manifest.json"), Path(".polyedge-daily-complete.json")}
    actual = set(_files(root))
    if actual != allowed:
        raise SnapshotError("normalized directory contains files outside the native derived inventory")
    rows = []
    for relative in [*payload, Path("events_manifest.json"), Path(".polyedge-daily-complete.json")]:
        sha, size = _sha_file(root / relative)
        rows.append(dict(path=relative.as_posix(), sha256=sha, size=size))
    return local, rows


def _bytes(response: Any) -> Iterable[bytes]:
    data = getattr(response, "data", response)
    if isinstance(data, bytes):
        yield data
        return
    raw = getattr(data, "raw", None)
    stream = getattr(raw, "stream", None) or getattr(data, "stream", None)
    if stream:
        yield from stream(CHUNK)
        return
    read = getattr(data, "read", None)
    if not read:
        raise SnapshotError("OCI response has no readable body")
    while block := read(CHUNK):
        yield block


def _readback(client: Any, key: str, expected_sha: str, expected_size: int, sink=None) -> str:
    response = client.get_object(NAMESPACE, BUCKET, key)
    headers = {str(k).lower(): str(v) for k, v in (getattr(response, "headers", None) or {}).items()}
    etag = headers.get("etag")
    if not etag:
        raise SnapshotError(f"OCI object readback omitted ETag: {key}")
    digest, size = hashlib.sha256(), 0
    for block in _bytes(response):
        if not isinstance(block, bytes):
            block = bytes(block)
        digest.update(block); size += len(block)
        if sink is not None:
            sink.write(block)
    if size != expected_size or "sha256:" + digest.hexdigest() != expected_sha:
        raise SnapshotError(f"OCI object readback hash mismatch: {key}")
    return etag


def _key(manifest_sha: str, file_sha: str) -> str:
    return f"{PREFIX}/{manifest_sha[7:]}/{file_sha[7:]}"


def _put_and_verify(client: Any, root: Path, manifest_sha: str, row: dict[str, Any]) -> dict[str, Any]:
    key = _key(manifest_sha, row["sha256"])
    try:
        with (root / row["path"]).open("rb") as source:
            client.put_object(NAMESPACE, BUCKET, key, source, if_none_match="*",
                              content_length=row["size"],
                              opc_content_sha256=base64.b64encode(bytes.fromhex(row["sha256"][7:])).decode())
    except Exception:
        # A precondition failure is safe only when the independently read back
        # content matches; other errors still fail during the readback.
        pass
    etag = _readback(client, key, row["sha256"], row["size"])
    return dict(**row, key=key, etag=etag)


def _write_receipt(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", delete=False) as out:
            temporary = Path(out.name)
            json.dump(value, out, sort_keys=True, separators=(",", ":")); out.write("\n")
            out.flush(); os.fsync(out.fileno())
        os.link(temporary, path)  # atomic create-only final receipt
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except FileExistsError as error:
        raise SnapshotError("snapshot receipt already exists") from error
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _evict_verified_payload(local: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    for row in rows:
        if row["path"] in ("events_manifest.json", ".polyedge-daily-complete.json"):
            continue
        local_file = local["root"] / row["path"]
        if local_file.is_symlink():
            raise SnapshotError("eviction refuses symbolic link")
        if local_file.exists():
            if not local_file.is_file() or _sha_file(local_file) != (row["sha256"], row["size"]):
                raise SnapshotError("eviction local payload differs from verified receipt")
            local_file.unlink()


def _load_receipt(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise SnapshotError("snapshot receipt is invalid") from error
    if not isinstance(value, dict) or value.get("schema") != SCHEMA or value.get("status") != "verified":
        raise SnapshotError("snapshot receipt schema or status is invalid")
    _sha_value(value.get("normalized_manifest_sha256"), "receipt manifest")
    _sha_value(value.get("source_inventory_sha256"), "receipt source inventory")
    if not isinstance(value.get("normalized_path"), str) or not Path(value["normalized_path"]).is_absolute():
        raise SnapshotError("snapshot receipt normalized path is invalid")
    files = value.get("files")
    if not isinstance(files, list) or not files:
        raise SnapshotError("snapshot receipt has no files")
    seen = set()
    for row in files:
        if not isinstance(row, dict) or not isinstance(row.get("path"), str) or row["path"] in seen:
            raise SnapshotError("snapshot receipt has duplicate or invalid paths")
        seen.add(row["path"]); _safe_path(row["path"]); _sha_value(row.get("sha256"), "receipt file")
        if not isinstance(row.get("size"), int) or row["size"] < 0 or not isinstance(row.get("etag"), str):
            raise SnapshotError("snapshot receipt has invalid file metadata")
        if row.get("key") != _key(value["normalized_manifest_sha256"], row["sha256"]):
            raise SnapshotError("snapshot receipt object key is invalid")
    return value


def publish(client: Any, normalized: Path, receipt: Path, evict: bool = False) -> dict[str, Any]:
    if receipt.exists():
        local, manifest = _local_binding(normalized)
        existing = _load_receipt(receipt)
        if Path(existing["normalized_path"]) != local["root"]:
            raise SnapshotError("existing receipt belongs to a different normalized directory")
        if (existing["normalized_manifest_sha256"] != local["manifest_sha256"]
                or existing["source_inventory_sha256"] != local["source_inventory_sha256"]):
            raise SnapshotError("existing receipt is not bound to local normalized evidence")
        expected_paths = {path.as_posix() for path in _expected_payload(manifest, local["root"])} | {"events_manifest.json", ".polyedge-daily-complete.json"}
        if {row["path"] for row in existing["files"]} != expected_paths:
            raise SnapshotError("existing receipt file inventory differs from local normalized evidence")
        for row in existing["files"]:
            local_file = local["root"] / row["path"]
            if local_file.exists():
                if local_file.is_symlink() or not local_file.is_file():
                    raise SnapshotError("existing local normalized file is unsafe")
                local_sha, local_size = _sha_file(local_file)
                if row["sha256"] != local_sha or row["size"] != local_size:
                    raise SnapshotError("existing receipt metadata differs from local normalized evidence")
            _readback(client, row["key"], row["sha256"], row["size"])
        if evict:
            _evict_verified_payload(local, existing["files"])
        return existing
    local, rows = local_snapshot(normalized)
    verified = [_put_and_verify(client, local["root"], local["manifest_sha256"], row) for row in rows]
    result = dict(schema=SCHEMA, schema_version=1, status="verified", namespace=NAMESPACE,
                  region=REGION, bucket=BUCKET, normalized_manifest_sha256=local["manifest_sha256"],
                  source_inventory_sha256=local["source_inventory_sha256"],
                  normalized_path=str(local["root"]), files=verified)
    _write_receipt(receipt, result)
    if evict:
        _evict_verified_payload(local, verified)
    return result


def restore(client: Any, normalized: Path, receipt: Path) -> dict[str, Any]:
    value = _load_receipt(receipt)
    if normalized.is_symlink():
        raise SnapshotError("restore destination is a symbolic link")
    root = normalized.resolve()
    if root != Path(value["normalized_path"]):
        raise SnapshotError("restore destination differs from receipt normalized directory")
    root.mkdir(parents=True, exist_ok=True)
    receipt_paths = {Path(row["path"]) for row in value["files"]}
    if set(_files(root)) - receipt_paths:
        raise SnapshotError("restore destination contains files outside the receipt inventory")
    temp = Path(tempfile.mkdtemp(prefix=".primary-normalized-restore-", dir=root.parent))
    try:
        for row in value["files"]:
            relative, target = _safe_path(row["path"]), root / _safe_path(row["path"])
            if target.exists():
                if target.is_symlink() or not target.is_file() or _sha_file(target) != (row["sha256"], row["size"]):
                    raise SnapshotError(f"restore refuses conflicting local file: {relative}")
                _readback(client, row["key"], row["sha256"], row["size"])
                continue
            staged = temp / relative; staged.parent.mkdir(parents=True, exist_ok=True)
            with staged.open("xb") as out:
                _readback(client, row["key"], row["sha256"], row["size"], out)
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(staged, target)  # atomic creation; never overwrites a raced local file
            except FileExistsError as error:
                raise SnapshotError(f"restore refuses raced local file: {relative}") from error
        if set(_files(root)) != receipt_paths:
            raise SnapshotError("restore did not produce the exact receipt inventory")
    finally:
        shutil.rmtree(temp, ignore_errors=True)
    return value


def _client():
    import oci
    return oci.object_storage.ObjectStorageClient({"region": REGION}, signer=oci.auth.signers.InstancePrincipalsSecurityTokenSigner())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in (commands.add_parser("publish"), commands.add_parser("restore")):
        command.add_argument("--normalized", required=True, type=Path)
        command.add_argument("--receipt", required=True, type=Path)
    commands.choices["publish"].add_argument("--evict", action="store_true")
    args = parser.parse_args()
    result = publish(_client(), args.normalized, args.receipt, args.evict) if args.command == "publish" else restore(_client(), args.normalized, args.receipt)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except SnapshotError as error:
        raise SystemExit(f"primary normalized snapshot: {error}")
