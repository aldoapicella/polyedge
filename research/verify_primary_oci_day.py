#!/usr/bin/env python3
"""Verify one OCI archived day using manifests and object metadata only."""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import re
import sys
import uuid
from pathlib import Path
from typing import Any

NAMESPACE = "axl4ryaas895"
REGION = "sa-bogota-1"
BUCKET = "bot-events"
PREFIX = "events-oci-hot7-v1"
SEGMENTS = 24 * 6
STEP = 600
SHA_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
LOCAL_RE = re.compile(r"(?:^|/)" r"(\d{4})/(\d{2})/(\d{2})/(\d{2})/(\d+)\.jsonl$")


class ProofError(Exception):
    pass


def sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def raw_inventory_hash(canonical: dict[str, Any]) -> str:
    # Match serde_json's struct-field order used by RawSourceInventoryCanonical
    # and RawSourceBlobBinding. The native digest is over these bytes.
    fields = ("domain", "schema_version", "source_kind", "account", "container", "prefix", "max_blobs", "max_bytes", "ordering", "exhaustive_listing", "blob_count", "total_bytes", "blobs")
    blob_fields = ("ordinal", "name", "etag", "version_id", "content_md5", "blob_type", "sealed", "content_length", "last_modified", "sha256")
    try:
        ordered = {key: canonical[key] for key in fields}
        ordered["blobs"] = [{key: blob[key] for key in blob_fields} for blob in canonical["blobs"]]
    except (KeyError, TypeError):
        raise ProofError("native raw inventory canonical fields are incomplete") from None
    return sha256(canonical_json(ordered))


def _body(response: Any) -> bytes:
    data = getattr(response, "data", response)
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return data.encode()
    content = getattr(data, "content", None)
    if isinstance(content, bytes):
        return content
    read = getattr(data, "read", None)
    if read is None:
        raise ProofError("OCI manifest response has no readable body")
    value = read()
    return value if isinstance(value, bytes) else bytes(value)


def _headers(response: Any) -> dict[str, str]:
    headers = getattr(response, "headers", None) or {}
    return {str(k).lower(): str(v) for k, v in headers.items()}


def _page_objects(response: Any) -> tuple[list[Any], str | None]:
    data = getattr(response, "data", response)
    objects = getattr(data, "objects", None)
    if objects is None and isinstance(data, dict):
        objects = data.get("objects", [])
    next_start = getattr(data, "next_start_with", None)
    if next_start is None and isinstance(data, dict):
        next_start = data.get("next_start_with")
    return list(objects or []), next_start


def _obj_name(obj: Any) -> str:
    name = getattr(obj, "name", None)
    if name is None and isinstance(obj, dict):
        name = obj.get("name")
    return str(name or "")


def _object_sha(headers: dict[str, str], label: str) -> str:
    value = headers.get("opc-content-sha256")
    if not value:
        raise ProofError(f"{label} is missing opc-content-sha256")
    try:
        digest = "sha256:" + base64.b64decode(value, validate=True).hex()
    except (ValueError, UnicodeError):
        raise ProofError(f"{label} has invalid opc-content-sha256") from None
    if not SHA_RE.fullmatch(digest):
        raise ProofError(f"{label} has invalid content hash")
    return digest


def _head(client: Any, name: str, list_etag: str | None = None) -> dict[str, str]:
    try:
        response = client.head_object(NAMESPACE, BUCKET, name, if_match=list_etag)
        headers = _headers(response)
        if list_etag and headers.get("etag") != list_etag:
            raise ProofError(f"OCI HEAD etag changed for {name}")
        return headers
    except Exception as exc:
        raise ProofError(f"OCI HEAD failed for {name}") from None


def _get_manifest(client: Any, name: str, list_etag: str | None = None) -> bytes:
    try:
        response = client.get_object(NAMESPACE, BUCKET, name, if_match=list_etag)
        return _body(response)
    except Exception:
        raise ProofError(f"OCI manifest GET failed for {name}") from None


def _list_all(client: Any, prefix: str) -> list[dict[str, str | None]]:
    listed: list[dict[str, str | None]] = []
    start = None
    while True:
        kwargs = {"namespace_name": NAMESPACE, "bucket_name": BUCKET, "prefix": prefix, "fields": "name,size,etag,timeModified"}
        if start:
            kwargs["start"] = start
        try:
            response = client.list_objects(**kwargs)
        except TypeError:
            # The fake and older SDK clients may expose positional arguments.
            try:
                response = client.list_objects(NAMESPACE, BUCKET, prefix=prefix, start=start)
            except Exception:
                raise ProofError("OCI object listing failed") from None
        except Exception:
            raise ProofError("OCI object listing failed") from None
        page_objects, next_start = _page_objects(response)
        for obj in page_objects:
            name = _obj_name(obj)
            etag = getattr(obj, "etag", None)
            if etag is None and isinstance(obj, dict):
                etag = obj.get("etag")
            listed.append({"name": name, "etag": str(etag) if etag else None})
        if not next_start:
            return listed
        if next_start == start:
            raise ProofError("OCI object listing pagination repeated a cursor")
        start = next_start


def _date_epoch(date: str) -> int:
    try:
        day = dt.date.fromisoformat(date)
    except ValueError:
        raise ProofError("date must be YYYY-MM-DD") from None
    today = dt.datetime.now(dt.timezone.utc).date()
    if day >= today:
        raise ProofError("open or future UTC day is not verifiable")
    return int(dt.datetime.combine(day, dt.time(), tzinfo=dt.timezone.utc).timestamp())


def _local_sources(manifest: dict[str, Any], date: str) -> tuple[dict[int, dict[str, Any]], str]:
    inventory = manifest.get("raw_source_inventory") or {}
    canonical = inventory.get("canonical") or {}
    if canonical.get("source_kind") != "local_files":
        raise ProofError("normalized manifest must use native local_files inventory")
    if not canonical.get("exhaustive_listing") or canonical.get("max_blobs") is not None or canonical.get("max_bytes") is not None:
        raise ProofError("native source inventory must be exhaustive and unbounded")
    blobs = canonical.get("blobs") or []
    if len(blobs) != SEGMENTS or canonical.get("blob_count") != SEGMENTS:
        raise ProofError("native source inventory must contain exactly 144 files")
    expected: dict[int, dict[str, Any]] = {}
    for ordinal, blob in enumerate(blobs):
        if blob.get("ordinal") != ordinal or blob.get("sealed") is not True:
            raise ProofError("native source file inventory is unsealed or unordered")
        match = LOCAL_RE.search(str(blob.get("name", "")))
        if not match or "sha256:" not in str(blob.get("sha256", "")):
            raise ProofError("native source file name or hash is invalid")
        y, month, day, hour, epoch_text = match.groups()
        if f"{y}-{month}-{day}" != date or not 0 <= int(hour) <= 23:
            raise ProofError("native source file is outside requested UTC day")
        epoch = int(epoch_text)
        if int(hour) != dt.datetime.fromtimestamp(epoch, dt.timezone.utc).hour:
            raise ProofError("native source hour does not match segment epoch")
        if epoch in expected:
            raise ProofError("duplicate native source segment")
        expected[epoch] = blob
    if sorted(expected) != [ _date_epoch(date) + i * STEP for i in range(SEGMENTS) ]:
        raise ProofError("native source segments are missing or not 10-minute contiguous")
    expected_hash = raw_inventory_hash(canonical)
    if inventory.get("canonical_sha256") != expected_hash:
        raise ProofError("native raw inventory canonical hash mismatch")
    return expected, inventory["canonical_sha256"]


def verify(client: Any, date: str, normalized_manifest: Path) -> dict[str, Any]:
    start_epoch = _date_epoch(date)
    manifest = json.loads(normalized_manifest.read_text())
    sources, inventory_hash = _local_sources(manifest, date)
    prefix = f"{PREFIX}/{date.replace('-', '/')}/"
    listed = _list_all(client, prefix)
    expected_names = {
        f"{prefix}{dt.datetime.fromtimestamp(start_epoch + i * STEP, dt.timezone.utc):%H}/{start_epoch + i * STEP}.jsonl.gz{suffix}"
        for i in range(SEGMENTS) for suffix in ("", ".manifest.json")
    }
    listed_names = [item["name"] for item in listed]
    if len(listed_names) != len(set(listed_names)) or set(listed_names) != expected_names:
        raise ProofError("OCI listing has missing, duplicate, or unknown objects")
    listed_by_name = {item["name"]: item for item in listed}
    if any(not item.get("etag") for item in listed):
        raise ProofError("OCI listing is missing an object ETag")
    rows: list[dict[str, Any]] = []
    head_receipts: list[dict[str, Any]] = []
    prior_instance = None
    prior_last = None
    for i in range(SEGMENTS):
        epoch = start_epoch + i * STEP
        hour = dt.datetime.fromtimestamp(epoch, dt.timezone.utc).strftime("%H")
        blob = f"{prefix}{hour}/{epoch}.jsonl.gz"
        manifest_name = blob + ".manifest.json"
        body = _get_manifest(client, manifest_name, listed_by_name[manifest_name].get("etag"))
        try:
            remote = json.loads(body)
        except Exception:
            raise ProofError(f"OCI manifest JSON invalid for {blob}") from None
        source = sources[epoch]
        if remote.get("schema_version") != 4 or remote.get("compression") != "gzip":
            raise ProofError(f"unsupported OCI manifest schema for {blob}")
        if remote.get("blob_name") != blob or remote.get("segment_start_epoch") != epoch or remote.get("segment_end_epoch") != epoch + STEP:
            raise ProofError(f"OCI source binding mismatch for {blob}")
        if remote.get("source_sha256") != source.get("sha256") or remote.get("source_bytes") != source.get("content_length"):
            raise ProofError(f"OCI native source hash or byte binding mismatch for {blob}")
        if not isinstance(remote.get("lines"), int) or remote["lines"] < 0:
            raise ProofError(f"OCI manifest lines invalid for {blob}")
        runs = remote.get("recorder_runs")
        if not isinstance(runs, list) or not runs:
            raise ProofError(f"OCI recorder continuity missing for {blob}")
        run_lines = 0
        for run in runs:
            first = run.get("recorder_first_sequence")
            last = run.get("recorder_last_sequence")
            count = run.get("recorder_event_count")
            if any(type(value) is not int for value in (first, last, count)) or first < 0 or count <= 0 or last - first + 1 != count:
                raise ProofError(f"OCI recorder range mismatch for {blob}")
            try:
                uuid.UUID(run.get("recorder_instance_id", ""))
            except (ValueError, TypeError, AttributeError):
                raise ProofError(f"OCI recorder identity invalid for {blob}") from None
            if prior_instance is not None and run.get("recorder_instance_id") != prior_instance:
                raise ProofError("OCI recorder instance changed within day")
            if prior_last is not None and first != prior_last + 1:
                raise ProofError("OCI recorder sequence gap within day")
            prior_instance = run.get("recorder_instance_id")
            prior_last = last
            run_lines += count
        if run_lines != remote["lines"]:
            raise ProofError(f"OCI recorder lines mismatch for {blob}")
        compressed_head = _head(client, blob, listed_by_name[blob].get("etag"))
        manifest_head = _head(client, manifest_name, listed_by_name[manifest_name].get("etag"))
        compressed_sha = _object_sha(compressed_head, blob)
        manifest_sha = _object_sha(manifest_head, manifest_name)
        if compressed_sha != remote.get("sha256") or manifest_sha != sha256(body):
            raise ProofError(f"OCI HEAD hash mismatch for {blob}")
        if int(compressed_head.get("content-length", -1)) != remote.get("bytes") or int(manifest_head.get("content-length", -1)) != len(body):
            raise ProofError(f"OCI HEAD length mismatch for {blob}")
        head_receipts.extend([
            {"name": blob, "etag": compressed_head.get("etag"), "content_length": int(compressed_head["content-length"]), "opc_content_sha256": compressed_sha},
            {"name": manifest_name, "etag": manifest_head.get("etag"), "content_length": int(manifest_head["content-length"]), "opc_content_sha256": manifest_sha},
        ])
        rows.append({"blob": blob, "manifest": manifest_name, "manifest_sha256": manifest_sha, "compressed_sha256": compressed_sha, "source_sha256": source["sha256"], "source_bytes": source["content_length"], "segment_start_epoch": epoch, "segment_end_epoch": epoch + STEP, "lines": remote["lines"], "recorder_runs": runs, "blob_etag": compressed_head.get("etag"), "manifest_etag": manifest_head.get("etag")})
    normalized_hash = sha256(normalized_manifest.read_bytes())
    return {
        "schema": "polyedge.primary_oci_day_proof.v1",
        "status": "passed",
        "verified": True,
        "date": date,
        "normalized_manifest_sha256": normalized_hash,
        "native_manifest_sha256": normalized_hash,
        "source_inventory_sha256": inventory_hash,
        "raw_inventory_sha256": inventory_hash,
        "source": {"provider": "oci_object_storage", "namespace": NAMESPACE, "region": REGION, "bucket": BUCKET, "prefix": PREFIX + "/", "listing_prefix": prefix, "runtime_role": "primary", "execution_mode": "paper", "shadow_only": False, "market_payloads_read": False},
        "exhaustive_listing": True,
        "listed_objects": sorted(listed_names),
        "head_receipts": head_receipts,
        "segments": rows,
        "segments_verified": len(rows),
        "source_admitted": False,
        "promotion_eligible": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--normalized-manifest", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    try:
        import oci  # type: ignore
        client = oci.object_storage.ObjectStorageClient({"region": REGION}, signer=oci.auth.signers.InstancePrincipalsSecurityTokenSigner())
        proof = verify(client, args.date, args.normalized_manifest)
        if args.out.exists():
            if json.loads(args.out.read_text()) != proof:
                raise ProofError("existing proof differs; refusing overwrite")
            return 0
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("x") as stream:
            stream.write(json.dumps(proof, indent=2) + "\n")
        return 0
    except ProofError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print("error: OCI proof failed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
