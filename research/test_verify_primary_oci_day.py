import base64
import datetime as dt
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import verify_primary_oci_day as proof


class FakeClient:
    def __init__(self, objects, manifests, bad_head=None):
        self.objects = objects
        self.manifests = manifests
        self.bad_head = bad_head

    def list_objects(self, **kwargs):
        names = sorted(n for n in self.objects if n.startswith(kwargs["prefix"]))
        start = kwargs.get("start")
        if start:
            names = [n for n in names if n >= start]
        page, rest = names[:100], names[100:]
        return SimpleNamespace(data=SimpleNamespace(objects=[SimpleNamespace(name=n, etag="etag-" + n[-8:]) for n in page], next_start_with=rest[0] if rest else None))

    def get_object(self, namespace, bucket, name, if_match=None):
        if if_match != "etag-" + name[-8:]:
            raise RuntimeError("etag precondition")
        return SimpleNamespace(data=self.manifests[name])

    def head_object(self, namespace, bucket, name, if_match=None):
        body = self.objects[name]
        if name == self.bad_head:
            body = b"bad"
        etag = "etag-" + name[-8:]
        if if_match != etag:
            raise RuntimeError("etag precondition")
        return SimpleNamespace(headers={"etag": etag, "content-length": str(len(self.objects[name])), "opc-content-sha256": base64.b64encode(hashlib.sha256(body).digest()).decode()})


def build_fixture(date="2026-09-18"):
    day = dt.date.fromisoformat(date)
    start = int(dt.datetime.combine(day, dt.time(), tzinfo=dt.timezone.utc).timestamp())
    blobs = []
    objects, manifests = {}, {}
    sequence = 1000
    prefix = f"{proof.PREFIX}/{date.replace('-', '/')}/"
    for i in range(proof.SEGMENTS):
        epoch = start + i * proof.STEP
        hour = dt.datetime.fromtimestamp(epoch, dt.timezone.utc).strftime("%H")
        local_name = f"/input/events/{date.replace('-', '/')}/{hour}/{epoch}.jsonl"
        raw = f"raw-{i}".encode()
        blobs.append({"ordinal": i, "name": local_name, "etag": None, "version_id": None, "content_md5": None, "blob_type": "LocalFile", "sealed": True, "content_length": len(raw), "last_modified": None, "sha256": proof.sha256(raw)})
        name = f"{prefix}{hour}/{epoch}.jsonl.gz"
        manifest_name = name + ".manifest.json"
        compressed = f"compressed-{i}".encode()
        run = {"recorder_instance_id": "284b7f0f-b08a-45c9-a9f9-aac8aa75940e", "recorder_first_sequence": sequence, "recorder_last_sequence": sequence, "recorder_event_count": 1}
        remote = {"schema_version": 4, "compression": "gzip", "blob_name": name, "sha256": proof.sha256(compressed), "source_sha256": proof.sha256(raw), "bytes": len(compressed), "source_bytes": len(raw), "lines": 1, "segment_start_epoch": epoch, "segment_end_epoch": epoch + proof.STEP, "recorder_runs": [run]}
        body = json.dumps(remote, separators=(",", ":")).encode()
        objects[name] = compressed; objects[manifest_name] = body; manifests[manifest_name] = body
        sequence += 1
    canonical = {"domain": "polyedge.raw-source-inventory.v1", "schema_version": 1, "source_kind": "local_files", "account": None, "container": None, "prefix": "/input/events/" + date.replace('-', '/') + "/", "max_blobs": None, "max_bytes": None, "ordering": "blob_name_ascii_ascending", "exhaustive_listing": True, "blob_count": proof.SEGMENTS, "total_bytes": sum(x["content_length"] for x in blobs), "blobs": blobs}
    manifest = {"raw_source_inventory": {"schema_version": 1, "canonical_sha256": proof.raw_inventory_hash(canonical), "canonical": canonical}}
    return manifest, objects, manifests


class VerifyPrimaryOciDayTest(unittest.TestCase):
    def run_proof(self, client, manifest, date="2026-09-18"):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "manifest.json"; path.write_text(json.dumps(manifest))
            return proof.verify(client, date, path)

    def test_valid_metadata_only_proof(self):
        manifest, objects, remote = build_fixture()
        result = self.run_proof(FakeClient(objects, remote), manifest)
        self.assertEqual(result["segments_verified"], 144)
        self.assertFalse(result["source"]["market_payloads_read"])
        self.assertFalse(result["source_admitted"])

    def test_rejects_missing_unknown_bad_hash_and_sequence_gap(self):
        manifest, objects, remote = build_fixture()
        for mutate in ("missing", "unknown", "hash", "gap"):
            with self.subTest(mutate=mutate):
                obj = dict(objects); rem = dict(remote)
                if mutate == "missing": obj.pop(next(iter(obj)))
                if mutate == "unknown": obj["events-oci-hot7-v1/2026/09/18/00/9999999999.jsonl.gz"] = b"x"
                if mutate == "hash": client = FakeClient(obj, rem, next(iter(obj)))
                else: client = FakeClient(obj, rem)
                if mutate == "gap":
                    key = sorted(rem)[0]; value = json.loads(rem[key]); value["recorder_runs"][0]["recorder_first_sequence"] += 2; rem[key] = json.dumps(value).encode(); client = FakeClient(obj, rem)
                with self.assertRaises(proof.ProofError): self.run_proof(client, manifest)

    def test_rejects_open_day_and_source_binding_mismatch(self):
        manifest, objects, remote = build_fixture()
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "manifest.json"; path.write_text(json.dumps(manifest))
            with self.assertRaises(proof.ProofError): proof.verify(FakeClient(objects, remote), dt.datetime.now(dt.timezone.utc).date().isoformat(), path)
        key = sorted(remote)[0]; value = json.loads(remote[key]); value["source_sha256"] = proof.sha256(b"wrong"); remote[key] = json.dumps(value).encode()
        with self.assertRaises(proof.ProofError): self.run_proof(FakeClient(objects, remote), manifest)


if __name__ == "__main__": unittest.main()
