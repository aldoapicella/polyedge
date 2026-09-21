#!/usr/bin/env python3
"""Unit tests for the OCI normalized snapshot helper; no OCI credentials used."""
import hashlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import primary_normalized_snapshot as snapshot


class Response:
    def __init__(self, body, etag="etag"):
        self.data, self.headers = io.BytesIO(body), {"etag": etag}


class Client:
    def __init__(self):
        self.objects, self.bad_readback = {}, False

    def put_object(self, namespace, bucket, key, body, if_none_match=None, **kwargs):
        if key in self.objects and if_none_match == "*":
            raise RuntimeError("already exists")
        self.objects[key] = body.read()

    def get_object(self, namespace, bucket, key):
        body = self.objects[key]
        if self.bad_readback:
            body = b"changed" + body
        return Response(body, "etag-" + hashlib.sha256(body).hexdigest()[:8])


def digest(body):
    return "sha256:" + hashlib.sha256(body).hexdigest()


def fixture(root):
    payload = {}
    paths = {
        "runtime_provenance": "runtime_provenance.jsonl", "market": "markets.jsonl",
        "reference": "references.jsonl", "book": "books.jsonl", "fair_value": "fair_values.jsonl",
        "decision": "decisions.jsonl", "execution_report": "execution_reports.jsonl",
        "paper_settlement": "paper_settlements.jsonl", "feed_error": "feed_errors.jsonl",
        "raw_market_event": "raw_market_events.jsonl", "price_change": "price_changes.jsonl",
        "last_trade": "last_trades.jsonl", "book_snapshot": "book_snapshots.jsonl",
        "level_change": "level_changes.jsonl", "other": "other.jsonl",
    }
    for kind, name in paths.items():
        name += ".gz"
        (root / name).write_bytes((kind + "\n").encode())
        payload[kind] = {"path": "data/research/daily/2026-09-22/normalized/" + name, "rows": 1}
    payload["events"] = None
    inventory = "sha256:" + "a" * 64
    manifest = {"format": "jsonl-indexed-gzip-sharded", "files": payload,
                "raw_source_inventory": {"canonical_sha256": inventory}}
    manifest_path = root / "events_manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    marker = {"events_manifest_sha256": digest(manifest_path.read_bytes()),
              "raw_source_inventory_sha256": inventory}
    (root / ".polyedge-daily-complete.json").write_text(json.dumps(marker))


class SnapshotTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "normalized"; self.root.mkdir(); fixture(self.root)
        self.receipt = Path(self.tmp.name) / "receipt.json"; self.client = Client()

    def test_roundtrip_evict_and_idempotent_retry(self):
        result = snapshot.publish(self.client, self.root, self.receipt, evict=True)
        self.assertEqual(result["status"], "verified")
        self.assertTrue((self.root / "events_manifest.json").exists())
        self.assertTrue((self.root / ".polyedge-daily-complete.json").exists())
        self.assertFalse((self.root / "markets.jsonl.gz").exists())
        self.assertEqual(snapshot.publish(self.client, self.root, self.receipt, evict=True), result)
        snapshot.restore(self.client, self.root, self.receipt)
        self.assertEqual((self.root / "markets.jsonl.gz").read_bytes(), b"market\n")

    def test_restore_rejects_different_normalized_directory(self):
        snapshot.publish(self.client, self.root, self.receipt)
        with self.assertRaises(snapshot.SnapshotError):
            snapshot.restore(self.client, Path(self.tmp.name) / "other-normalized", self.receipt)

    def test_changed_manifest_payload_path_is_rejected(self):
        manifest = json.loads((self.root / "events_manifest.json").read_text())
        manifest["files"]["market"]["path"] = "/tmp/not-normalized"
        (self.root / "events_manifest.json").write_text(json.dumps(manifest))
        with self.assertRaises(snapshot.SnapshotError):
            snapshot.publish(self.client, self.root, self.receipt)

    def test_failed_readback_never_evicts_payload(self):
        self.client.bad_readback = True
        with self.assertRaises(snapshot.SnapshotError):
            snapshot.publish(self.client, self.root, self.receipt, evict=True)
        self.assertTrue((self.root / "markets.jsonl.gz").exists())
        self.assertFalse(self.receipt.exists())

    def test_existing_different_object_is_rejected(self):
        local, rows = snapshot.local_snapshot(self.root)
        row = rows[0]
        self.client.objects[snapshot._key(local["manifest_sha256"], row["sha256"])] = b"wrong"
        with self.assertRaises(snapshot.SnapshotError):
            snapshot.publish(self.client, self.root, self.receipt)

    def test_existing_receipt_rejects_changed_local_payload(self):
        snapshot.publish(self.client, self.root, self.receipt)
        (self.root / "markets.jsonl.gz").write_bytes(b"changed local payload")
        with self.assertRaises(snapshot.SnapshotError):
            snapshot.publish(self.client, self.root, self.receipt)

    def test_partial_receipt_is_fail_closed_and_not_overwritten(self):
        self.receipt.write_text("{")
        with self.assertRaises(snapshot.SnapshotError):
            snapshot.publish(self.client, self.root, self.receipt)
        self.assertEqual(self.receipt.read_text(), "{")


if __name__ == "__main__":
    unittest.main()
