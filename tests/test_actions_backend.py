from __future__ import annotations

import hashlib
import unittest
from email.message import Message
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts.actions_backend import (
    KACHO_REVISION,
    KACHO_TICKS,
    RemoteAsset,
    acquire_kacho,
    matrix_stages,
    select_proof_partition,
    unfinished_plan,
    verified_partitions,
)


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0
        self.status = 200
        self.headers = Message()
        self.headers["Content-Type"] = "application/octet-stream"
        self.headers["Content-Length"] = str(len(payload))
        self.headers["ETag"] = '"object-etag"'

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self.payload) - self.offset
        chunk = self.payload[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk

    def geturl(self) -> str:
        return "https://cdn-lfs.example.test/object?credential=secret"


class ActionsBackendTests(unittest.TestCase):
    def test_kacho_accepts_only_pinned_object_at_immutable_revision(self) -> None:
        payload = b"pinned-parquet-object"
        digest = hashlib.sha256(payload).hexdigest()
        response = FakeResponse(payload)
        with TemporaryDirectory() as temporary:
            with (
                patch.dict(
                    KACHO_TICKS,
                    {"DOGE": ("doge_ticks.parquet", f"{digest}.source", digest, len(payload))},
                ),
                patch(
                    "scripts.actions_backend.urllib.request.urlopen", return_value=response
                ) as urlopen,
            ):
                self.assertEqual(acquire_kacho("DOGE", Path(temporary)), len(payload))
                requested = str(urlopen.call_args.args[0].full_url)
                self.assertIn(f"/resolve/{KACHO_REVISION}/doge_ticks.parquet", requested)
                self.assertEqual((Path(temporary) / f"{digest}.source").read_bytes(), payload)

    def test_kacho_rejects_wrong_object_with_bounded_safe_diagnostics(self) -> None:
        with TemporaryDirectory() as temporary:
            with patch(
                "scripts.actions_backend.urllib.request.urlopen",
                return_value=FakeResponse(b"not-the-pinned-parquet"),
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    r'"actual_bytes": 22.*"actual_sha256":.*'
                    r'"response_host": "cdn-lfs.example.test"',
                ) as raised:
                    acquire_kacho("DOGE", Path(temporary))
        self.assertNotIn("credential", str(raised.exception))
        self.assertNotIn("not-the-pinned-parquet", str(raised.exception))

    @staticmethod
    def _assets(partition: str) -> list[RemoteAsset]:
        asset, _, day = partition.split("/")
        return [
            RemoteAsset(
                f"{asset}--5m--{day}--{'a' * 64}--{filename}",
                1,
                "https://example.test/asset",
                "a" * 64,
                filename,
            )
            for filename in (
                "book-200ms.parquet",
                "book-events.parquet",
                "exclusions.parquet",
                "manifest.json",
                "markets.parquet",
                "underlying.parquet",
            )
        ]

    def test_complete_remote_partition_is_omitted_and_partial_fails_closed(self) -> None:
        partition = "DOGE/5m/2026-04-05"
        assets = [
            RemoteAsset(
                f"DOGE--5m--2026-04-05--{'a' * 64}--{filename}",
                1,
                "https://example.test/asset",
                "a" * 64,
                filename,
            )
            for filename in (
                "book-200ms.parquet",
                "book-events.parquet",
                "exclusions.parquet",
                "manifest.json",
                "markets.parquet",
                "underlying.parquet",
            )
        ]
        inventory = {partition: assets}
        self.assertEqual(verified_partitions(inventory), {partition})
        self.assertNotIn(partition, {item["partition_id"] for item in unfinished_plan(inventory)})
        self.assertEqual(verified_partitions({partition: assets[:-1]}), set())

    def test_proof_partition_is_ledger_frontier_with_remote_evidence(self) -> None:
        partition = "HYPE/5m/2026-04-05"
        ledger = {"completed": 3, "last_partition": partition}
        inventory = {partition: self._assets(partition)}
        self.assertEqual(select_proof_partition(ledger, inventory), partition)

    def test_proof_partition_rejects_inconsistent_or_unsupported_ledger_claim(self) -> None:
        partition = "HYPE/5m/2026-04-05"
        with self.assertRaisesRegex(RuntimeError, "frontier is inconsistent"):
            select_proof_partition(
                {"completed": 3, "last_partition": "DOGE/5m/2026-04-05"},
                {partition: self._assets(partition)},
            )
        with self.assertRaisesRegex(RuntimeError, "lacks durable remote evidence"):
            select_proof_partition({"completed": 3, "last_partition": partition}, {})

    def test_matrices_are_finite_unique_and_bounded(self) -> None:
        plan = unfinished_plan({})
        stages = matrix_stages(plan)
        flattened = [item["partition_id"] for stage in stages for item in stage["include"]]
        self.assertEqual(len(flattened), 375)
        self.assertEqual(len(set(flattened)), 375)
        self.assertTrue(all(len(stage["include"]) <= 256 for stage in stages))
        self.assertEqual([len(stage["include"]) for stage in stages], [256, 119])
