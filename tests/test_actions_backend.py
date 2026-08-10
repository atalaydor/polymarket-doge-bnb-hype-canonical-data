from __future__ import annotations

import unittest

from scripts.actions_backend import (
    RemoteAsset,
    matrix_stages,
    select_proof_partition,
    unfinished_plan,
    verified_partitions,
)


class ActionsBackendTests(unittest.TestCase):
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
