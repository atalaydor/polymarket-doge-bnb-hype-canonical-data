from __future__ import annotations

import unittest

from scripts.actions_backend import RemoteAsset, matrix_stages, unfinished_plan, verified_partitions


class ActionsBackendTests(unittest.TestCase):
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

    def test_matrices_are_finite_unique_and_bounded(self) -> None:
        plan = unfinished_plan({})
        stages = matrix_stages(plan)
        flattened = [item["partition_id"] for stage in stages for item in stage["include"]]
        self.assertEqual(len(flattened), 375)
        self.assertEqual(len(set(flattened)), 375)
        self.assertTrue(all(len(stage["include"]) <= 256 for stage in stages))
        self.assertEqual([len(stage["include"]) for stage in stages], [256, 119])
