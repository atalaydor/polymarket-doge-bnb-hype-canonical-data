from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq
from helpers import market, pmxt_rows, provenance

from canonical_data.errors import ConflictError, PipelineError, ResourceLimitError
from canonical_data.manifest import (
    build_manifest,
    build_notice,
    build_release_index,
    verify_manifest,
)
from canonical_data.models import (
    Asset,
    Exclusion,
    ExclusionReason,
    QualityTier,
)
from canonical_data.parquetio import write_partition_tables
from canonical_data.pipeline import PartitionInputs, Pipeline, PipelineLimits
from canonical_data.pmxt import BookReconstructor, decode_rows
from canonical_data.release import (
    DirectoryReleaseBackend,
    Publisher,
    download_and_verify_release,
)
from canonical_data.resample import resample_200ms
from canonical_data.state import Checkpoint, Phase, StateStore

COMMIT = "d" * 40


class ParquetManifestTests(unittest.TestCase):
    def test_release_index_and_notice_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            counts = write_partition_tables(root / "p", [market()], [], [], [], [])
            build_manifest(
                root / "p",
                Asset.DOGE,
                "2026-04-13",
                QualityTier.TIER_A,
                [provenance()],
                [],
                COMMIT,
                {"row_counts": counts},
                market().market_end_ns,
            )
            index_digest = build_release_index(
                root / "index.json",
                "v1",
                market().market_end_ns,
                [root / "p" / "manifest.json"],
            )
            config = {
                "sources": [
                    {
                        "id": "pmxt_v2",
                        "class": "PRIMARY",
                        "url": "https://example.test",
                        "license": "CC-BY-4.0",
                        "role": "event_book",
                    },
                    {
                        "id": "excluded",
                        "class": "EXCLUDED",
                        "url": "",
                        "license": "none",
                        "role": "none",
                    },
                ]
            }
            notice_digest = build_notice(root / "NOTICE.json", config)
            self.assertEqual(len(index_digest), 64)
            self.assertEqual(len(notice_digest), 64)
            notice = json.loads((root / "NOTICE.json").read_bytes())
            self.assertIsNone(notice["combined_dataset_license"])
            self.assertEqual([item["source_id"] for item in notice["sources"]], ["pmxt_v2"])

    def test_deterministic_parquet_and_manifest(self) -> None:
        events = decode_rows(pmxt_rows(), "fixture")
        samples, _ = resample_200ms(market(), BookReconstructor().reconstruct(events))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            digests = []
            for name in ("one", "two"):
                directory = root / name
                counts = write_partition_tables(directory, [market()], events, samples, [], [])
                _, digest = build_manifest(
                    directory,
                    Asset.DOGE,
                    "2026-04-13",
                    QualityTier.TIER_A,
                    [provenance()],
                    [],
                    COMMIT,
                    {"row_counts": counts},
                    market().market_end_ns,
                )
                self.assertEqual(verify_manifest(directory), digest)
                digests.append(digest)
            self.assertEqual(digests[0], digests[1])
            for filename in (
                "markets.parquet",
                "book-events.parquet",
                "book-200ms.parquet",
                "underlying.parquet",
                "exclusions.parquet",
            ):
                self.assertEqual(
                    (root / "one" / filename).read_bytes(), (root / "two" / filename).read_bytes()
                )

    def test_manifest_detects_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            counts = write_partition_tables(directory, [market()], [], [], [], [])
            build_manifest(
                directory,
                Asset.DOGE,
                "2026-04-13",
                QualityTier.TIER_A,
                [provenance()],
                [],
                COMMIT,
                {"row_counts": counts},
                market().market_end_ns,
            )
            (directory / "markets.parquet").write_bytes(b"substituted")
            with self.assertRaises(ConflictError):
                verify_manifest(directory)

    def test_exclusions_are_canonical_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            exclusion = Exclusion("m", ExclusionReason.SOURCE_GAP, "gap", {"z": 1, "a": 2})
            counts = write_partition_tables(Path(temp), [], [], [], [], [exclusion])
            self.assertEqual(counts["exclusions.parquet"], 1)


class StateTests(unittest.TestCase):
    def test_resume_idempotency_and_conflict_refusal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = StateStore(Path(temp))
            first = Checkpoint("DOGE/5m/2026-04-13", Phase.INVENTORIED, "a")
            self.assertTrue(store.advance(first))
            self.assertFalse(store.advance(first))
            store.advance(Checkpoint(first.partition_id, Phase.VERIFIED, "a", "b"))
            with self.assertRaises(ConflictError):
                store.advance(Checkpoint(first.partition_id, Phase.ACQUIRED, "a"))
            with self.assertRaises(ConflictError):
                store.advance(Checkpoint(first.partition_id, Phase.PUBLISHED, "different", "b"))
            with self.assertRaises(ConflictError):
                store.advance(Checkpoint(first.partition_id, Phase.PUBLISHED, "a", "different"))


class PipelineTests(unittest.TestCase):
    def test_pmxt_failure_can_only_degrade_to_explicit_kacho_tier_b(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT)
            bad_pmxt = (pmxt_rows()[2],)  # increment without any snapshot
            kacho = (
                {
                    "condition_id": market().condition_id,
                    "t": market().market_start_ns // 1_000_000_000,
                    "bu": "0.4",
                    "au": "0.6",
                    "su": "1",
                    "sau": "1",
                    "bd": "0.4",
                    "ad": "0.6",
                    "sd": "1",
                    "sad": "1",
                    "outcome": "Down",
                },
            )
            inputs = PartitionInputs(
                Asset.DOGE,
                "2026-04-13",
                (market(),),
                bad_pmxt,
                "bad-fixture",
                kacho,
                provenance=(provenance("kacho_5m"),),
            )
            built = pipeline.build(inputs, market().market_end_ns)
            self.assertEqual(built.tier, QualityTier.TIER_B)
            events = pq.read_table(built.directory / "book-events.parquet").to_pylist()
            self.assertEqual({item["source_id"] for item in events}, {"kacho_5m"})
            self.assertTrue(all(item["receive_ts_ns"] is None for item in events))
            markets = pq.ParquetFile(built.directory / "markets.parquet").read().to_pylist()
            self.assertEqual(markets[0]["official_outcome"], "UP")

    def test_partition_resource_caps_fail_before_work(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            limits = PipelineLimits(max_markets=0)
            pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT, limits)
            inputs = PartitionInputs(Asset.DOGE, "2026-04-13", (market(),))
            with self.assertRaises(ResourceLimitError):
                pipeline.build(inputs, market().market_end_ns)
            self.assertFalse((root / "state").exists())

    def test_memory_and_transformed_byte_caps_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inputs = PartitionInputs(
                Asset.DOGE,
                "2026-04-13",
                (market(),),
                tuple(pmxt_rows()),
                "fixture",
                provenance=(provenance(),),
            )
            memory_limited = Pipeline(
                root / "memory",
                StateStore(root / "memory-state"),
                COMMIT,
                PipelineLimits(minimum_available_memory_bytes=10**30),
            )
            with self.assertRaises(ResourceLimitError):
                memory_limited.build(inputs, market().market_end_ns)
            byte_limited = Pipeline(
                root / "bytes",
                StateStore(root / "byte-state"),
                COMMIT,
                PipelineLimits(max_partition_bytes=1),
            )
            with self.assertRaises(ResourceLimitError):
                byte_limited.build(inputs, market().market_end_ns)

    def test_tiny_end_to_end_and_exact_noop(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT)
            inputs = PartitionInputs(
                asset=Asset.DOGE,
                day="2026-04-13",
                markets=(market(),),
                pmxt_rows=tuple(pmxt_rows()),
                pmxt_source_object="fixture",
                provenance=(provenance(),),
            )
            built = pipeline.build(inputs, market().market_end_ns)
            again = pipeline.build(inputs, market().market_end_ns)
            self.assertEqual(built.manifest_digest, again.manifest_digest)
            manifest = json.loads((built.directory / "manifest.json").read_bytes())
            self.assertEqual(manifest["quality_tier"], "TIER_A")
            self.assertEqual(manifest["statistics"]["market_count"], 1)
            raw = root / "raw.source"
            raw.write_bytes(b"temporary raw")
            publisher = Publisher(DirectoryReleaseBackend(root / "remote"))
            assets = pipeline.publish(built, publisher, "pilot", [raw])
            self.assertFalse(raw.exists())
            self.assertEqual(len(assets), 6)
            checkpoint = StateStore(root / "state").load(built.partition_id)
            assert checkpoint is not None
            self.assertEqual(checkpoint.phase, Phase.PUBLISHED)

    def test_same_partition_different_identity_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT)
            original = PartitionInputs(
                Asset.DOGE,
                "2026-04-13",
                (market(),),
                tuple(pmxt_rows()),
                "fixture",
                provenance=(provenance(),),
            )
            pipeline.build(original, market().market_end_ns)
            changed = PartitionInputs(
                Asset.DOGE,
                "2026-04-13",
                (market(),),
                tuple(pmxt_rows()),
                "fixture",
                provenance=(provenance("different"),),
            )
            with self.assertRaises(PipelineError):
                pipeline.build(changed, market().market_end_ns)

    def test_pipeline_resumes_after_acquired_or_transformed_checkpoint(self) -> None:
        for interrupted_phase in (Phase.ACQUIRED, Phase.TRANSFORMED):
            with self.subTest(phase=interrupted_phase), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inputs = PartitionInputs(
                    Asset.DOGE,
                    "2026-04-13",
                    (market(),),
                    tuple(pmxt_rows()),
                    "fixture",
                    provenance=(provenance(),),
                )
                pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT)
                identity = pipeline._identity_digest(inputs)
                StateStore(root / "state").advance(
                    Checkpoint("DOGE/5m/2026-04-13", interrupted_phase, identity)
                )
                built = pipeline.build(inputs, market().market_end_ns)
                self.assertEqual(built.checkpoint.phase, Phase.VERIFIED)
                self.assertEqual(verify_manifest(built.directory), built.manifest_digest)

    def test_no_publishable_market_refuses_canonical_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT)
            inputs = PartitionInputs(
                Asset.DOGE, "2026-04-13", (market(),), provenance=(provenance(),)
            )
            with self.assertRaises(PipelineError):
                pipeline.build(inputs, market().market_end_ns)


class FailingOnceBackend(DirectoryReleaseBackend):
    def __init__(self, root: Path):
        super().__init__(root)
        self.fail = True

    def upload(self, release_id: str, name: str, path: Path):  # type: ignore[no-untyped-def]
        if self.fail:
            self.fail = False
            raise OSError("simulated interruption")
        return super().upload(release_id, name, path)


class ReleaseTests(unittest.TestCase):
    def _fixture(self, root: Path) -> Path:
        fixture = root / "partition"
        fixture.mkdir()
        (fixture / "manifest.json").write_text('{"tiny":true}\n')
        (fixture / "book.parquet").write_bytes(b"tiny fixture only")
        return fixture

    def test_draft_roundtrip_idempotency_and_download_verify(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = self._fixture(root)
            backend = DirectoryReleaseBackend(root / "remote")
            publisher = Publisher(backend)
            first = publisher.publish_partition("pilot", "DOGE/5m/2026-04-13", fixture)
            second = publisher.publish_partition("pilot", "DOGE/5m/2026-04-13", fixture)
            self.assertEqual(first, second)
            downloaded = download_and_verify_release(backend, "pilot", root / "download")
            self.assertEqual(len(downloaded), 2)
            self.assertTrue((root / "remote" / "pilot" / ".draft").exists())

    def test_conflicting_remote_output_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = self._fixture(root)
            publisher = Publisher(DirectoryReleaseBackend(root / "remote"))
            publisher.publish_partition("pilot", "DOGE/5m/2026-04-13", fixture)
            (fixture / "book.parquet").write_bytes(b"different")
            with self.assertRaises(ConflictError):
                publisher.publish_partition("pilot", "DOGE/5m/2026-04-13", fixture)

    def test_interruption_is_restartable_and_raw_cleanup_waits_for_publish(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = self._fixture(root)
            backend = FailingOnceBackend(root / "remote")
            raw = root / "raw.source"
            raw.write_bytes(b"raw")
            publisher = Publisher(backend)
            with self.assertRaises(OSError):
                publisher.publish_partition("pilot", "DOGE/5m/2026-04-13", fixture)
            self.assertTrue(raw.exists())
            publisher.publish_partition("pilot", "DOGE/5m/2026-04-13", fixture)

    def test_download_detects_remote_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = self._fixture(root)
            backend = DirectoryReleaseBackend(root / "remote")
            assets = Publisher(backend).publish_partition("pilot", "DOGE/5m/2026-04-13", fixture)
            Path(assets[0].download_url).write_bytes(b"tampered")
            with self.assertRaises(ConflictError):
                download_and_verify_release(backend, "pilot", root / "download")
