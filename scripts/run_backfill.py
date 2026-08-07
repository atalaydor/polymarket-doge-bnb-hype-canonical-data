"""Sequential, restart-safe Chapter 4 executor."""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq

from canonical_data.acquire import BoundedAcquirer
from canonical_data.audit import canonical_json_bytes
from canonical_data.binance import ingest_binance_zip
from canonical_data.discovery import GammaClient
from canonical_data.errors import ResourceLimitError, SourceError
from canonical_data.httpclient import USER_AGENT
from canonical_data.inventory import SourceObject, pmxt_hourly_objects
from canonical_data.manifest import hash_file
from canonical_data.models import Asset, Market, Provenance
from canonical_data.pipeline import PartitionInputs, Pipeline, PipelineLimits
from canonical_data.release import GitHubReleaseBackend, Publisher
from canonical_data.sources import ProductionSourceLoader
from canonical_data.spool import EventSpool
from canonical_data.state import StateStore

RETRY_DELAYS = (2, 8, 32)
RELEASE_CUTOFF = datetime(2026, 8, 7, 15, tzinfo=UTC)
KACHO_FILES = {
    Asset.DOGE: (
        "ab8ddc3e1a143d5786b6817c04a2732e5be45da77b137e52275c3877fb5f00ef.source",
        "99c1446c1f9f21f4822d34d10213afe82225ade6ec878d058585f8e4d15e8a92.source",
    ),
    Asset.BNB: (
        "85744b85e942f6d7348b5d9a87602fd523c2e42e33febeb519d5c2878d5c1efe.source",
        "9c8b8613d42a7255a47cd5ea3bdf5e7d109e6aeda716000cb0016bd06a4d7d56.source",
    ),
    Asset.HYPE: (
        "acfc39a2315f1340f41aec70b5e17e4a6275ad4614ffdf62cbfe3b23b035e022.source",
        "0b0c15aada2423874c71f4bc9020ecc0edd849f0110152226efeadc3db43ebc9.source",
    ),
}


def _atomic_json(path: Path, value: Any) -> None:
    payload = canonical_json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".partial")
    with temporary.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _tool_commit() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True, encoding="utf-8"
    ).strip()


def _fetch_text(url: str) -> str:
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=60) as response:
                return cast(bytes, response.read(4096)).decode()
        except urllib.error.HTTPError as exc:
            if exc.code not in {408, 429, 500, 502, 503, 504}:
                raise
            last = exc
        except urllib.error.URLError as exc:
            last = exc
        if attempt == len(RETRY_DELAYS):
            break
    assert last is not None
    raise last


def _instrument(asset: Asset) -> tuple[str, str, str]:
    symbol = f"{asset.value}USDT"
    if asset is Asset.HYPE:
        root = "https://data.binance.vision/data/futures/um/daily"
        precision = "ms"
    else:
        root = "https://data.binance.vision/data/spot/daily"
        precision = "us"
    return symbol, root, precision


def _market_starts(metadata: Path, day: date) -> list[int]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    table = pq.read_table(
        metadata,
        columns=["market_start"],
        filters=[("market_start", ">=", start), ("market_start", "<", end)],
    )
    return sorted(int(value.timestamp()) for value in table["market_start"].to_pylist())


def _expected_market_starts(day: date) -> list[int]:
    midnight = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())
    cutoff = int(RELEASE_CUTOFF.timestamp())
    return [
        midnight + offset
        for offset in range(0, 86_400, 300)
        if midnight + offset < cutoff
    ]


def _retry_pmxt(
    loader: ProductionSourceLoader, url: str, markets: tuple[Market, ...]
) -> Any:
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            return loader.load_pmxt([url], markets)
        except ResourceLimitError:
            raise
        except (SourceError, urllib.error.URLError, TimeoutError) as exc:
            last = exc
        if attempt == len(RETRY_DELAYS):
            break
    if last is not None:
        raise last
    raise AssertionError("unreachable PMXT retry state")


def _load_pmxt_hour(
    loader: ProductionSourceLoader,
    source: SourceObject,
    markets: tuple[Market, ...],
    raw_dir: Path,
) -> Any:
    try:
        return _retry_pmxt(loader, source.url, markets)
    except ResourceLimitError as exc:
        if "selected row groups exceed scan-row cap" not in str(exc):
            raise
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            acquired = BoundedAcquirer(raw_dir, 750_000_000, 8_000_000_000).acquire(source)
            loaded = loader.load_downloaded_pmxt(
                acquired.path, source.url, markets, acquired.etag
            )
            acquired.path.unlink()
            return loaded
        except (SourceError, urllib.error.URLError, TimeoutError) as exc:
            last = exc
        if attempt == len(RETRY_DELAYS):
            break
    assert last is not None
    raise last


def _acquire_with_retry(source: SourceObject, raw_dir: Path) -> Any:
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            return BoundedAcquirer(raw_dir, 750_000_000, 8_000_000_000).acquire(source)
        except (SourceError, urllib.error.URLError, TimeoutError) as exc:
            last = exc
        if attempt == len(RETRY_DELAYS):
            break
    assert last is not None
    raise last


def _provenance_from_json(value: dict[str, Any]) -> Provenance:
    return Provenance(
        source_id=str(value["source_id"]),
        source_url=str(value["source_url"]),
        retrieved_at_ns=int(value["retrieved_at_ns"]),
        byte_length=int(value["byte_length"]),
        sha256=str(value["sha256"]),
        license_id=str(value["license_id"]),
        source_precision=str(value["source_precision"]),
        etag=cast(str | None, value.get("etag")),
        upstream_checksum=cast(str | None, value.get("upstream_checksum")),
        transformations=tuple(str(item) for item in value["transformations"]),
    )


def prepare_shared_day(
    day: date, kacho_root: Path, work_root: Path
) -> tuple[Path, dict[Asset, tuple[Provenance, ...]], int]:
    shared = work_root / f"shared-{day.isoformat()}"
    state_path = shared / "state.json"
    state: dict[str, Any] = (
        json.loads(state_path.read_bytes())
        if state_path.exists()
        else {"completed_urls": {}, "source_bytes": 0}
    )
    discoveries: dict[Asset, Any] = {}
    for asset in Asset:
        metadata_name, _ = KACHO_FILES[asset]
        starts = _market_starts(kacho_root / metadata_name, day)
        if not starts:
            starts = _expected_market_starts(day)
        loader = ProductionSourceLoader(
            GammaClient(), time.time_ns(), work_root / f"{asset.value}-{day}" / "official"
        )
        discoveries[asset] = (loader, loader.discover(asset, starts))
    day_start_s = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())
    day_start_ns = day_start_s * 1_000_000_000
    day_end_ns = min(
        day_start_ns + 86_400_000_000_000,
        int(RELEASE_CUTOFF.timestamp()) * 1_000_000_000,
    )
    inventory_start_ns = max(day_start_ns - 3_600_000_000_000, 1_776_106_800_000_000_000)
    with EventSpool(shared / "events.sqlite") as spool:
        for source in pmxt_hourly_objects(inventory_start_ns, day_end_ns):
            if source.url in state["completed_urls"]:
                continue
            acquired = _acquire_with_retry(source, shared / "raw")
            by_asset: dict[str, Any] = {}
            for asset in Asset:
                loader, discovery = discoveries[asset]
                loaded = loader.load_downloaded_pmxt(
                    acquired.path, source.url, discovery.markets, acquired.etag
                )
                spool.append(loaded.events)
                by_asset[asset.value] = asdict(loaded.provenance[0])
            state["completed_urls"][source.url] = by_asset
            state["source_bytes"] = int(state["source_bytes"]) + acquired.byte_length
            _atomic_json(state_path, state)
            acquired.path.unlink()
    provenance = {
        asset: tuple(
            _provenance_from_json(item[asset.value])
            for _, item in sorted(state["completed_urls"].items())
        )
        for asset in Asset
    }
    return shared / "events.sqlite", provenance, int(state["source_bytes"])


def run_tier_b(
    asset: Asset,
    day: date,
    kacho_root: Path,
    work_root: Path,
    ledger_path: Path,
) -> dict[str, Any]:
    partition_id = f"{asset.value}/5m/{day.isoformat()}"
    ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {"partitions": {}}
    if partition_id in ledger["partitions"]:
        return cast(dict[str, Any], ledger["partitions"][partition_id])
    began = time.perf_counter()
    metadata_name, ticks_name = KACHO_FILES[asset]
    starts = _market_starts(kacho_root / metadata_name, day)
    if not starts:
        result = {
            "partition_id": partition_id,
            "quality": "EXCLUDED",
            "reason": "NO_MARKETS_IN_AUTHORITATIVE_PRODUCT_WINDOW",
            "markets": 0,
        }
        ledger["partitions"][partition_id] = result
        _atomic_json(ledger_path, ledger)
        return result
    work = work_root / f"{asset.value}-{day.isoformat()}"
    loader = ProductionSourceLoader(GammaClient(), time.time_ns(), work / "official")
    discovered = loader.discover(asset, starts)
    kacho_rows, kacho_provenance = loader.load_kacho(kacho_root / ticks_name, discovered.markets)
    symbol, binance_root, precision = _instrument(asset)
    filename = f"{symbol}-1m-{day.isoformat()}.zip"
    url = f"{binance_root}/klines/{symbol}/1m/{filename}"
    checksum = _fetch_text(url + ".CHECKSUM").split()[0]
    acquired = BoundedAcquirer(work / "raw", 10_000_000, 8_000_000_000).acquire(
        SourceObject("binance_public", url, checksum)
    )
    observations = ingest_binance_zip(acquired.path, checksum, asset, "klines", url)
    binance_bytes, binance_digest = hash_file(acquired.path)
    binance_provenance = Provenance(
        "binance_public",
        url,
        time.time_ns(),
        binance_bytes,
        binance_digest,
        "MIT_archive_repository_claim",
        precision,
        upstream_checksum=checksum,
        transformations=("checksum_verify",),
    )
    inputs = PartitionInputs(
        asset,
        day.isoformat(),
        discovered.markets,
        kacho_rows=tuple(kacho_rows),
        underlying=tuple(observations),
        provenance=(*discovered.provenance, kacho_provenance, binance_provenance),
        temporary_raw_paths=(acquired.path,),
    )
    pipeline = Pipeline(
        work / "output",
        StateStore(work / "state"),
        _tool_commit(),
        PipelineLimits(),
    )
    cutoff = int(
        (datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(days=1)).timestamp()
    )
    built = pipeline.build(inputs, cutoff * 1_000_000_000)
    release_tag = f"dataset-v1-{day:%Y-%m}"
    assets = pipeline.publish(
        built,
        Publisher(GitHubReleaseBackend("atalaydor/polymarket-doge-bnb-hype-canonical-data")),
        release_tag,
        inputs.temporary_raw_paths,
    )
    result = {
        "partition_id": partition_id,
        "quality": built.tier.value,
        "markets": len(discovered.markets),
        "kacho_rows": len(kacho_rows),
        "underlying_rows": len(observations),
        "canonical_bytes": sum(path.stat().st_size for path in built.directory.iterdir()),
        "manifest_sha256": built.manifest_digest,
        "release_tag": release_tag,
        "remote_assets": len(assets),
        "source_bytes": binance_bytes,
        "wall_seconds": time.perf_counter() - began,
    }
    ledger["partitions"][partition_id] = result
    _atomic_json(ledger_path, ledger)
    shutil.rmtree(work)
    return result


def run_partition(
    asset: Asset,
    day: date,
    kacho_root: Path,
    work_root: Path,
    ledger_path: Path,
    shared_spool: Path | None = None,
    shared_provenance: tuple[Provenance, ...] = (),
    shared_source_bytes: int = 0,
) -> dict[str, Any]:
    if day <= date(2026, 4, 12):
        return run_tier_b(asset, day, kacho_root, work_root, ledger_path)
    partition_id = f"{asset.value}/5m/{day.isoformat()}"
    ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {"partitions": {}}
    if partition_id in ledger["partitions"]:
        return cast(dict[str, Any], ledger["partitions"][partition_id])
    began = time.perf_counter()
    cpu_began = time.process_time()
    metadata_name, ticks_name = KACHO_FILES[asset]
    starts = _market_starts(kacho_root / metadata_name, day)
    if not starts:
        starts = _expected_market_starts(day)
    work = work_root / f"{asset.value}-{day.isoformat()}"
    loader = ProductionSourceLoader(GammaClient(), time.time_ns(), work / "official")
    discovered = loader.discover(asset, starts)
    spool_path = shared_spool or work / "pmxt-events.sqlite"
    pmxt_provenance: list[Provenance] = list(shared_provenance)
    pmxt_events = 0
    day_start_ns = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1e9)
    inventory_start_ns = max(day_start_ns - 3_600_000_000_000, 1_776_106_800_000_000_000)
    day_end_ns = min(
        day_start_ns + 86_400_000_000_000,
        int(RELEASE_CUTOFF.timestamp()) * 1_000_000_000,
    )
    if shared_spool is None:
        with EventSpool(spool_path) as spool:
            for source in pmxt_hourly_objects(inventory_start_ns, day_end_ns):
                loaded = _load_pmxt_hour(loader, source, discovered.markets, work / "raw-pmxt")
                pmxt_events += spool.append(loaded.events)
                pmxt_provenance.extend(loaded.provenance)
    else:
        with EventSpool(spool_path) as spool:
            pmxt_events = sum(
                spool.count_condition(market.condition_id) for market in discovered.markets
            )
    kacho_rows: list[dict[str, object]] = []
    kacho_provenance: tuple[Provenance, ...] = ()
    if starts and day <= date(2026, 5, 18):
        rows, provenance = loader.load_kacho(kacho_root / ticks_name, discovered.markets)
        kacho_rows = rows
        kacho_provenance = (provenance,)
    symbol, binance_root, precision = _instrument(asset)
    filename = f"{symbol}-1m-{day.isoformat()}.zip"
    url = f"{binance_root}/klines/{symbol}/1m/{filename}"
    checksum = _fetch_text(url + ".CHECKSUM").split()[0]
    acquired = BoundedAcquirer(work / "raw", 10_000_000, 8_000_000_000).acquire(
        SourceObject("binance_public", url, checksum)
    )
    observations = ingest_binance_zip(acquired.path, checksum, asset, "klines", url)
    binance_bytes, binance_digest = hash_file(acquired.path)
    binance_provenance = Provenance(
        "binance_public",
        url,
        time.time_ns(),
        binance_bytes,
        binance_digest,
        "MIT_archive_repository_claim",
        precision,
        upstream_checksum=checksum,
        transformations=("checksum_verify",),
    )
    inputs = PartitionInputs(
        asset,
        day.isoformat(),
        discovered.markets,
        kacho_rows=tuple(kacho_rows),
        underlying=tuple(observations),
        provenance=(
            *discovered.provenance,
            *pmxt_provenance,
            *kacho_provenance,
            binance_provenance,
        ),
        temporary_raw_paths=(acquired.path,) if shared_spool else (acquired.path, spool_path),
        event_spool_path=spool_path,
    )
    pipeline = Pipeline(work / "output", StateStore(work / "state"), _tool_commit())
    built = pipeline.build(inputs, day_end_ns)
    release_tag = f"dataset-v1-{day:%Y-%m}"
    assets = pipeline.publish(
        built,
        Publisher(GitHubReleaseBackend("atalaydor/polymarket-doge-bnb-hype-canonical-data")),
        release_tag,
        inputs.temporary_raw_paths,
    )
    result = {
        "partition_id": partition_id,
        "quality": built.tier.value,
        "markets": len(discovered.markets),
        "pmxt_events": pmxt_events,
        "kacho_rows": len(kacho_rows),
        "underlying_rows": len(observations),
        "canonical_bytes": sum(path.stat().st_size for path in built.directory.iterdir()),
        "manifest_sha256": built.manifest_digest,
        "release_tag": release_tag,
        "remote_assets": len(assets),
        "source_bytes": binance_bytes
        + (shared_source_bytes if asset is Asset.DOGE else 0)
        + (0 if shared_spool else sum(item.byte_length for item in pmxt_provenance)),
        "wall_seconds": time.perf_counter() - began,
        "cpu_seconds": time.process_time() - cpu_began,
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    }
    ledger["partitions"][partition_id] = result
    _atomic_json(ledger_path, ledger)
    shutil.rmtree(work)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kacho-root", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 4, 5))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 4, 12))
    args = parser.parse_args()
    current = args.start
    while current <= args.end:
        shared_spool: Path | None = None
        shared_provenance: dict[Asset, tuple[Provenance, ...]] = {}
        shared_source_bytes = 0
        if current >= date(2026, 4, 14):
            shared_spool, shared_provenance, shared_source_bytes = prepare_shared_day(
                current, args.kacho_root, args.work_root
            )
        for asset in Asset:
            print(
                json.dumps(
                    run_partition(
                        asset,
                        current,
                        args.kacho_root,
                        args.work_root,
                        args.ledger,
                        shared_spool,
                        shared_provenance.get(asset, ()),
                        shared_source_bytes,
                    ),
                    sort_keys=True,
                ),
                flush=True,
            )
        if shared_spool is not None:
            shutil.rmtree(shared_spool.parent)
        current += timedelta(days=1)


if __name__ == "__main__":
    main()
