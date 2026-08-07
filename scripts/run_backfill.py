"""Sequential, restart-safe Chapter 4 executor."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq

from canonical_data.acquire import BoundedAcquirer
from canonical_data.audit import canonical_json_bytes
from canonical_data.binance import ingest_binance_zip
from canonical_data.discovery import GammaClient
from canonical_data.httpclient import USER_AGENT
from canonical_data.inventory import SourceObject
from canonical_data.manifest import hash_file
from canonical_data.models import Asset, Provenance
from canonical_data.pipeline import PartitionInputs, Pipeline, PipelineLimits
from canonical_data.release import GitHubReleaseBackend, Publisher
from canonical_data.sources import ProductionSourceLoader
from canonical_data.state import StateStore

RETRY_DELAYS = (2, 8, 32)
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
        for asset in Asset:
            print(
                json.dumps(
                    run_tier_b(asset, current, args.kacho_root, args.work_root, args.ledger),
                    sort_keys=True,
                ),
                flush=True,
            )
        current += timedelta(days=1)


if __name__ == "__main__":
    main()
