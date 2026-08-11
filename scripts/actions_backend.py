"""Bounded GitHub Actions adapter for the finite Chapter 4 executor."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from canonical_data.audit import canonical_json_bytes
from canonical_data.planner import build_backfill_plan

REPOSITORY = "atalaydor/polymarket-doge-bnb-hype-canonical-data"
API = f"https://api.github.com/repos/{REPOSITORY}"
PLAN_START = date(2026, 4, 5)
PLAN_END = date(2026, 8, 7)
EXPECTED_FILES = {
    "book-200ms.parquet",
    "book-events.parquet",
    "exclusions.parquet",
    "manifest.json",
    "markets.parquet",
    "underlying.parquet",
}
ASSET_PATTERN = re.compile(
    r"^(DOGE|BNB|HYPE)--5m--(\d{4}-\d{2}-\d{2})--([0-9a-f]{64})--(.+)$"
)
KACHO_REVISION = "f646aa62c6ffe3873fefb9f4a9b4ef7df0f5b4e5"
KACHO_TICKS = {
    "DOGE": (
        "doge_ticks.parquet",
        "7fabd65d9af567ca39ac520f4e7ee749d059f8574aa3f53ea5d3db37a3707cee.source",
        "7fabd65d9af567ca39ac520f4e7ee749d059f8574aa3f53ea5d3db37a3707cee",
        80_060_676,
    ),
    "BNB": (
        "bnb_ticks.parquet",
        "1958c9643a15add7620703329f47d3f67d8ff4c5c8d6115696bc745fb866478e.source",
        "1958c9643a15add7620703329f47d3f67d8ff4c5c8d6115696bc745fb866478e",
        76_690_643,
    ),
    "HYPE": (
        "hype_ticks.parquet",
        "f99e833ba91bde2406d288b3419d502db396282749e5c07125c01ee4ebbe3dd6.source",
        "f99e833ba91bde2406d288b3419d502db396282749e5c07125c01ee4ebbe3dd6",
        74_977_804,
    ),
}
TRANSIENT_HTTP_STATUS = {408, 429, 500, 502, 503, 504}
TRANSFER_RETRY_DELAYS = (2, 8, 32)


@dataclass(frozen=True)
class RemoteAsset:
    name: str
    size: int
    url: str
    digest: str
    filename: str


def _request(url: str, accept: str = "application/vnd.github+json") -> bytes:
    token = os.environ.get("GITHUB_TOKEN", "")
    headers = {
        "Accept": accept,
        "User-Agent": "chapter-4-actions-backend/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=120) as response:
        return cast(bytes, response.read())


def _json(url: str) -> Any:
    return json.loads(_request(url))


def remote_inventory() -> dict[str, list[RemoteAsset]]:
    releases = _json(f"{API}/releases?per_page=100")
    result: dict[str, list[RemoteAsset]] = {}
    for release in releases:
        if not str(release.get("tag_name", "")).startswith("dataset-v1-"):
            continue
        release_id = int(release["id"])
        for page in range(1, 11):
            assets = _json(f"{API}/releases/{release_id}/assets?per_page=100&page={page}")
            for item in assets:
                match = ASSET_PATTERN.fullmatch(str(item["name"]))
                if match is None:
                    continue
                partition = f"{match[1]}/5m/{match[2]}"
                result.setdefault(partition, []).append(
                    RemoteAsset(
                        str(item["name"]),
                        int(item["size"]),
                        str(item["url"]),
                        match[3],
                        match[4],
                    )
                )
            if len(assets) < 100:
                break
        else:
            raise RuntimeError("release asset inventory exceeded bounded pagination")
    return result


def verified_partitions(inventory: dict[str, list[RemoteAsset]]) -> set[str]:
    verified: set[str] = set()
    for partition, assets in inventory.items():
        filenames = [asset.filename for asset in assets]
        if len(assets) == len(EXPECTED_FILES) and set(filenames) == EXPECTED_FILES:
            verified.add(partition)
    return verified


def select_proof_partition(
    ledger: dict[str, Any], inventory: dict[str, list[RemoteAsset]]
) -> str:
    plan = build_backfill_plan(PLAN_START, PLAN_END)
    completed = int(ledger["completed"])
    if completed < 1 or completed > len(plan):
        raise RuntimeError("ledger completed count is outside the finite plan")
    complete = verified_partitions(inventory)
    if completed != len(complete):
        raise RuntimeError("ledger completed count does not match durable remote evidence")
    requested = str(ledger.get("last_partition", ""))
    if requested not in {str(item["partition_id"]) for item in plan}:
        raise RuntimeError("ledger proof partition is outside the finite plan")
    if requested not in complete:
        raise RuntimeError(
            f"ledger proof partition lacks durable remote evidence: {requested}"
        )
    return requested


def unfinished_plan(inventory: dict[str, list[RemoteAsset]]) -> list[dict[str, Any]]:
    complete = verified_partitions(inventory)
    return [
        item
        for item in build_backfill_plan(PLAN_START, PLAN_END)
        if item["partition_id"] not in complete
    ]


def matrix_stages(plan: list[dict[str, Any]]) -> list[dict[str, list[dict[str, str]]]]:
    stages = []
    for offset in range(0, len(plan), 256):
        include = [
            {"partition_id": str(item["partition_id"])} for item in plan[offset : offset + 256]
        ]
        stages.append({"include": include})
    return stages or [{"include": []}]


def _download_verify(asset: RemoteAsset, directory: Path) -> None:
    target = directory / asset.filename
    target.write_bytes(_request(asset.url, "application/octet-stream"))
    payload = target.read_bytes()
    if len(payload) != asset.size or hashlib.sha256(payload).hexdigest() != asset.digest:
        raise RuntimeError(f"remote digest verification failed: {asset.name}")


def verify_remote_partition(
    partition: str, inventory: dict[str, list[RemoteAsset]]
) -> dict[str, Any]:
    assets = inventory.get(partition, [])
    if partition not in verified_partitions(inventory):
        raise RuntimeError(f"partition is not durably complete: {partition}")
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        for asset in sorted(assets, key=lambda item: item.filename):
            _download_verify(asset, directory)
        manifest = json.loads((directory / "manifest.json").read_bytes())
        if manifest["partition_id"] != partition:
            raise RuntimeError("remote manifest partition identity mismatch")
    return {
        "partition_id": partition,
        "result": "VERIFIED_NO_OP",
        "assets": len(assets),
        "bytes": sum(asset.size for asset in assets),
        "authenticated_redownload": bool(os.environ.get("GITHUB_TOKEN")),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1_048_576):
            digest.update(chunk)
    return digest.hexdigest()


def kacho_required(day: str) -> bool:
    return date.fromisoformat(day) <= date(2026, 5, 18)


def _raise_child_failure(completed: subprocess.CompletedProcess[str]) -> None:
    if not completed.returncode:
        return
    sys.stdout.write(completed.stdout)
    sys.stderr.write(completed.stderr)
    raise RuntimeError(f"one-partition executor failed with exit code {completed.returncode}")


def acquire_kacho(asset: str, root: Path) -> int:
    source_name, target_name, expected_digest, expected_bytes = KACHO_TICKS[asset]
    root.mkdir(parents=True, exist_ok=True)
    target = root / target_name
    url = (
        "https://huggingface.co/datasets/kachoio/"
        "polymarket-5-minute-crypto-up-down-markets/resolve/"
        f"{KACHO_REVISION}/{source_name}"
    )
    request = urllib.request.Request(url, headers={"User-Agent": "chapter-4-actions-backend/1.0"})
    last_error: Exception | None = None
    for attempt, delay in enumerate((0, *TRANSFER_RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            with urllib.request.urlopen(request, timeout=300) as response, target.open(
                "wb"
            ) as handle:
                shutil.copyfileobj(response, handle, 1_048_576)
                status = int(response.status)
                content_type = response.headers.get_content_type()
                content_length = response.headers.get("Content-Length")
                etag = response.headers.get("ETag")
                response_host = urlsplit(response.geturl()).hostname
            break
        except urllib.error.HTTPError as exc:
            target.unlink(missing_ok=True)
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last_error = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            target.unlink(missing_ok=True)
            last_error = exc
        if attempt == len(TRANSFER_RETRY_DELAYS):
            assert last_error is not None
            raise last_error
    actual_bytes = target.stat().st_size
    actual_digest = _sha256(target)
    if actual_bytes != expected_bytes or actual_digest != expected_digest:
        diagnostics = {
            "source": source_name,
            "revision": KACHO_REVISION,
            "status": status,
            "response_host": response_host,
            "content_type": content_type,
            "content_length": content_length,
            "etag": etag,
            "expected_bytes": expected_bytes,
            "actual_bytes": actual_bytes,
            "expected_sha256": expected_digest,
            "actual_sha256": actual_digest,
        }
        raise RuntimeError(
            f"Kacho source integrity mismatch: {json.dumps(diagnostics, sort_keys=True)}"
        )
    return actual_bytes


def _atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(".partial")
    temporary.write_bytes(canonical_json_bytes(value))
    os.replace(temporary, path)


def reconcile_ledger(result: dict[str, Any], inventory: dict[str, list[RemoteAsset]]) -> None:
    path = Path("config/backfill-ledger.json")
    ledger = json.loads(path.read_bytes())
    complete = verified_partitions(inventory)
    if result["partition_id"] not in complete:
        raise RuntimeError("refusing ledger reconciliation without durable remote receipt")
    if int(ledger["completed"]) + 1 != len(complete):
        raise RuntimeError("serialized ledger baseline does not match remote authority")
    quality = str(result["quality"])
    ledger["completed"] = len(complete)
    ledger["tier_a"] = int(ledger["tier_a"]) + int(quality == "TIER_A")
    ledger["tier_b"] = int(ledger["tier_b"]) + int(quality == "TIER_B")
    ledger["excluded"] = int(ledger["excluded"]) + int(quality == "EXCLUDED")
    ledger["last_partition"] = result["partition_id"]
    remaining = unfinished_plan(inventory)
    ledger["continuation_partition"] = (
        remaining[0]["partition_id"] if remaining else None
    )
    ledger["measured_canonical_bytes"] = int(ledger["measured_canonical_bytes"]) + int(
        result["canonical_bytes"]
    )
    ledger["measured_source_bytes"] = int(ledger["measured_source_bytes"]) + int(
        result["source_bytes"]
    )
    ledger["remote_assets"] = sum(len(items) for items in inventory.values())
    _atomic_json(path, ledger)


def _write_output(name: str, value: str) -> None:
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with Path(output).open("a", encoding="utf-8") as handle:
            handle.write(f"{name}={value}\n")


def command_plan() -> None:
    inventory = remote_inventory()
    complete = verified_partitions(inventory)
    plan = unfinished_plan(inventory)
    stages = matrix_stages(plan)
    report = {
        "planned": 375,
        "verified": len(complete),
        "unfinished": len(plan),
        "verified_in_plan": sum(item["partition_id"] in complete for item in plan),
        "unique_unfinished": len({item["partition_id"] for item in plan}),
        "stages": [len(stage["include"]) for stage in stages],
        "max_matrix": max(len(stage["include"]) for stage in stages),
        "max_parallel": 1,
        "fail_fast": False,
    }
    print(json.dumps(report, sort_keys=True))
    _write_output("matrix_0", json.dumps(stages[0], separators=(",", ":")))
    _write_output(
        "matrix_1",
        json.dumps(stages[1] if len(stages) > 1 else {"include": []}, separators=(",", ":")),
    )
    _write_output("unfinished", str(len(plan)))


def command_proof() -> None:
    began = time.monotonic()
    inventory = remote_inventory()
    ledger = json.loads(Path("config/backfill-ledger.json").read_bytes())
    partition = select_proof_partition(ledger, inventory)
    report = verify_remote_partition(partition, inventory)
    report["wall_seconds"] = time.monotonic() - began
    print(json.dumps(report, sort_keys=True))


def command_execute(requested: str | None) -> None:
    before = remote_inventory()
    if requested:
        partition = requested
        if partition in verified_partitions(before):
            print(json.dumps(verify_remote_partition(partition, before), sort_keys=True))
            return
        valid = {item["partition_id"] for item in unfinished_plan(before)}
        if partition not in valid:
            raise RuntimeError("requested partition is outside the finite unfinished plan")
    else:
        plan = unfinished_plan(before)
        if not plan:
            raise RuntimeError("finite plan is already complete")
        partition = str(plan[0]["partition_id"])
    asset, timeframe, day = partition.split("/")
    if timeframe != "5m":
        raise RuntimeError("unsupported timeframe")
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        kacho_bytes = acquire_kacho(asset, root / "kacho") if kacho_required(day) else 0
        detailed = root / "partition-ledger.json"
        command = [
            "python",
            "scripts/run_backfill.py",
            "--kacho-root",
            str(root / "kacho"),
            "--work-root",
            str(root / "work"),
            "--ledger",
            str(detailed),
            "--start",
            day,
            "--end",
            day,
            "--asset",
            asset,
        ]
        samples = {"peak_work_bytes": 0, "minimum_free_disk_bytes": shutil.disk_usage(root).free}
        stopped = threading.Event()

        def sample_disk() -> None:
            while not stopped.wait(1):
                used = 0
                for path in root.rglob("*"):
                    try:
                        if path.is_file():
                            used += path.stat().st_size
                    except FileNotFoundError:
                        # The child removes verified temporary inputs concurrently.
                        continue
                samples["peak_work_bytes"] = max(samples["peak_work_bytes"], used)
                samples["minimum_free_disk_bytes"] = min(
                    samples["minimum_free_disk_bytes"], shutil.disk_usage(root).free
                )

        sampler = threading.Thread(target=sample_disk, daemon=True)
        sampler.start()
        began = time.monotonic()
        try:
            completed = subprocess.run(command, check=False, text=True, capture_output=True)
        finally:
            stopped.set()
            sampler.join()
        wall = time.monotonic() - began
        _raise_child_failure(completed)
        lines = [line for line in completed.stdout.splitlines() if line.startswith("{")]
        if len(lines) != 1:
            raise RuntimeError("one-partition executor did not return exactly one result")
        result = json.loads(lines[0])
        result["kacho_transfer_bytes"] = kacho_bytes
        result["actions_wall_seconds"] = wall
        result.update(samples)
        result["timeout_margin_seconds"] = 21_600 - wall
        print(json.dumps(result, sort_keys=True))
    after = remote_inventory()
    proof = verify_remote_partition(partition, after)
    print(json.dumps(proof, sort_keys=True))
    reconcile_ledger(result, after)


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan")
    commands.add_parser("proof")
    execute = commands.add_parser("execute")
    execute.add_argument("--partition")
    args = parser.parse_args()
    if args.command == "plan":
        command_plan()
    elif args.command == "proof":
        command_proof()
    else:
        command_execute(args.partition)


if __name__ == "__main__":
    main()
