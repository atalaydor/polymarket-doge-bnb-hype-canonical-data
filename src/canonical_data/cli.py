"""Production command line entry points."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from canonical_data.inventory import pmxt_hourly_objects
from canonical_data.manifest import verify_manifest
from canonical_data.release import DirectoryReleaseBackend, download_and_verify_release
from canonical_data.timeutil import iso_to_ns


def main() -> None:
    parser = argparse.ArgumentParser(prog="dataset-pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    inventory = commands.add_parser("inventory-pmxt")
    inventory.add_argument("--start", required=True)
    inventory.add_argument("--end", required=True)
    verify = commands.add_parser("verify-partition")
    verify.add_argument("directory", type=Path)
    download = commands.add_parser("verify-directory-release")
    download.add_argument("root", type=Path)
    download.add_argument("release")
    download.add_argument("target", type=Path)
    args = parser.parse_args()
    if args.command == "inventory-pmxt":
        objects = pmxt_hourly_objects(iso_to_ns(args.start), iso_to_ns(args.end))
        print(
            json.dumps([item.__dict__ for item in objects], sort_keys=True, separators=(",", ":"))
        )
    elif args.command == "verify-partition":
        print(verify_manifest(args.directory))
    else:
        paths = download_and_verify_release(
            DirectoryReleaseBackend(args.root), args.release, args.target
        )
        print(json.dumps([str(path) for path in paths], sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
