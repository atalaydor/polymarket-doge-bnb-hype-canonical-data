"""Draft GitHub Release publication and complete re-verification."""

from __future__ import annotations

import http.client
import json
import os
import shutil
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from canonical_data.errors import ConflictError, ResourceLimitError, SourceError
from canonical_data.manifest import hash_file

MAX_RELEASE_ASSET_BYTES = 1_900_000_000


class GitHubAPIError(SourceError):
    def __init__(self, method: str, code: int):
        super().__init__(f"GitHub API {method} failed: {code}")
        self.code = code


@dataclass(frozen=True)
class ReleaseAsset:
    name: str
    byte_length: int
    sha256: str
    download_url: str


class ReleaseBackend(Protocol):
    def ensure_draft(self, tag: str) -> str: ...

    def list_assets(self, release_id: str) -> list[ReleaseAsset]: ...

    def upload(self, release_id: str, name: str, path: Path) -> ReleaseAsset: ...

    def download(self, asset: ReleaseAsset, target: Path) -> None: ...

    def finalize(self, release_id: str) -> None: ...


def content_addressed_name(partition_id: str, path: Path, digest: str) -> str:
    return f"{partition_id.replace('/', '--')}--{digest}--{path.name}"


class Publisher:
    def __init__(self, backend: ReleaseBackend):
        self.backend = backend

    def publish_partition(
        self, tag: str, partition_id: str, directory: Path, finalize: bool = False
    ) -> list[ReleaseAsset]:
        release_id = self.backend.ensure_draft(tag)
        existing = {asset.name: asset for asset in self.backend.list_assets(release_id)}
        accepted: list[ReleaseAsset] = []
        for path in sorted(directory.iterdir()):
            if not path.is_file():
                continue
            length, digest = hash_file(path)
            if length >= MAX_RELEASE_ASSET_BYTES:
                raise ResourceLimitError("release asset exceeds repository cap")
            name = content_addressed_name(partition_id, path, digest)
            logical_suffix = f"--{path.name}"
            conflicts = [
                item
                for item in existing.values()
                if item.name.endswith(logical_suffix) and item.name != name
            ]
            if conflicts:
                raise ConflictError(f"conflicting remote asset identity: {path.name}")
            asset = existing.get(name)
            if asset is None:
                asset = self.backend.upload(release_id, name, path)
            if asset.byte_length != length or asset.sha256 != digest:
                raise ConflictError(f"uploaded metadata mismatch: {path.name}")
            self._download_verify(asset, length, digest, directory)
            accepted.append(asset)
        if finalize:
            self.backend.finalize(release_id)
        return accepted

    def _download_verify(
        self, asset: ReleaseAsset, length: int, digest: str, directory: Path
    ) -> None:
        target = directory / f".{asset.name}.verify"
        try:
            self.backend.download(asset, target)
            actual_length, actual_digest = hash_file(target)
            if (actual_length, actual_digest) != (length, digest):
                raise ConflictError(f"download re-verification failed: {asset.name}")
        finally:
            target.unlink(missing_ok=True)


class DirectoryReleaseBackend:
    """Filesystem backend for exhaustive publication semantics tests."""

    def __init__(self, root: Path):
        self.root = root

    def ensure_draft(self, tag: str) -> str:
        directory = self.root / tag
        directory.mkdir(parents=True, exist_ok=True)
        marker = directory / ".draft"
        marker.touch(exist_ok=True)
        return tag

    def list_assets(self, release_id: str) -> list[ReleaseAsset]:
        result = []
        for path in sorted((self.root / release_id).iterdir()):
            if path.name.startswith("."):
                continue
            length, _ = hash_file(path)
            result.append(ReleaseAsset(path.name, length, _digest_from_name(path.name), str(path)))
        return result

    def upload(self, release_id: str, name: str, path: Path) -> ReleaseAsset:
        target = self.root / release_id / name
        if target.exists():
            raise ConflictError("backend refuses overwrite")
        temporary = target.with_suffix(target.suffix + ".partial")
        shutil.copyfile(path, temporary)
        os.replace(temporary, target)
        length, digest = hash_file(target)
        return ReleaseAsset(name, length, digest, str(target))

    def download(self, asset: ReleaseAsset, target: Path) -> None:
        shutil.copyfile(asset.download_url, target)

    def finalize(self, release_id: str) -> None:
        marker = self.root / release_id / ".draft"
        if not marker.exists():
            raise ConflictError("release is not draft")
        marker.rename(self.root / release_id / ".published")


class GitHubReleaseBackend:
    """Production GitHub API backend using the existing authenticated environment."""

    def __init__(self, repository: str, token: str | None = None):
        self.repository = repository
        self.token = token or os.environ.get("GITHUB_TOKEN") or ""
        if not self.token:
            raise SourceError("GitHub authenticated environment is unavailable")
        self.api = f"https://api.github.com/repos/{repository}"

    def _request(
        self,
        method: str,
        url: str,
        payload: bytes | None = None,
        content_type: str = "application/json",
    ) -> Any:
        request = urllib.request.Request(
            url,
            data=payload,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "Content-Type": content_type,
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                body = response.read()
        except urllib.error.HTTPError as exc:
            raise GitHubAPIError(method, exc.code) from exc
        return json.loads(body) if body else None

    def ensure_draft(self, tag: str) -> str:
        try:
            release = self._request("GET", f"{self.api}/releases/tags/{urllib.parse.quote(tag)}")
            if not release.get("draft"):
                raise ConflictError("existing release is not staged as draft")
        except GitHubAPIError as exc:
            if exc.code != 404:
                raise
            payload = json.dumps(
                {"tag_name": tag, "name": tag, "draft": True, "prerelease": False}
            ).encode()
            release = self._request("POST", f"{self.api}/releases", payload)
        return str(release["id"])

    def list_assets(self, release_id: str) -> list[ReleaseAsset]:
        result: list[ReleaseAsset] = []
        for page in range(1, 11):
            raw = self._request(
                "GET", f"{self.api}/releases/{release_id}/assets?per_page=100&page={page}"
            )
            result.extend(
                ReleaseAsset(
                    item["name"],
                    int(item["size"]),
                    _digest_from_name(item["name"]),
                    item["url"],
                )
                for item in raw
            )
            if len(raw) < 100:
                return result
        raise ResourceLimitError("GitHub release reached the 1,000 asset limit")

    def upload(self, release_id: str, name: str, path: Path) -> ReleaseAsset:
        query = urllib.parse.urlencode({"name": name})
        length = path.stat().st_size
        if length >= MAX_RELEASE_ASSET_BYTES:
            raise ResourceLimitError("release asset exceeds repository cap")
        endpoint = f"/repos/{self.repository}/releases/{release_id}/assets?{query}"
        connection = http.client.HTTPSConnection("uploads.github.com", timeout=120)
        connection.putrequest("POST", endpoint)
        connection.putheader("Authorization", f"Bearer {self.token}")
        connection.putheader("Accept", "application/vnd.github+json")
        connection.putheader("Content-Type", "application/octet-stream")
        connection.putheader("Content-Length", str(length))
        connection.putheader("X-GitHub-Api-Version", "2022-11-28")
        connection.endheaders()
        with path.open("rb") as handle:
            while chunk := handle.read(1_048_576):
                connection.send(chunk)
        response = connection.getresponse()
        body = response.read()
        connection.close()
        if response.status not in {200, 201}:
            raise GitHubAPIError("POST", response.status)
        raw = json.loads(body)
        return ReleaseAsset(
            raw["name"],
            int(raw["size"]),
            _digest_from_name(raw["name"]),
            raw["url"],
        )

    def download(self, asset: ReleaseAsset, target: Path) -> None:
        request = urllib.request.Request(
            asset.download_url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/octet-stream",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        with urllib.request.urlopen(request, timeout=120) as response, target.open("wb") as handle:
            shutil.copyfileobj(response, handle, length=1_048_576)

    def finalize(self, release_id: str) -> None:
        self._request(
            "PATCH", f"{self.api}/releases/{release_id}", json.dumps({"draft": False}).encode()
        )


def _digest_from_name(name: str) -> str:
    parts = name.rsplit("--", 2)
    if (
        len(parts) != 3
        or len(parts[1]) != 64
        or not all(char in "0123456789abcdef" for char in parts[1])
    ):
        raise ConflictError("remote asset is not content-addressed")
    return parts[1]


def download_and_verify_release(
    backend: ReleaseBackend, release_id: str, target: Path
) -> list[Path]:
    target.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for asset in backend.list_assets(release_id):
        path = target / asset.name
        backend.download(asset, path)
        length, digest = hash_file(path)
        if length != asset.byte_length or digest != asset.sha256:
            raise ConflictError(f"release asset substituted: {asset.name}")
        paths.append(path)
    return paths
