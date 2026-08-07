"""Sequential production source loader joining discovery, ranges, and provenance."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from canonical_data.audit import canonical_json_bytes
from canonical_data.discovery import GammaClient
from canonical_data.kacho import read_kacho_parquet
from canonical_data.manifest import hash_file
from canonical_data.models import Asset, BookEvent, Market, Provenance
from canonical_data.pmxt import read_pmxt_parquet
from canonical_data.rangeio import open_http_range


@dataclass(frozen=True)
class OfficialDiscovery:
    markets: tuple[Market, ...]
    provenance: tuple[Provenance, ...]


@dataclass(frozen=True)
class PMXTLoad:
    events: tuple[BookEvent, ...]
    provenance: tuple[Provenance, ...]


class ProductionSourceLoader:
    def __init__(self, gamma: GammaClient, retrieved_at_ns: int):
        self.gamma = gamma
        self.retrieved_at_ns = retrieved_at_ns

    def discover(self, asset: Asset, market_starts_s: list[int]) -> OfficialDiscovery:
        markets: list[Market] = []
        provenance: list[Provenance] = []
        for start in sorted(set(market_starts_s)):
            market, payload, url = self.gamma.fetch_market(asset, start)
            markets.append(market)
            provenance.append(
                Provenance(
                    source_id="polymarket_gamma_clob",
                    source_url=url,
                    retrieved_at_ns=self.retrieved_at_ns,
                    byte_length=len(payload),
                    sha256=hashlib.sha256(payload).hexdigest(),
                    license_id="ungranted_for_bulk_redistribution",
                    source_precision="official_api",
                    transformations=("minimum_identity_rules_resolution_binding",),
                )
            )
        return OfficialDiscovery(tuple(markets), tuple(provenance))

    def load_pmxt(
        self,
        urls: list[str],
        markets: tuple[Market, ...],
        max_transfer_per_object: int = 750_000_000,
    ) -> PMXTLoad:
        conditions = {market.condition_id for market in markets}
        tokens = {token for market in markets for token in (market.token_up, market.token_down)}
        events: list[BookEvent] = []
        provenance: list[Provenance] = []
        for url in sorted(set(urls)):
            reader, identity, raw = open_http_range(url, max_transfer_per_object)
            try:
                object_events = read_pmxt_parquet(reader, conditions, tokens, url)
            finally:
                reader.close()
            ledger = [item.__dict__ for item in raw.ledger]
            ledger_payload = canonical_json_bytes(
                {"etag": identity.etag, "object_bytes": identity.byte_length, "ranges": ledger}
            )
            events.extend(object_events)
            provenance.append(
                Provenance(
                    source_id="pmxt_v2",
                    source_url=url,
                    retrieved_at_ns=self.retrieved_at_ns,
                    byte_length=raw.transferred,
                    sha256=hashlib.sha256(ledger_payload).hexdigest(),
                    license_id="CC-BY-4.0",
                    source_precision="ms",
                    etag=identity.etag,
                    transformations=("conditional_range_read", "condition_token_filter"),
                )
            )
        return PMXTLoad(tuple(events), tuple(provenance))

    def load_kacho(
        self, path: Path, markets: tuple[Market, ...]
    ) -> tuple[list[dict[str, object]], Provenance]:
        conditions = {market.condition_id for market in markets}
        rows = read_kacho_parquet(path, conditions)
        length, digest = hash_file(path)
        provenance = Provenance(
            source_id="kacho_5m",
            source_url=str(path),
            retrieved_at_ns=self.retrieved_at_ns,
            byte_length=length,
            sha256=digest,
            license_id="CC0-1.0",
            source_precision="s",
            transformations=("condition_filter", "discard_inferred_outcome"),
        )
        return rows, provenance
