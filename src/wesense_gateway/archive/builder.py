"""Parquet archive builder — exports readings to deterministic Parquet files."""

import asyncio
import hashlib
import io
import json
import logging
from datetime import datetime, timezone


import pyarrow as pa
import pyarrow.parquet as pq

from wesense_ingester.ids.reading_id import generate_reading_id
from wesense_ingester.pipeline import CANONICAL_FIELDS
from wesense_ingester.signing.keys import IngesterKeyManager
from wesense_ingester.signing.trust import TrustStore

from wesense_gateway.archive.manifest import (
    build_manifest,
    build_trust_snapshot_from_readings,
    compute_readings_hash,
)
from wesense_gateway.archive.verifier import verify_signatures
from wesense_gateway.backends.base import StorageBackend

logger = logging.getLogger(__name__)


class ArchiveVerificationError(Exception):
    """A region-day has signed readings but none of them verify."""

# ClickHouse column name for each canonical (signed) field. Only the transport
# field is named differently in storage.
_STORAGE_NAME = {"sensor_transport": "transport_type"}

# Signature metadata stored alongside the canonical fields (not itself signed)
SIGNATURE_COLUMNS = [
    "ingester_id", "key_version", "signing_payload_version", "public_key", "signature",
]

# Every signed field plus signature metadata, in ClickHouse names. Derived from
# the canonical field list so the archive query can never drop a signed field:
# a missing field is rebuilt as its default and the signature no longer verifies.
SIGNED_COLUMNS = [_STORAGE_NAME.get(f, f) for f in CANONICAL_FIELDS] + SIGNATURE_COLUMNS

# Nullable in the canonical reading: None must stay None (not 0) to verify
NULLABLE_FLOAT_COLUMNS = {"latitude", "longitude", "altitude"}
_FLOAT_COLUMNS = {"value"} | NULLABLE_FLOAT_COLUMNS
_UINT_COLUMNS = {"key_version", "signing_payload_version"}


def _parquet_type(name: str) -> pa.DataType:
    if name in _FLOAT_COLUMNS:
        return pa.float64()
    if name in _UINT_COLUMNS:
        return pa.uint32()
    return pa.string()


# Parquet schema v2: reading_id + every signed field + signature metadata, so an
# archive can be verified on its own. Timestamp is ISO 8601 (UTC).
PARQUET_SCHEMA = pa.schema(
    [("reading_id", pa.string())] + [(name, _parquet_type(name)) for name in SIGNED_COLUMNS]
)


class ParquetArchiveBuilder:
    """
    Builds Parquet archives from ClickHouse readings.

    Subdivision-level partitioning:
        {country}/{subdivision}/{YYYY}/{MM}/{DD}/readings.parquet
    """

    def __init__(
        self,
        ch_client,
        trust_store: TrustStore,
        key_manager: IngesterKeyManager,
        backend: StorageBackend,
    ):
        self._ch = ch_client
        self._trust_store = trust_store
        self._key_manager = key_manager
        self._backend = backend

    async def archive_period(
        self, period: str, country: str, subdivision: str
    ) -> dict | None:
        """
        Archive a single country/subdivision/day.

        1. Query ClickHouse for signed readings
        2. Verify signatures
        3. Build trust snapshot
        4. Export deterministic Parquet
        5. Build and sign manifest
        6. Store all files via backend

        Returns the manifest dict, or None if no readings.
        """
        logger.info("Archiving %s/%s/%s", country, subdivision, period)

        loop = asyncio.get_event_loop()
        readings = await loop.run_in_executor(
            None, self._query_readings, period, country, subdivision
        )
        if not readings:
            logger.info("No signed readings for %s/%s/%s — skipping", country, subdivision, period)
            return None

        logger.info("Fetched %d signed readings for %s/%s/%s", len(readings), country, subdivision, period)

        # Verify signatures
        verified, failed = verify_signatures(readings, self._trust_store)
        logger.info(
            "Signature verification: %d verified, %d failed for %s/%s/%s",
            len(verified), failed, country, subdivision, period,
        )

        if not verified:
            raise ArchiveVerificationError(
                f"{country}/{subdivision}/{period}: none of {failed} signed readings verify, so "
                "no archive was written. The stored rows differ from what was signed; see "
                "scripts/repair_signed_fields.py and the Data Integrity docs."
            )
        if failed:
            logger.warning(
                "%s/%s/%s: %d of %d signed readings do not verify and are left out of the archive",
                country, subdivision, period, failed, failed + len(verified),
            )

        # Trust snapshot — sourced directly from the readings' public_key column.
        # Makes archives self-contained without any live TrustStore dependency.
        trust_snapshot = build_trust_snapshot_from_readings(verified)

        # Parquet
        parquet_bytes = self._export_parquet(verified)

        # Readings hash
        reading_ids = sorted(r["reading_id"] for r in verified if r.get("reading_id"))
        readings_hash = compute_readings_hash(reading_ids)

        # Trust snapshot hash
        trust_snapshot_json = json.dumps(trust_snapshot, sort_keys=True, indent=2)
        trust_snapshot_hash = hashlib.sha256(trust_snapshot_json.encode()).hexdigest()

        # Manifest
        manifest = build_manifest(
            period=period,
            region=country,
            subdivision=subdivision,
            verified_count=len(verified),
            failed_count=failed,
            readings_hash=readings_hash,
            trust_snapshot_hash=trust_snapshot_hash,
            key_manager=self._key_manager,
        )

        # Store files via backend
        date_parts = period.split("-")
        base_path = f"{country}/{subdivision}/{date_parts[0]}/{date_parts[1]}/{date_parts[2]}"

        blake3_hash = await self._backend.store(f"{base_path}/readings.parquet", parquet_bytes)
        await self._backend.store(f"{base_path}/trust_snapshot.json", trust_snapshot_json.encode())

        manifest_json = json.dumps(manifest, indent=2)
        await self._backend.store(f"{base_path}/manifest.json", manifest_json.encode())

        logger.info(
            "Archived %s/%s/%s — %d readings, hash=%s...",
            country, subdivision, period, len(verified), readings_hash[:16],
        )

        return manifest

    def _query_readings(
        self, period: str, country: str, subdivision: str
    ) -> list[dict]:
        """Query ClickHouse for signed readings for a country/subdivision/day.

        All accepted readings are archived (both locally ingested and
        P2P-received). Stations running the same canonical version produce
        byte-identical archives; iroh gossip deduplicates. Older stations
        reject newer readings at ingestion (forward rejection), so they
        never produce divergent archives.
        """
        columns = SIGNED_COLUMNS
        query = f"""
            SELECT {", ".join(columns)}
            FROM sensor_readings FINAL
            WHERE toDate(timestamp) = {{period:String}}
              AND geo_country = {{country:String}}
              AND geo_subdivision = {{subdivision:String}}
              AND signature != ''
            ORDER BY device_id, reading_type, timestamp
        """
        result = self._ch.query(
            query,
            parameters={
                "period": period,
                "country": country,
                "subdivision": subdivision,
            },
        )

        readings = []
        for row in result.result_rows:
            reading = dict(zip(columns, row))

            # Preserve unix timestamp for signature verification and reading_id
            ts = reading["timestamp"]
            if hasattr(ts, "timestamp"):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                reading["_ts_unix"] = int(ts.timestamp())
                reading["timestamp"] = ts.isoformat()
            else:
                ts = datetime.fromisoformat(str(ts))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                reading["_ts_unix"] = int(ts.timestamp())

            reading["reading_id"] = generate_reading_id(
                reading["device_id"],
                reading["_ts_unix"],
                reading["reading_type"],
                reading["value"],
            )
            readings.append(reading)

        return readings

    def _export_parquet(self, readings: list[dict]) -> bytes:
        """Export readings to a deterministic Parquet file."""
        readings_sorted = sorted(readings, key=lambda r: r.get("reading_id", ""))

        columns = {}
        for field in PARQUET_SCHEMA:
            col_name = field.name
            if col_name in NULLABLE_FLOAT_COLUMNS:
                columns[col_name] = [
                    None if r.get(col_name) is None else float(r[col_name]) for r in readings_sorted
                ]
            elif field.type == pa.float64():
                columns[col_name] = [float(r.get(col_name, 0) or 0) for r in readings_sorted]
            elif field.type == pa.uint32():
                columns[col_name] = [int(r.get(col_name, 0) or 0) for r in readings_sorted]
            else:
                columns[col_name] = [str(r.get(col_name, "") or "") for r in readings_sorted]

        table = pa.table(columns, schema=PARQUET_SCHEMA)

        buf = io.BytesIO()
        pq.write_table(
            table, buf,
            compression="zstd",
            use_dictionary=False,
            write_statistics=False,
        )
        return buf.getvalue()
