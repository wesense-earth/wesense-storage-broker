"""A reading signed by an ingester must still verify after the broker stores it
and the archive builder reads it back, and its Parquet archive must carry
every signed field.

Regression: the broker dropped location_source and calibration_status, and the
builder selected 21 of the canonical columns, so no reading signed since the
v1 canonical freeze (2026-04-14) verified and no archives were written.
"""

import base64
import io
from unittest.mock import AsyncMock, MagicMock

import pyarrow.parquet as pq
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from wesense_ingester.pipeline import (
    CANONICAL_FIELDS,
    CURRENT_CANONICAL_VERSION,
    build_canonical,
    canonical_to_json,
)

from wesense_gateway.archive.builder import (
    PARQUET_SCHEMA,
    SIGNED_COLUMNS,
    ArchiveVerificationError,
    ParquetArchiveBuilder,
)
from wesense_gateway.archive.verifier import verify_signatures
from wesense_gateway.models.reading import ReadingBatch, ReadingIn
from wesense_gateway.pipeline.processor import ReadingProcessor
from wesense_gateway.storage.clickhouse import CLICKHOUSE_COLUMNS

STORAGE_NAME = {"sensor_transport": "transport_type"}


def _signed_gateway_dict(**overrides):
    """Build, sign and shape a reading exactly as ReadingPipeline sends it to the broker."""
    reading = {
        "device_id": "govaq_ecan_1",
        "timestamp": 1764288000,
        "reading_type": "pm2_5",
        "reading_type_name": "PM2.5",
        "value": 12.3,
        "unit": "µg/m³",
        "latitude": -43.53,
        "longitude": 172.63,
        "altitude": None,
        "data_source": "ecan",
        "data_source_name": "Environment Canterbury",
        "sensor_transport": "",
        "geo_country": "nz",
        "geo_subdivision": "can",
        "board_model": "",
        "sensor_model": "BAM",
        "calibration_status": "calibrated",
        "deployment_type": "OUTDOOR",
        "deployment_type_source": "manual",
        "node_name": "Christchurch",
        "node_info": "",
        "node_info_url": "",
        "location_source": "manual",
        "data_license": "CC-BY-4.0",
    }
    reading.update(overrides)
    key = Ed25519PrivateKey.generate()
    canonical = build_canonical(reading)
    signature = key.sign(canonical_to_json(canonical)).hex()
    public_key = base64.b64encode(
        key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    ).decode()

    gateway = dict(canonical)
    gateway["transport_type"] = gateway.pop("sensor_transport")
    gateway.update(
        signature=signature,
        ingester_id="wsi_test0001",
        key_version=1,
        signing_payload_version=CURRENT_CANONICAL_VERSION,
        public_key=public_key,
    )
    return gateway


async def _store_and_read_back(gateway: dict) -> list[dict]:
    """POST /readings model → stored ClickHouse row → archive builder fetch."""
    writer = MagicMock()
    writer.add = AsyncMock()
    dedup = MagicMock()
    dedup.is_duplicate.return_value = False
    processor = ReadingProcessor(ch_writer=writer, dedup_cache=dedup)

    await processor.process_batch(ReadingBatch(readings=[ReadingIn(**gateway)]))
    writer.add.assert_awaited_once()
    stored = dict(zip(CLICKHOUSE_COLUMNS, writer.add.call_args[0][0]))

    ch = MagicMock()
    ch.query.return_value.result_rows = [tuple(stored[c] for c in SIGNED_COLUMNS)]
    builder = ParquetArchiveBuilder.__new__(ParquetArchiveBuilder)
    builder._ch = ch
    return builder._query_readings("2025-11-28", "nz", "can")


def test_broker_stores_every_signed_field():
    for field in CANONICAL_FIELDS:
        assert STORAGE_NAME.get(field, field) in CLICKHOUSE_COLUMNS, field


def test_archive_query_selects_every_signed_field():
    for field in CANONICAL_FIELDS:
        assert STORAGE_NAME.get(field, field) in SIGNED_COLUMNS, field


@pytest.mark.parametrize("overrides", [
    {},  # government AQ: location_source=manual, calibration_status=calibrated
    {"data_source": "wesense", "location_source": "gps", "calibration_status": "",
     "sensor_transport": "wifi", "altitude": 12.5},
])
async def test_signed_reading_verifies_after_storage(overrides):
    readings = await _store_and_read_back(_signed_gateway_dict(**overrides))
    verified, failed = verify_signatures(readings, trust_store=MagicMock())
    assert failed == 0
    assert len(verified) == 1


async def test_parquet_archive_carries_every_signed_field_and_keeps_null_altitude():
    readings = await _store_and_read_back(_signed_gateway_dict())
    builder = ParquetArchiveBuilder.__new__(ParquetArchiveBuilder)
    table = pq.read_table(io.BytesIO(builder._export_parquet(readings)))

    for field in CANONICAL_FIELDS:
        assert STORAGE_NAME.get(field, field) in table.column_names, field
    assert table.column("altitude")[0].as_py() is None
    assert table.column("location_source")[0].as_py() == "manual"
    assert table.column("calibration_status")[0].as_py() == "calibrated"


async def test_archive_period_raises_when_nothing_verifies():
    gateway = _signed_gateway_dict()
    readings = await _store_and_read_back(gateway)
    readings[0]["location_source"] = ""  # what the broker used to store

    builder = ParquetArchiveBuilder.__new__(ParquetArchiveBuilder)
    builder._trust_store = MagicMock()
    builder._query_readings = MagicMock(return_value=readings)
    with pytest.raises(ArchiveVerificationError, match="none of 1 signed readings verify"):
        await builder.archive_period("2025-11-28", "nz", "can")
