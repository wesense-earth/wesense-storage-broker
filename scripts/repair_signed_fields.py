"""
Restore signed fields the storage broker dropped, so stored readings verify again.

Until the fix that added them to ReadingIn, the broker silently dropped two
canonical (signed) fields from every reading it stored: location_source and
calibration_status. Ingesters using ReadingPipeline (from 2026-04-13) sign
both, so those stored rows can never verify and the archive builder could not
archive them.

This script finds signed rows whose location_source is empty and that do not
verify, tries the values the ingesters sign, and re-inserts a row only when
its signature then verifies. Nothing is changed for a row that already
verifies or that cannot be made to verify. Re-inserted rows have the same
sorting key and version, so ReplacingMergeTree collapses each onto the
original (FINAL reads see the repaired row straight away).

Dry run by default; --apply writes.

Usage, from inside the storage-broker container (uses its CLICKHOUSE_* env):
    python scripts/repair_signed_fields.py                  # report only
    python scripts/repair_signed_fields.py --apply          # repair
    python scripts/repair_signed_fields.py --data-source ecan --apply
"""

import argparse
import base64
import logging
import sys
from collections import Counter
from datetime import timezone

import clickhouse_connect
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from wesense_gateway.archive import BACKGROUND_QUERY_SETTINGS
from wesense_gateway.config import GatewayConfig
from wesense_ingester.pipeline import build_canonical, canonical_to_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [repair] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("repair")

# Ingesters that sign a non-empty location_source through ReadingPipeline.
# Meshtastic signs an empty one, so its rows are not affected.
DEFAULT_DATA_SOURCES = [
    # wesense-ingester-wesense (location_source="gps")
    "wesense", "WESENSE", "TTN",
    # wesense-ingester-govaq-nz (location_source="manual"; ecan also calibration_status="calibrated")
    "ecan", "horizons", "gisborne", "marlborough", "tasman", "hawkesbay", "westcoast", "nelson",
    # wesense-ingester-govaq-au (location_source="manual")
    "govaq_au_nsw", "govaq_au_qld", "govaq_au_act", "govaq_au_vic", "govaq_au_sa", "govaq_au_wa",
]

# Values the ingesters sign for the two dropped fields
CANDIDATES = [
    {"location_source": ls, "calibration_status": cs}
    for ls in ("manual", "gps")
    for cs in ("", "calibrated")
]

INSERT_BATCH = 50_000


def connect(config: GatewayConfig):
    kwargs = dict(
        host=config.clickhouse_host,
        port=8443 if config.tls_enabled else config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_database,
        # No shared session: the reader streams while the writer inserts, and
        # ClickHouse refuses concurrent use of one session (code 373).
        autogenerate_session_id=False,
    )
    if config.tls_enabled:
        kwargs["secure"] = True
        if config.tls_ca_certfile:
            kwargs["verify"] = True
            kwargs["ca_cert"] = config.tls_ca_certfile
        else:
            kwargs["verify"] = False
    return clickhouse_connect.get_client(**kwargs)


def verifies(row: dict, overrides: dict | None = None) -> bool:
    src = dict(row)
    if overrides:
        src.update(overrides)
    ts = row["timestamp"]
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    src["timestamp"] = int(ts.timestamp())
    src["sensor_transport"] = row.get("transport_type", "")
    try:
        payload = canonical_to_json(
            build_canonical(src, version=int(row.get("signing_payload_version") or 1))
        )
        key = Ed25519PublicKey.from_public_bytes(base64.b64decode(row["public_key"]))
        key.verify(bytes.fromhex(row["signature"]), payload)
        return True
    except Exception:
        return False


def repair_source(client, writer, table: str, data_source: str, apply: bool) -> Counter:
    counts = Counter()
    months = [
        r[0] for r in client.query(
            f"SELECT DISTINCT toYYYYMM(timestamp) m FROM {table} "
            "WHERE data_source = {ds:String} AND signature != '' AND public_key != '' "
            "AND location_source = '' ORDER BY m",
            parameters={"ds": data_source},
            settings=BACKGROUND_QUERY_SETTINGS,
        ).result_rows
    ]
    for month in months:
        pending: list[tuple] = []
        columns: list[str] = []
        with client.query_row_block_stream(
            f"SELECT * FROM {table} FINAL "
            "WHERE data_source = {ds:String} AND toYYYYMM(timestamp) = {m:UInt32} "
            "AND signature != '' AND public_key != '' AND location_source = ''",
            parameters={"ds": data_source, "m": month},
            settings=BACKGROUND_QUERY_SETTINGS,
        ) as stream:
            columns = list(stream.source.column_names)
            for block in stream:
                for values in block:
                    row = dict(zip(columns, values))
                    if verifies(row):
                        counts["already_verify"] += 1
                        continue
                    fix = next((c for c in CANDIDATES if verifies(row, c)), None)
                    if fix is None:
                        counts["still_fail"] += 1
                        continue
                    counts["repaired"] += 1
                    row.update(fix)
                    pending.append(tuple(row[c] for c in columns))
                    if apply and len(pending) >= INSERT_BATCH:
                        writer.insert(table, pending, column_names=columns)
                        pending = []
        if apply and pending:
            writer.insert(table, pending, column_names=columns)
        logger.info("%s %s: %s", data_source, month, dict(counts))
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true", help="write repaired rows (default: report only)")
    parser.add_argument("--data-source", action="append", help="limit to these data_source values")
    args = parser.parse_args()

    config = GatewayConfig()
    client = connect(config)   # reads (streams)
    writer = connect(config)   # inserts, on its own connection
    table = config.clickhouse_table
    sources = args.data_source or DEFAULT_DATA_SOURCES

    logger.info("%s on %s (%s)", "Repairing" if args.apply else "Dry run", table, ", ".join(sources))
    total = Counter()
    for ds in sources:
        total += repair_source(client, writer, table, ds, args.apply)

    logger.info(
        "Done. %d already verified, %d %s, %d still do not verify.",
        total["already_verify"], total["repaired"],
        "repaired" if args.apply else "would be repaired", total["still_fail"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
