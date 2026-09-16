import csv
import json
import time
import os
from datetime import datetime, timezone

from cassandra.cluster import Cluster

INTERVAL_SECONDS = 1
KEYSPACE = os.environ.get("MONITOR_KEYSPACE", "tablet_probe")
OUTPUT_DIR = os.environ.get("MONITOR_OUTPUT_DIR", "/data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TABLETS_CSV = os.path.join(OUTPUT_DIR, "tablets.csv")
SIZES_CSV = os.path.join(OUTPUT_DIR, "tablet_sizes.csv")

def utc_now():
    return datetime.now(timezone.utc).isoformat()

cluster = Cluster(["r-scylla1"])
session = cluster.connect()

table_row = session.execute("""
    SELECT id
    FROM system_schema.tables
    WHERE keyspace_name = %s
      AND table_name = 'test'
""", (KEYSPACE,)).one()

table_id = table_row.id

statement = session.prepare("""
    SELECT last_token, replicas, new_replicas,
           tablet_count, stage, transition,
           resize_type, resize_seq_number
    FROM system.tablets
    WHERE table_id = ?
""")

size_statement = session.prepare("""
    SELECT last_token, replicas, missing_replicas
    FROM system.tablet_sizes
    WHERE table_id = ?
""")

def collect_snapshot():
    rows = list(session.execute(statement, [table_id]))

    size_rows = list(session.execute(size_statement, [table_id]))

    timestamp = utc_now()

    tablets_file_exists = os.path.exists(TABLETS_CSV)
    sizes_file_exists = os.path.exists(SIZES_CSV)
    with open(TABLETS_CSV, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        if not tablets_file_exists:
            writer.writerow([
                "timestamp",
                "table_id",
                "last_token",
                "replicas",
                "new_replicas",
                "tablet_count",
                "stage",
                "transition",
                "resize_type",
                "resize_seq_number",
            ])

        for row in rows:
            writer.writerow([
                timestamp,
                table_id,
                row.last_token,
                json.dumps([[str(host), shard] for host, shard in row.replicas]),
                json.dumps([[str(host), shard] for host, shard in (row.new_replicas or [])]),
                row.tablet_count,
                row.stage,
                row.transition,
                row.resize_type,
                row.resize_seq_number,
            ])

    with open(SIZES_CSV, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        if not sizes_file_exists:
            writer.writerow([
                "timestamp",
                "table_id",
                "last_token",
                "host_id",
                "size_bytes",
                "missing_replicas",
            ])

        for row in size_rows:
            missing = [str(host) for host in (row.missing_replicas or [])]

            for host_id, size_bytes in (row.replicas or {}).items():
                writer.writerow([
                    timestamp,
                    table_id,
                    row.last_token,
                    str(host_id),
                    size_bytes,
                    json.dumps(missing),
                ])

    print(f"Collected {len(rows)} tablet rows.")
    print(f"Saved to {TABLETS_CSV}")

    replica_size_count = sum(
        len(row.replicas or {})
        for row in size_rows
    )

    print(
        f"Collected {len(size_rows)} tablets "
        f"with {replica_size_count} replica-size observations."
    )
    print(f"Saved to {SIZES_CSV}")

try:
    while True:
        collect_snapshot()
        time.sleep(INTERVAL_SECONDS)
except KeyboardInterrupt:
    print("Monitoring stopped.")
finally:
    cluster.shutdown()