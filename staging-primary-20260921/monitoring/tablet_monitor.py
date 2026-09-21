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
INDEX_CSV = os.path.join(OUTPUT_DIR, "snapshot_index.csv")
for path in (TABLETS_CSV, SIZES_CSV, INDEX_CSV):
    if os.path.exists(path):
        raise SystemExit(f"Refusing to append to existing output: {path}")

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

if table_row is None:
    raise SystemExit(f"Table {KEYSPACE}.test does not exist")
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
    tokens = [row.last_token for row in rows]
    size_tokens = [row.last_token for row in size_rows]
    if not rows or len(tokens) != len(set(tokens)):
        raise RuntimeError(f"Empty or duplicate tablet placement group at {timestamp}")
    if len(size_tokens) != len(set(size_tokens)):
        raise RuntimeError(f"Duplicate tablet size group at {timestamp}")
    if set(size_tokens) != set(tokens):
        # Keep the full raw groups and mark the mismatch; during a transition,
        # the two system tables may briefly represent different stages.
        print(f"Incomplete paired size snapshot at {timestamp}", flush=True)
    size_replica_count = sum(len(row.replicas or {}) for row in size_rows)
    placement_replica_count = sum(len(row.replicas or []) for row in rows)
    sizes_by_token = {row.last_token: row for row in size_rows}
    complete_size_groups = sum(
        1 for row in rows
        if row.last_token in sizes_by_token
        and {str(host) for host, _ in (row.replicas or [])}
        == {str(host) for host in (sizes_by_token[row.last_token].replicas or {})}
        and not (sizes_by_token[row.last_token].missing_replicas or [])
    )

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

    replica_size_count = size_replica_count

    index_exists = os.path.exists(INDEX_CSV)
    with open(INDEX_CSV, "a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        if not index_exists:
            writer.writerow(["timestamp", "table_id", "tablet_rows", "size_rows",
                             "placement_replicas", "size_replicas", "token_sets_equal",
                             "complete_size_groups"])
        writer.writerow([timestamp, table_id, len(rows), len(size_rows),
                         placement_replica_count, size_replica_count,
                         set(size_tokens) == set(tokens), complete_size_groups])

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
