import argparse
import base64
import hashlib
import re

from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load a reproducible ScyllaDB research dataset."
    )
    parser.add_argument("--keyspace", required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--value-bytes", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    return parser.parse_args()

def make_value(seed, row_id, value_bytes):
    source = f"{seed}:{row_id}".encode("ascii")
    raw = hashlib.shake_256(source).digest((value_bytes * 3 + 3) // 4)
    return base64.b64encode(raw).decode("ascii")[:value_bytes]

if __name__ == "__main__":
    args = parse_args()

    if args.rows <= 0 or args.value_bytes <= 0:
        raise SystemExit("--rows and --value-bytes must be positive")
    if not re.fullmatch(r"[a-z][a-z0-9_]*", args.keyspace):
        raise SystemExit("Invalid keyspace name")

    cluster = Cluster(["r-scylla1"])
    try:
        session = cluster.connect(args.keyspace)
        insert = session.prepare(
            "INSERT INTO test (id, value) VALUES (?, ?)"
        )
        insert.consistency_level = ConsistencyLevel.LOCAL_QUORUM

        for row_id in range(args.rows):
            value = make_value(args.seed, row_id, args.value_bytes)
            session.execute(insert, (row_id, value))
            if (row_id + 1) % 1000 == 0:
                print(f"Loaded {row_id + 1} rows...", flush=True)

        print(
            f"Loaded {args.rows} rows into {args.keyspace}.test "
            f"with seed {args.seed} and {args.value_bytes}-character values."
        )
    finally:
        cluster.shutdown()