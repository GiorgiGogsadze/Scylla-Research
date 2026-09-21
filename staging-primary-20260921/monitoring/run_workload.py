import argparse
import re
import csv
import os
import math
import random
import time
from datetime import datetime, timezone
from load_data import make_value
import json

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

LOG_FIELDS = [
    "index",
    "scheduled_offset_ms",
    "start_offset_ms",
    "timestamp_utc",
    "end_timestamp_utc",
    "operation",
    "row_id",
    "latency_ms",
    "success",
    "error",
]

def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a timed ScyllaDB research workload."
    )
    parser.add_argument("--keyspace", required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--value-bytes", type=int, required=True)
    parser.add_argument("--duration-seconds", type=int, required=True)
    parser.add_argument("--rate", type=float, required=True)
    parser.add_argument("--read-percent", type=int, required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--dataset-seed", type=int, default=42)
    parser.add_argument("--state-file", required=True,
                        help="Shared write-state JSON for all phases of one lifecycle")
    parser.add_argument("--output", required=True)
    return parser.parse_args()

def validate_args(args):
    if not re.fullmatch(r"[a-z][a-z0-9_]*", args.keyspace):
        raise SystemExit("Invalid keyspace name")
    if args.rows <= 0 or args.value_bytes <= 0:
        raise SystemExit("--rows and --value-bytes must be positive")
    if args.duration_seconds <= 0 or args.rate <= 0:
        raise SystemExit("--duration-seconds and --rate must be positive")
    if not 0 <= args.read_percent <= 100:
        raise SystemExit("--read-percent must be between 0 and 100")

def open_log(path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    file = open(path, "x", newline="", encoding="utf-8")
    writer = csv.DictWriter(file, fieldnames=LOG_FIELDS)
    writer.writeheader()
    return file, writer

def operation_plan(args):
    rng = random.Random(args.seed)
    count = math.ceil(args.duration_seconds * args.rate)

    for index in range(count):
        row_id = rng.randrange(args.rows)
        operation = (
            "read" if rng.randrange(100) < args.read_percent else "write"
        )
        scheduled_offset_seconds = index / args.rate
        yield index, operation, row_id, scheduled_offset_seconds

def prepare_workload(keyspace):
    cluster = Cluster(["r-scylla1"])
    session = cluster.connect(keyspace)

    read = session.prepare("SELECT value FROM test WHERE id = ?")
    write = session.prepare("UPDATE test SET value = ? WHERE id = ?")

    read.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    write.consistency_level = ConsistencyLevel.LOCAL_QUORUM

    return cluster, session, read, write

def execute_one(session, read, write, args, plan, run_start, expected_writes):
    index, operation, row_id, scheduled_offset_seconds = plan
    started = time.perf_counter()
    timestamp = datetime.now(timezone.utc).isoformat()

    try:
        if operation == "read":
            row = session.execute(read, (row_id,)).one()
            if row is None:
                raise ValueError(f"Row {row_id} was not found")
            if len(row.value) != args.value_bytes:
                raise ValueError(f"Row {row_id} has an unexpected value length")
            expected_seed = expected_writes.get(row_id, args.dataset_seed)
            if row.value != make_value(expected_seed, row_id, args.value_bytes):
                raise ValueError(f"Row {row_id} has unexpected deterministic content")
        else:
            write_seed = args.seed + index + 1
            value = make_value(write_seed, row_id, args.value_bytes)
            session.execute(write, (value, row_id))
            expected_writes[row_id] = write_seed
        success = True
        error = ""
    except Exception as exc:
        success = False
        error = f"{type(exc).__name__}: {exc}".replace("\n", " ")

    latency_ms = (time.perf_counter() - started) * 1000
    end_timestamp = datetime.now(timezone.utc).isoformat()
    return {
        "index": index,
        "scheduled_offset_ms": round(scheduled_offset_seconds * 1000, 3),
        "start_offset_ms": round((started - run_start) * 1000, 3),
        "timestamp_utc": timestamp,
        "end_timestamp_utc": end_timestamp,
        "operation": operation,
        "row_id": row_id,
        "latency_ms": round(latency_ms, 3),
        "success": success,
        "error": error,
    }

def run_schedule(args, session, read, write, writer, file):
    if os.path.exists(args.state_file):
        with open(args.state_file, encoding="utf-8") as state_input:
            state = json.load(state_input)
        if (state["keyspace"], state["rows"], state["dataset_seed"]) != (
            args.keyspace, args.rows, args.dataset_seed
        ):
            raise ValueError("Write state does not match this lifecycle")
        expected_writes = {int(k): int(v) for k, v in state["writes"].items()}
    else:
        expected_writes = {}
    run_start = time.perf_counter()
    completed = 0
    failed = 0

    try:
        for plan in operation_plan(args):
            index, _, _, scheduled_offset_seconds = plan
            wait = run_start + scheduled_offset_seconds - time.perf_counter()
            if wait > 0:
                time.sleep(wait)

            record = execute_one(session, read, write, args, plan, run_start, expected_writes)
            writer.writerow(record)
            completed += 1
            failed += not record["success"]

            if completed % 100 == 0:
                file.flush()
    finally:
        file.flush()
        os.makedirs(os.path.dirname(args.state_file) or ".", exist_ok=True)
        temp_path = args.state_file + ".tmp"
        with open(temp_path, "w", encoding="utf-8") as state_output:
            json.dump({"keyspace": args.keyspace, "rows": args.rows,
                       "dataset_seed": args.dataset_seed,
                       "writes": expected_writes}, state_output)
        os.replace(temp_path, args.state_file)

    file.flush()
    elapsed = time.perf_counter() - run_start
    print(
        f"Completed {completed} operations in {elapsed:.2f}s; "
        f"errors={failed}; actual rate={completed / elapsed:.2f}/s"
    )

def write_manifest(args):
    manifest = {
        **vars(args),
        "table": "test",
        "read_consistency": "LOCAL_QUORUM",
        "write_consistency": "LOCAL_QUORUM",
        "client": "single synchronous client",
        "planned_operations": math.ceil(args.duration_seconds * args.rate),
        "created_utc": datetime.now(timezone.utc).isoformat(),
    }
    with open(args.output + ".json", "x", encoding="utf-8") as file:
        json.dump(manifest, file, indent=2)

if __name__ == "__main__":
    args = parse_args()
    validate_args(args)

    cluster = None
    try:
        cluster, session, read, write = prepare_workload(args.keyspace)
        file, writer = open_log(args.output)
        write_manifest(args)
        with file:
            run_schedule(args, session, read, write, writer, file)
    finally:
        if cluster is not None:
            cluster.shutdown()
