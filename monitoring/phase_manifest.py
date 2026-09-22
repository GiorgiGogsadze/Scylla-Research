"""Write an immutable planned-settings manifest before a measured phase."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source-dir", required=True, type=Path)
    parser.add_argument("--lifecycle", required=True)
    parser.add_argument("--block", type=int, required=True)
    parser.add_argument("--treatment", choices=("tablets", "vnodes"), required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--keyspace", required=True)
    parser.add_argument("--phase", choices=("baseline", "scaleout", "four_node",
                                            "scalein", "final_three"), required=True)
    parser.add_argument("--duration-seconds", type=int, required=True)
    parser.add_argument("--workload-seed", type=int, required=True)
    args = parser.parse_args()
    if args.block < 1 or args.duration_seconds <= 0:
        parser.error("block and duration must be positive")
    expected_durations = {"baseline": 120, "scaleout": 900, "four_node": 120,
                          "scalein": 900, "final_three": 120}
    if args.duration_seconds != expected_durations[args.phase]:
        parser.error("duration differs from the frozen protocol")
    expected_topology = {"baseline": "3", "scaleout": "3-to-4",
                         "four_node": "4", "scalein": "4-to-3", "final_three": "3"}
    files = ("load_data.py", "run_workload.py", "tablet_monitor.py",
             "capture_streaming_counters.py", "capture_docker_stats_dynamic.ps1",
             "capture_vnode_ring.ps1", "record_event.ps1", "phase_manifest.py")
    hashes = {}
    for name in files:
        path = args.source_dir / name
        hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "lifecycle": args.lifecycle, "block": args.block,
        "treatment": args.treatment, "project": args.project,
        "keyspace": args.keyspace, "phase": args.phase,
        "topology_plan": expected_topology[args.phase],
        "rows": 750000, "id_range": [0, 749999], "dataset_seed": 42,
        "value_bytes": 1024, "replication_factor": 2,
        "rate_per_second": 500, "read_percent": 80,
        "read_consistency": "LOCAL_QUORUM",
        "write_consistency": "LOCAL_QUORUM",
        "warmup_seconds": 30, "warmup_read_percent": 100,
        "duration_seconds": args.duration_seconds,
        "event_target_offset_seconds": 120 if args.phase in ("scaleout", "scalein") else None,
        "workload_seed": args.workload_seed,
        "source_sha256": hashes,
        "verification_status": "planned; live checks and outcomes recorded separately",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2)
        stream.write("\n")


if __name__ == "__main__":
    main()
