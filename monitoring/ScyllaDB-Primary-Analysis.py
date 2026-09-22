#!/usr/bin/env python3
"""Build compact, auditable summaries from the ScyllaDB primary-study evidence.

The script is read-only with respect to data/raw.  It writes derived CSV files,
a manifest, a concise Markdown report, and a ZIP archive under data/analysis.
It uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import shutil
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_VERSION = "1.0.0"
PHASES = ("baseline", "scaleout", "four_node", "scalein", "final_three")
TREATMENTS = {
    "tablets": "primaryb1tablets",
    "vnodes": "primaryb1vnodes",
}
TRANSITION_EVENT = {
    "scaleout": "join_command_start",
    "scalein": "decommission_command_start",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/analysis/primary-comparison"),
    )
    return parser.parse_args()


def dt(value: str) -> datetime:
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso(value: datetime | None) -> str:
    return "" if value is None else value.isoformat()


def truth(value: str) -> bool:
    return value.strip().lower() in {"true", "1", "yes"}


def number(value: str | None) -> float:
    if value is None or not value.strip():
        return math.nan
    return float(value)


def percentile(values: list[float], probability: float) -> float:
    clean = sorted(v for v in values if not math.isnan(v))
    if not clean:
        return math.nan
    if len(clean) == 1:
        return clean[0]
    position = (len(clean) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return clean[lower]
    return clean[lower] + (clean[upper] - clean[lower]) * (position - lower)


def fmt(value: float, digits: int = 3) -> str:
    if math.isnan(value):
        return ""
    return f"{value:.{digits}f}"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_events(path: Path) -> list[dict]:
    events = []
    with path.open(encoding="utf-8-sig") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            row = json.loads(line)
            row["_time"] = dt(row["host_utc"])
            row["_line"] = line_number
            events.append(row)
    return events


def event_maps(events: list[dict]) -> tuple[dict, dict, dict]:
    starts, ends, named = {}, {}, defaultdict(list)
    for row in events:
        named[row["event"]].append(row["_time"])
        phase = row.get("phase")
        if row["event"] == "phase_start" and phase:
            starts[phase] = row["_time"]
        elif row["event"] == "phase_end" and phase:
            ends[phase] = row["_time"]
    return starts, ends, named


def phase_for_time(value: datetime, starts: dict, ends: dict) -> str:
    for phase in PHASES:
        if phase in starts and phase in ends and starts[phase] <= value <= ends[phase]:
            return phase
    return ""


def summarize_operations(rows: list[dict]) -> dict:
    latency = defaultdict(list)
    lag = []
    operation_counts = Counter()
    failures = 0
    bad_indices = 0
    bad_ids = 0
    previous_index = None
    starts, ends = [], []
    for row in rows:
        index = int(row["index"])
        if previous_index is not None and index != previous_index + 1:
            bad_indices += 1
        previous_index = index
        row_id = int(row["row_id"])
        if not 0 <= row_id < 750000:
            bad_ids += 1
        operation = row["operation"].strip().lower()
        operation_counts[operation] += 1
        if not truth(row["success"]):
            failures += 1
        latency[operation].append(float(row["latency_ms"]))
        lag.append(float(row["start_offset_ms"]) - float(row["scheduled_offset_ms"]))
        starts.append(dt(row["timestamp_utc"]))
        ends.append(dt(row["end_timestamp_utc"]))

    output = {
        "requests": len(rows),
        "reads": operation_counts["read"],
        "writes": operation_counts["write"],
        "other_operations": len(rows) - operation_counts["read"] - operation_counts["write"],
        "failures": failures,
        "bad_indices": bad_indices,
        "bad_row_ids": bad_ids,
        "first_request_utc": iso(min(starts) if starts else None),
        "last_completion_utc": iso(max(ends) if ends else None),
        "request_span_s": fmt((max(ends) - min(starts)).total_seconds(), 6) if starts else "",
        "observed_rate_per_s": fmt(len(rows) / (max(ends) - min(starts)).total_seconds(), 6)
        if len(rows) > 1 and max(ends) > min(starts)
        else "",
        "schedule_lag_mean_ms": fmt(statistics.fmean(lag) if lag else math.nan),
        "schedule_lag_p50_ms": fmt(percentile(lag, 0.50)),
        "schedule_lag_p95_ms": fmt(percentile(lag, 0.95)),
        "schedule_lag_p99_ms": fmt(percentile(lag, 0.99)),
        "schedule_lag_max_ms": fmt(max(lag) if lag else math.nan),
    }
    for operation in ("read", "write"):
        values = latency[operation]
        output.update(
            {
                f"{operation}_mean_ms": fmt(statistics.fmean(values) if values else math.nan),
                f"{operation}_p50_ms": fmt(percentile(values, 0.50)),
                f"{operation}_p95_ms": fmt(percentile(values, 0.95)),
                f"{operation}_p99_ms": fmt(percentile(values, 0.99)),
                f"{operation}_max_ms": fmt(max(values) if values else math.nan),
            }
        )
    return output


def load_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def workload_outputs(raw_root: Path, contexts: dict, checks: list[dict]) -> tuple[list[dict], list[dict]]:
    summaries, bins = [], []
    for treatment, context in contexts.items():
        lifecycle = context["lifecycle"]
        for phase in PHASES:
            measured = lifecycle / phase / "measured.csv"
            exists = measured.is_file()
            checks.append({"treatment": treatment, "check": f"{phase}_measured_exists", "value": str(exists), "pass": str(exists)})
            if not exists:
                continue
            rows = load_csv(measured)
            summary = {"treatment": treatment, "phase": phase, **summarize_operations(rows)}
            summaries.append(summary)

            if phase not in TRANSITION_EVENT:
                continue
            event_name = TRANSITION_EVENT[phase]
            event_times = context["named"].get(event_name, [])
            if len(event_times) != 1:
                checks.append({"treatment": treatment, "check": f"{phase}_{event_name}_count", "value": str(len(event_times)), "pass": "False"})
                continue
            event_time = event_times[0]
            for basis in ("actual", "scheduled"):
                grouped = defaultdict(list)
                for row in rows:
                    relative = (
                        (dt(row["timestamp_utc"]) - event_time).total_seconds()
                        if basis == "actual"
                        else float(row["scheduled_offset_ms"]) / 1000.0 - 120.0
                    )
                    bin_start = math.floor(relative / 5.0) * 5
                    grouped[bin_start].append(row)
                for bin_start in sorted(grouped):
                    subset = grouped[bin_start]
                    item = summarize_operations(subset)
                    bins.append(
                        {
                            "treatment": treatment,
                            "phase": phase,
                            "event": event_name,
                            "event_utc": iso(event_time),
                            "alignment": basis,
                            "bin_start_s": bin_start,
                            "bin_end_s": bin_start + 5,
                            **item,
                        }
                    )
    return summaries, bins


UNIT_FACTORS = {
    "B": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "TIB": 1024**4,
}


def parse_size(value: str) -> int:
    # Docker may switch to scientific notation at unit boundaries, for
    # example ``1e+03MB``.  Accept ordinary decimals and exponent notation.
    match = re.fullmatch(
        r"\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*([KMGT]?i?B)\s*",
        value,
        re.IGNORECASE,
    )
    if not match:
        raise ValueError(f"Unsupported byte-size value: {value!r}")
    return round(float(match.group(1)) * UNIT_FACTORS[match.group(2).upper()])


def split_pair(value: str) -> tuple[int, int]:
    left, right = value.split("/", 1)
    return parse_size(left), parse_size(right)


def resource_outputs(contexts: dict, checks: list[dict]) -> list[dict]:
    output = []
    for treatment, context in contexts.items():
        path = context["lifecycle"] / "docker_stats.csv"
        rows = load_csv(path)
        previous = {}
        for row in rows:
            timestamp = dt(row["timestamp_utc"])
            phase = phase_for_time(timestamp, context["starts"], context["ends"])
            if not phase:
                continue
            memory_used, memory_limit = split_pair(row["memory_usage"])
            net_rx, net_tx = split_pair(row["network_io"])
            block_read, block_write = split_pair(row["block_io"])
            key = row["node"]
            prev = previous.get(key)
            reset = bool(prev and (net_rx < prev[0] or net_tx < prev[1] or block_read < prev[2] or block_write < prev[3]))
            event_name = TRANSITION_EVENT.get(phase, "")
            event_times = context["named"].get(event_name, []) if event_name else []
            event_relative = (timestamp - event_times[0]).total_seconds() if len(event_times) == 1 else math.nan
            output.append(
                {
                    "treatment": treatment,
                    "phase": phase,
                    "event": event_name,
                    "timestamp_utc": iso(timestamp),
                    "event_relative_s": fmt(event_relative, 6),
                    "sample": row["sample"],
                    "node": row["node"],
                    "node_count": row["node_count"],
                    "cpu_percent": row["cpu_percent"].replace("%", ""),
                    "memory_used_bytes": memory_used,
                    "memory_limit_bytes": memory_limit,
                    "net_rx_bytes": net_rx,
                    "net_tx_bytes": net_tx,
                    "block_read_bytes": block_read,
                    "block_write_bytes": block_write,
                    "vm_mem_available_kib": row["vm_mem_available_kib"],
                    "counter_reset": reset,
                }
            )
            previous[key] = (net_rx, net_tx, block_read, block_write)
        checks.append({"treatment": treatment, "check": "docker_rows_in_phases", "value": str(len([r for r in output if r["treatment"] == treatment])), "pass": str(bool(rows))})
    return output


def streaming_outputs(contexts: dict, checks: list[dict]) -> list[dict]:
    output = []
    for treatment, context in contexts.items():
        path = context["lifecycle"] / "streaming_counters.csv"
        grouped = defaultdict(lambda: {"bytes": 0, "shards": 0, "node_count": ""})
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                timestamp = dt(row["timestamp_utc"])
                phase = phase_for_time(timestamp, context["starts"], context["ends"])
                if not phase:
                    continue
                key = (phase, row["timestamp_utc"], row["sample"], row["node"], row["direction"])
                grouped[key]["bytes"] += int(row["bytes"])
                grouped[key]["shards"] += 1
                grouped[key]["node_count"] = row["node_count"]

        previous = {}
        for key in sorted(grouped, key=lambda item: (dt(item[1]), int(item[2]), item[3], item[4])):
            phase, timestamp_text, sample, node, direction = key
            value = grouped[key]["bytes"]
            series = (node, direction)
            prev = previous.get(series)
            reset = prev is not None and value < prev
            delta = 0 if prev is None or reset else value - prev
            timestamp = dt(timestamp_text)
            event_name = TRANSITION_EVENT.get(phase, "")
            event_times = context["named"].get(event_name, []) if event_name else []
            relative = (timestamp - event_times[0]).total_seconds() if len(event_times) == 1 else math.nan
            output.append(
                {
                    "treatment": treatment,
                    "phase": phase,
                    "event": event_name,
                    "timestamp_utc": iso(timestamp),
                    "event_relative_s": fmt(relative, 6),
                    "sample": sample,
                    "node": node,
                    "node_count": grouped[key]["node_count"],
                    "direction": direction,
                    "shards": grouped[key]["shards"],
                    "counter_bytes": value,
                    "increment_bytes": delta,
                    "counter_reset": reset,
                }
            )
            previous[series] = value
        checks.append({"treatment": treatment, "check": "stream_series_rows", "value": str(len([r for r in output if r["treatment"] == treatment])), "pass": str(bool(grouped))})
    return output


def tablet_outputs(context: dict, checks: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    lifecycle = context["lifecycle"]
    index_rows, placement_rows, size_rows = [], [], []
    with (lifecycle / "snapshot_index.csv").open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            timestamp = dt(row["timestamp"])
            phase = phase_for_time(timestamp, context["starts"], context["ends"])
            event_name = TRANSITION_EVENT.get(phase, "")
            event_times = context["named"].get(event_name, []) if event_name else []
            relative = (timestamp - event_times[0]).total_seconds() if len(event_times) == 1 else math.nan
            index_rows.append({"treatment": "tablets", "phase": phase, "event": event_name, "event_relative_s": fmt(relative, 6), **row})

    with (lifecycle / "tablets.csv").open(newline="", encoding="utf-8-sig") as handle:
        grouped = defaultdict(list)
        for row in csv.DictReader(handle):
            grouped[row["timestamp"]].append(row)
    previous_signature = None
    for timestamp_text in sorted(grouped, key=dt):
        rows = grouped[timestamp_text]
        canonical = "\n".join(
            "|".join(str(row.get(column, "")) for column in ("last_token", "replicas", "new_replicas", "stage", "transition", "resize_type", "resize_seq_number"))
            for row in sorted(rows, key=lambda item: int(item["last_token"]))
        )
        signature = hashlib.sha256(canonical.encode()).hexdigest()
        pending = sum(1 for row in rows if row["new_replicas"].strip() not in {"", "[]", "null", "None"})
        active = sum(
            1
            for row in rows
            if any(row[column].strip().lower() not in {"", "none", "null"} for column in ("stage", "transition", "resize_type"))
        )
        timestamp = dt(timestamp_text)
        phase = phase_for_time(timestamp, context["starts"], context["ends"])
        event_name = TRANSITION_EVENT.get(phase, "")
        event_times = context["named"].get(event_name, []) if event_name else []
        relative = (timestamp - event_times[0]).total_seconds() if len(event_times) == 1 else math.nan
        placement_rows.append(
            {
                "timestamp": timestamp_text,
                "phase": phase,
                "event_relative_s": fmt(relative, 6),
                "tablet_rows": len(rows),
                "pending_replica_rows": pending,
                "active_transition_rows": active,
                "placement_sha256": signature,
                "changed_from_previous": previous_signature is not None and signature != previous_signature,
            }
        )
        previous_signature = signature

    aggregates = defaultdict(lambda: {"replica_entries": 0, "reported_bytes": 0, "missing_rows": 0})
    with (lifecycle / "tablet_sizes.csv").open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            key = (row["timestamp"], row["host_id"])
            aggregates[key]["replica_entries"] += 1
            aggregates[key]["reported_bytes"] += int(row["size_bytes"])
            if row["missing_replicas"].strip() not in {"", "[]", "null", "None"}:
                aggregates[key]["missing_rows"] += 1
    for (timestamp_text, host_id), values in sorted(aggregates.items(), key=lambda item: (dt(item[0][0]), item[0][1])):
        timestamp = dt(timestamp_text)
        phase = phase_for_time(timestamp, context["starts"], context["ends"])
        event_name = TRANSITION_EVENT.get(phase, "")
        event_times = context["named"].get(event_name, []) if event_name else []
        relative = (timestamp - event_times[0]).total_seconds() if len(event_times) == 1 else math.nan
        size_rows.append(
            {
                "timestamp": timestamp_text,
                "phase": phase,
                "event_relative_s": fmt(relative, 6),
                "host_id": host_id,
                **values,
            }
        )

    incomplete = sum(
        1
        for row in index_rows
        if row["token_sets_equal"] != "True"
        or int(row["complete_size_groups"]) != int(row["tablet_rows"])
        or int(row["placement_replicas"]) != int(row["size_replicas"])
    )
    checks.append({"treatment": "tablets", "check": "snapshot_index_rows", "value": str(len(index_rows)), "pass": str(bool(index_rows))})
    checks.append({"treatment": "tablets", "check": "incomplete_size_snapshots", "value": str(incomplete), "pass": "True"})
    return index_rows, placement_rows, size_rows


def parse_ring_file(path: Path) -> tuple[list[dict], str]:
    rows = []
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            parts = line.split()
            # A joining node can report unknown load as the single field ``?``
            # instead of the usual two fields such as ``373.11 MB``.  Both
            # layouts still end with ownership and token, so seven fields are
            # sufficient for a valid ring row.
            if len(parts) < 7 or not re.fullmatch(r"\d+\.\d+\.\d+\.\d+", parts[0]):
                continue
            token = parts[-1]
            owns = parts[-2]
            if not re.fullmatch(r"-?\d+", token):
                continue
            rows.append(
                {
                    "address": parts[0],
                    "rack": parts[1],
                    "status": parts[2],
                    "state": parts[3],
                    "owns": owns,
                    "token": token,
                }
            )
    canonical = "\n".join(
        f"{row['address']}|{row['rack']}|{row['status']}|{row['state']}|{row['owns']}|{row['token']}"
        for row in sorted(rows, key=lambda item: (item["address"], int(item["token"])))
    )
    return rows, hashlib.sha256(canonical.encode()).hexdigest()


def vnode_ring_outputs(context: dict, checks: list[dict]) -> list[dict]:
    ring_dir = context["lifecycle"] / "ring"
    index_path = ring_dir / "index.csv"
    index_rows = load_csv(index_path)
    output = []
    bad_file_hashes = 0
    bad_address_counts = 0
    for position, index in enumerate(index_rows, 1):
        path = ring_dir / index["file"]
        if not path.is_file() or sha256_file(path).lower() != index["sha256"].lower():
            bad_file_hashes += 1
            continue
        rows, ownership_signature = parse_ring_file(path)
        if len(rows) != int(index["address_rows"]):
            bad_address_counts += 1
        timestamp = dt(index["start_utc"])
        phase = phase_for_time(timestamp, context["starts"], context["ends"])
        event_name = TRANSITION_EVENT.get(phase, "")
        event_times = context["named"].get(event_name, []) if event_name else []
        relative = (timestamp - event_times[0]).total_seconds() if len(event_times) == 1 else math.nan
        by_node = defaultdict(list)
        for row in rows:
            by_node[row["address"]].append(row)
        for address, node_rows in sorted(by_node.items()):
            owns_values = sorted({row["owns"] for row in node_rows})
            output.append(
                {
                    "sample": index["sample"],
                    "start_utc": index["start_utc"],
                    "end_utc": index["end_utc"],
                    "phase": phase,
                    "event_relative_s": fmt(relative, 6),
                    "address": address,
                    "token_rows": len(node_rows),
                    "up_normal_rows": sum(1 for row in node_rows if row["status"] == "Up" and row["state"] == "Normal"),
                    "owns_values": ";".join(owns_values),
                    "ownership_sha256": ownership_signature,
                    "source_file": index["file"],
                }
            )
        if position % 500 == 0:
            print(f"Ring analysis: {position}/{len(index_rows)} snapshots", flush=True)
    checks.append({"treatment": "vnodes", "check": "ring_index_rows", "value": str(len(index_rows)), "pass": str(bool(index_rows))})
    checks.append({"treatment": "vnodes", "check": "ring_bad_file_hashes", "value": str(bad_file_hashes), "pass": str(bad_file_hashes == 0)})
    checks.append({"treatment": "vnodes", "check": "ring_bad_address_counts", "value": str(bad_address_counts), "pass": str(bad_address_counts == 0)})
    return output


def contexts(raw_root: Path, checks: list[dict]) -> dict:
    result = {}
    for treatment, directory in TREATMENTS.items():
        lifecycle = raw_root / directory / "lifecycle_01"
        events_path = lifecycle / "events.jsonl"
        if not events_path.is_file():
            raise FileNotFoundError(events_path)
        events = read_events(events_path)
        starts, ends, named = event_maps(events)
        result[treatment] = {
            "lifecycle": lifecycle,
            "events": events,
            "starts": starts,
            "ends": ends,
            "named": named,
        }
        checks.append({"treatment": treatment, "check": "phase_start_count", "value": str(len(starts)), "pass": str(len(starts) == 5)})
        checks.append({"treatment": treatment, "check": "phase_end_count", "value": str(len(ends)), "pass": str(len(ends) == 5)})
    return result


def field_order(rows: list[dict]) -> list[str]:
    ordered = []
    for row in rows:
        for key in row:
            if key not in ordered:
                ordered.append(key)
    return ordered


def build_report(summaries: list[dict], checks: list[dict]) -> str:
    lines = [
        "# ScyllaDB Primary Comparison: Derived Analysis Checkpoint",
        "",
        f"Generated by `ScyllaDB-Primary-Analysis.py` version {SCRIPT_VERSION}.",
        "Raw evidence was read but not modified.",
        "",
        "## Phase-level workload summary",
        "",
        "| Treatment | Phase | Requests | Failures | Read p95 ms | Write p95 ms | Lag p95 ms | Lag max ms |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        lines.append(
            f"| {row['treatment']} | {row['phase']} | {row['requests']} | {row['failures']} | "
            f"{row['read_p95_ms']} | {row['write_p95_ms']} | {row['schedule_lag_p95_ms']} | {row['schedule_lag_max_ms']} |"
        )
    failed = [row for row in checks if row["pass"] != "True"]
    lines += [
        "",
        "## Automated checks",
        "",
        f"Checks recorded: {len(checks)}; checks requiring review: {len(failed)}.",
        "A nonzero tablet `incomplete_size_snapshots` count is retained as an observed transition phenomenon and is not automatically treated as a failed acquisition.",
        "",
        "## Interpretation guardrails",
        "",
        "- Request latency and client scheduling lag are separate quantities.",
        "- Streaming-counter bytes and Docker cumulative network I/O are separate measurement layers.",
        "- These files describe one completed lifecycle per treatment; samples within a lifecycle are not independent experimental replicates.",
        "- Derived files do not replace the raw evidence.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    raw_root = args.raw_root.resolve()
    output = args.output.resolve()
    if output.exists():
        raise FileExistsError(f"Output directory already exists: {output}")
    output.mkdir(parents=True)

    checks: list[dict] = []
    context = contexts(raw_root, checks)
    summaries, request_bins = workload_outputs(raw_root, context, checks)
    resources = resource_outputs(context, checks)
    streams = streaming_outputs(context, checks)
    tablet_index, tablet_placement, tablet_sizes = tablet_outputs(context["tablets"], checks)
    vnode_ring = vnode_ring_outputs(context["vnodes"], checks)

    events_output = []
    for treatment, item in context.items():
        for row in item["events"]:
            events_output.append(
                {
                    "treatment": treatment,
                    "line": row["_line"],
                    "event": row.get("event", ""),
                    "phase": row.get("phase", ""),
                    "host_utc": row.get("host_utc", ""),
                    "node4_container_started_utc": row.get("node4_container_started_utc") or "",
                }
            )

    datasets = {
        "phase_summary.csv": summaries,
        "transition_request_5s_bins.csv": request_bins,
        "resource_timeseries.csv": resources,
        "streaming_timeseries.csv": streams,
        "tablet_snapshot_index.csv": tablet_index,
        "tablet_placement_summary.csv": tablet_placement,
        "tablet_replica_size_by_node.csv": tablet_sizes,
        "vnode_ring_by_node.csv": vnode_ring,
        "events.csv": events_output,
        "quality_checks.csv": checks,
    }
    for filename, rows in datasets.items():
        if rows:
            write_csv(output / filename, field_order(rows), rows)

    report = build_report(summaries, checks)
    (output / "analysis_checkpoint.md").write_text(report, encoding="utf-8")

    manifest = {
        "script": Path(__file__).name,
        "script_version": SCRIPT_VERSION,
        "raw_root": str(raw_root),
        "output_directory": str(output),
        "generated_files": {},
    }
    for path in sorted(output.iterdir()):
        if path.is_file() and path.name != "manifest.json":
            manifest["generated_files"][path.name] = {
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    archive = Path(shutil.make_archive(str(output), "zip", root_dir=output))

    failed = [row for row in checks if row["pass"] != "True"]
    print("RESULTS")
    print(f"analysis_directory={output}")
    print(f"archive={archive}")
    print(f"phase_summary_rows={len(summaries)}")
    print(f"transition_bin_rows={len(request_bins)}")
    print(f"resource_rows={len(resources)}")
    print(f"streaming_rows={len(streams)}")
    print(f"tablet_snapshot_rows={len(tablet_index)}")
    print(f"tablet_placement_rows={len(tablet_placement)}")
    print(f"tablet_size_node_rows={len(tablet_sizes)}")
    print(f"vnode_ring_node_rows={len(vnode_ring)}")
    print(f"quality_checks={len(checks)}")
    print(f"checks_requiring_review={len(failed)}")
    for row in failed:
        print(f"review={row['treatment']}|{row['check']}|{row['value']}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR {type(exc).__name__}: {exc}", file=sys.stderr)
        raise
