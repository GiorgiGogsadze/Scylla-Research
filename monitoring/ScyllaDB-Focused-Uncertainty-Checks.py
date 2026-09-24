#!/usr/bin/env python3
"""Focused, read-only checks for the remaining primary-study questions."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
THRESHOLDS_S = (1, 5, 10, 20)


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def fmt(value: float | None) -> str:
    return "none" if value is None else f"{value:.3f}"


def load_events(path: Path) -> tuple[dict[str, tuple[datetime, datetime]], dict[str, datetime]]:
    starts: dict[str, datetime] = {}
    intervals: dict[str, tuple[datetime, datetime]] = {}
    triggers: dict[str, datetime] = {}
    # PowerShell-created JSONL commonly begins with a UTF-8 BOM.
    with path.open(encoding="utf-8-sig") as handle:
        for line in handle:
            if not line.strip():
                continue
            event = json.loads(line)
            name = event["event"]
            phase = event.get("phase")
            when = parse_time(event["host_utc"])
            if name == "phase_start" and phase:
                starts[phase] = when
            elif name == "phase_end" and phase:
                intervals[phase] = (starts[phase], when)
            elif name == "join_command_start":
                triggers["scaleout"] = when
            elif name == "decommission_command_start":
                triggers["scalein"] = when
    return intervals, triggers


@dataclass
class LagStats:
    total: int = 0
    affected: int = 0
    first_start_s: float | None = None
    last_start_s: float | None = None
    last_completion_s: float | None = None
    max_lag_s: float = 0.0
    run_count: int = 0
    run_first_s: float | None = None
    run_last_completion_s: float | None = None
    longest_count: int = 0
    longest_span_s: float = 0.0
    previous_index: int | None = None

    def finish_run(self) -> None:
        if self.run_count and self.run_first_s is not None and self.run_last_completion_s is not None:
            span = self.run_last_completion_s - self.run_first_s
            if self.run_count > self.longest_count or (
                self.run_count == self.longest_count and span > self.longest_span_s
            ):
                self.longest_count = self.run_count
                self.longest_span_s = span
        self.run_count = 0
        self.run_first_s = None
        self.run_last_completion_s = None


def analyze_lag(path: Path, event_time: datetime) -> dict[int, LagStats]:
    stats = {threshold: LagStats() for threshold in THRESHOLDS_S}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            start = parse_time(row["timestamp_utc"])
            if start < event_time:
                continue
            end = parse_time(row["end_timestamp_utc"])
            index = int(row["index"])
            lag_s = (float(row["start_offset_ms"]) - float(row["scheduled_offset_ms"])) / 1000
            start_s = (start - event_time).total_seconds()
            end_s = (end - event_time).total_seconds()
            for threshold, item in stats.items():
                item.total += 1
                if lag_s >= threshold:
                    item.affected += 1
                    item.max_lag_s = max(item.max_lag_s, lag_s)
                    if item.first_start_s is None:
                        item.first_start_s = start_s
                    item.last_start_s = start_s
                    item.last_completion_s = end_s
                    if item.previous_index is None or index != item.previous_index + 1:
                        item.finish_run()
                        item.run_first_s = start_s
                    item.run_count += 1
                    item.run_last_completion_s = end_s
                    item.previous_index = index
                else:
                    item.finish_run()
                    item.previous_index = None
    for item in stats.values():
        item.finish_run()
    return stats


def replica_ids(value: str) -> frozenset[str]:
    return frozenset(match.lower() for match in UUID_RE.findall(value or ""))


@dataclass(frozen=True)
class Snapshot:
    timestamp: datetime
    metadata: frozenset[tuple[str, ...]]
    placements: frozenset[tuple[str, str]]
    replicas_by_boundary: dict[str, frozenset[str]]

    @property
    def boundaries(self) -> frozenset[str]:
        return frozenset(self.replicas_by_boundary)


def snapshots(path: Path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        current_time: str | None = None
        group: list[dict[str, str]] = []
        for row in reader:
            if current_time is not None and row["timestamp"] != current_time:
                yield make_snapshot(current_time, group)
                group = []
            current_time = row["timestamp"]
            group.append(row)
        if current_time is not None:
            yield make_snapshot(current_time, group)


def make_snapshot(timestamp: str, rows: list[dict[str, str]]) -> Snapshot:
    metadata: set[tuple[str, ...]] = set()
    placements: set[tuple[str, str]] = set()
    replicas_by_boundary: dict[str, frozenset[str]] = {}
    fields = ("last_token", "replicas", "new_replicas", "stage", "transition", "resize_type", "resize_seq_number")
    for row in rows:
        metadata.add(tuple(row.get(field, "") for field in fields))
        token = row["last_token"]
        replicas = replica_ids(row.get("replicas", ""))
        replicas_by_boundary[token] = replicas
        placements.update((token, host) for host in replicas)
    return Snapshot(parse_time(timestamp), frozenset(metadata), frozenset(placements), replicas_by_boundary)


def delta(previous: Snapshot, current: Snapshot) -> dict[str, int]:
    shared = previous.boundaries & current.boundaries
    return {
        "shared_changed": sum(previous.replicas_by_boundary[key] != current.replicas_by_boundary[key] for key in shared),
        "boundaries_added": len(current.boundaries - previous.boundaries),
        "boundaries_removed": len(previous.boundaries - current.boundaries),
        "assignments_added": len(current.placements - previous.placements),
        "assignments_removed": len(previous.placements - current.placements),
    }


def analyze_tablet_churn(
    tablets_path: Path,
    intervals: dict[str, tuple[datetime, datetime]],
    triggers: dict[str, datetime],
) -> tuple[str, dict[str, dict[str, int]], dict[str, tuple[Snapshot, Snapshot, dict[str, int]]]]:
    all_hosts: set[str] = set()
    pre_join_hosts: set[str] = set()
    for snap in snapshots(tablets_path):
        hosts = {host for _, host in snap.placements}
        all_hosts.update(hosts)
        if snap.timestamp < triggers["scaleout"]:
            pre_join_hosts.update(hosts)
    candidates = all_hosts - pre_join_hosts
    node4 = sorted(candidates)[0] if len(candidates) == 1 else "unknown"

    totals = {
        phase: defaultdict(int)
        for phase in ("scaleout", "scalein")
    }
    before: dict[str, Snapshot | None] = {"scaleout": None, "scalein": None}
    endpoint: dict[str, Snapshot | None] = {"scaleout": None, "scalein": None}
    previous: Snapshot | None = None
    for snap in snapshots(tablets_path):
        for phase in ("scaleout", "scalein"):
            trigger = triggers[phase]
            if snap.timestamp < trigger:
                before[phase] = snap
            start, end = intervals[phase]
            if start <= snap.timestamp <= end:
                endpoint[phase] = snap
            if previous is not None and trigger <= snap.timestamp <= end:
                change = delta(previous, snap)
                totals[phase]["metadata_changed_steps"] += int(previous.metadata != snap.metadata)
                totals[phase]["placement_changed_steps"] += int(previous.placements != snap.placements)
                totals[phase]["tablet_count_changed_steps"] += int(len(previous.boundaries) != len(snap.boundaries))
                for key, value in change.items():
                    totals[phase][key] += value
        previous = snap

    comparisons: dict[str, tuple[Snapshot, Snapshot, dict[str, int]]] = {}
    for phase in ("scaleout", "scalein"):
        if before[phase] is None or endpoint[phase] is None:
            raise RuntimeError(f"Missing tablet endpoint for {phase}")
        comparisons[phase] = (before[phase], endpoint[phase], delta(before[phase], endpoint[phase]))
    return node4, totals, comparisons


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-root", type=Path, default=Path("data/raw"))
    args = parser.parse_args()

    roots = {
        "tablets": args.raw_root / "primaryb1tablets" / "lifecycle_01",
        "vnodes": args.raw_root / "primaryb1vnodes" / "lifecycle_01",
    }
    event_data = {name: load_events(root / "events.jsonl") for name, root in roots.items()}

    print("RESULTS")
    for treatment in ("tablets", "vnodes"):
        intervals, triggers = event_data[treatment]
        for phase in ("scaleout", "scalein"):
            measured = roots[treatment] / phase / "measured.csv"
            for threshold, item in analyze_lag(measured, triggers[phase]).items():
                percentage = 100 * item.affected / item.total if item.total else 0
                print(
                    f"BACKLOG treatment={treatment} phase={phase} threshold_s={threshold} "
                    f"post_event_requests={item.total} affected={item.affected} affected_pct={percentage:.3f} "
                    f"first_start_s={fmt(item.first_start_s)} last_start_s={fmt(item.last_start_s)} "
                    f"last_completion_s={fmt(item.last_completion_s)} max_lag_s={item.max_lag_s:.3f} "
                    f"longest_run_requests={item.longest_count} longest_run_span_s={item.longest_span_s:.3f}"
                )

    tablet_intervals, tablet_triggers = event_data["tablets"]
    node4, totals, comparisons = analyze_tablet_churn(
        roots["tablets"] / "tablets.csv", tablet_intervals, tablet_triggers
    )
    print(f"TABLET_NODE4 host_id={node4}")
    for phase in ("scaleout", "scalein"):
        values = totals[phase]
        print(
            f"TABLET_CHURN phase={phase} metadata_changed_steps={values['metadata_changed_steps']} "
            f"placement_changed_steps={values['placement_changed_steps']} "
            f"tablet_count_changed_steps={values['tablet_count_changed_steps']} "
            f"shared_boundary_changes_sum={values['shared_changed']} "
            f"boundary_adds_sum={values['boundaries_added']} boundary_removes_sum={values['boundaries_removed']} "
            f"assignment_adds_sum={values['assignments_added']} assignment_removes_sum={values['assignments_removed']}"
        )
        first, last, change = comparisons[phase]
        first_node4 = sum(host == node4 for _, host in first.placements)
        last_node4 = sum(host == node4 for _, host in last.placements)
        print(
            f"TABLET_ENDPOINT phase={phase} tablets={len(first.boundaries)}->{len(last.boundaries)} "
            f"assignments={len(first.placements)}->{len(last.placements)} "
            f"node4_assignments={first_node4}->{last_node4} "
            f"shared_boundary_changes={change['shared_changed']} "
            f"boundaries_added={change['boundaries_added']} boundaries_removed={change['boundaries_removed']} "
            f"assignments_added={change['assignments_added']} assignments_removed={change['assignments_removed']}"
        )
    print("NOTE tablet_churn_counts_are_metadata_transitions_not_physical_transfer_counts=True")
    print("NOTE split_merge_boundary_changes_can_inflate_assignment_add_remove_counts=True")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
