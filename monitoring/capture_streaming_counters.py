"""Sample ScyllaDB's per-shard streaming-byte counters during a 3-to-4 join.

Run inside the tablet-monitor Compose image, on the same network as the nodes.
The first three nodes must answer every sample. Node 4 is polled only after
the host creates --fourth-trigger-file following its container start. Before
node 4 supplies its first complete counter set, connection failures and
not-yet-published counters are treated as startup readiness states while
nodes 1-3 continue to be recorded. After node 4 has answered completely,
any missing response or counter is an error, not a silent gap.
"""

import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import re
import time
from typing import Any, Literal, Mapping, TypeAlias
from urllib.request import urlopen


METRIC = re.compile(
    r'^scylla_streaming_total_(incoming|outgoing)_bytes\{([^}]*)\}\s+([^\s]+)$'
)
SHARD = re.compile(r'(?:^|,)\s*shard="(\d+)"(?:,|$)')
NODES = ('r-scylla1', 'r-scylla2', 'r-scylla3', 'r-scylla4')
FieldName: TypeAlias = Literal[
    'timestamp_utc',
    'sample',
    'node',
    'node_count',
    'shard',
    'direction',
    'bytes',
]
FIELDS: tuple[FieldName, ...] = (
    'timestamp_utc',
    'sample',
    'node',
    'node_count',
    'shard',
    'direction',
    'bytes',
)


class CountersNotReady(RuntimeError):
    """The metrics endpoint is reachable but its complete counters are not ready."""


def read_counters(node: str) -> dict[tuple[str, str], int]:
    with urlopen(f'http://{node}:9180/metrics', timeout=5) as response:
        lines = response.read().decode('utf-8').splitlines()
    counters = {}
    for line in lines:
        match = METRIC.fullmatch(line)
        if match is None:
            continue
        direction, labels, raw_value = match.groups()
        shard_match = SHARD.search(labels)
        if shard_match is None:
            raise ValueError(f'{node}: streaming metric lacks shard label: {line}')
        key = (shard_match.group(1), direction)
        if key in counters:
            raise ValueError(f'{node}: duplicate counter {key}')
        counters[key] = int(Decimal(raw_value))
    if not counters:
        raise CountersNotReady(f'{node}: no streaming-byte counters in metrics response')
    shards = {shard for shard, _ in counters}
    expected_pairs = {
        (shard, direction)
        for shard in shards
        for direction in ('incoming', 'outgoing')
    }
    if set(counters) != expected_pairs:
        raise CountersNotReady(
            f'{node}: incomplete incoming/outgoing counter set for observed shards'
        )
    return counters


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--fourth-trigger-file', type=Path, required=True)
    parser.add_argument('--interval-seconds', type=float, default=1.0)
    parser.add_argument('--duration-seconds', type=float)
    args = parser.parse_args()
    if args.interval_seconds <= 0 or (
        args.duration_seconds is not None and args.duration_seconds <= 0
    ):
        parser.error('interval and duration must be positive')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists():
        parser.error(f'output already exists: {args.output}')

    started = time.monotonic()
    fourth_seen = False
    fourth_wait_logged = False
    previous_node_count = None
    sample = 0
    try:
        with args.output.open('x', newline='', encoding='utf-8') as output:
            writer = csv.DictWriter(output, fieldnames=FIELDS)
            writer.writeheader()
            output.flush()
            print(f'Saving streaming counters to {args.output}', flush=True)
            while True:
                cycle_started = time.monotonic()
                timestamp = datetime.now(timezone.utc).isoformat()
                readings = {}
                expected_keys = None
                for node in NODES:
                    fourth_triggered = args.fourth_trigger_file.exists()
                    if node == 'r-scylla4' and not (fourth_seen or fourth_triggered):
                        continue
                    try:
                        counters = read_counters(node)
                        counter_keys = set(counters)
                        if expected_keys is None:
                            expected_keys = counter_keys
                        elif counter_keys != expected_keys:
                            message = (
                                f'{node}: counter keys {sorted(counter_keys)} do not match '
                                f'established-node keys {sorted(expected_keys)}'
                            )
                            if node == 'r-scylla4' and not fourth_seen:
                                raise CountersNotReady(message)
                            raise ValueError(message)
                        readings[node] = counters
                    except (OSError, CountersNotReady) as error:
                        if node == 'r-scylla4' and not fourth_seen:
                            if not fourth_wait_logged:
                                print(
                                    f'Node 4 counters not ready at {timestamp}: {error}; '
                                    'continuing with nodes 1-3.',
                                    flush=True,
                                )
                                fourth_wait_logged = True
                            continue
                        raise
                if 'r-scylla4' in readings:
                    fourth_seen = True
                node_count = len(readings)
                for node, counters in readings.items():
                    for (shard, direction), count in sorted(counters.items()):
                        row: Mapping[FieldName, Any] = {
                            'timestamp_utc': timestamp,
                            'sample': sample,
                            'node': node,
                            'node_count': node_count,
                            'shard': shard,
                            'direction': direction,
                            'bytes': count,
                        }
                        writer.writerow(row)
                output.flush()
                if sample == 0 or node_count != previous_node_count:
                    print(
                        f'Sample {sample}: {node_count} nodes at {timestamp}',
                        flush=True,
                    )
                previous_node_count = node_count
                sample += 1
                if (
                    args.duration_seconds is not None
                    and time.monotonic() - started >= args.duration_seconds
                ):
                    break
                time.sleep(
                    max(0, args.interval_seconds - (time.monotonic() - cycle_started))
                )
    except KeyboardInterrupt:
        print(f'Stopped after {sample} complete samples.', flush=True)


if __name__ == '__main__':
    main()
