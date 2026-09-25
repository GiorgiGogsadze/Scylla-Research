# ScyllaDB: Tablets vs. Vnodes

What happens when you add or remove a database node while it is handling requests?

This project compares **tablets** and **vnodes**, two ways ScyllaDB distributes data across nodes. It follows how data moves, how evenly nodes share it, and how these changes affect the client.

## The experiment

Each approach went through the same lifecycle:

1. Measure a three-node cluster.
2. Add a fourth node while requests continue.
3. Measure the four-node cluster.
4. Decommission the fourth node while requests continue.
5. Measure the remaining three nodes.

Each dataset contained **750,000 rows** with a replication factor of **2**. Both runs used the same sequence of **1.08 million measured operations**, targeting **500 operations per second** with **80% reads and 20% writes**. Neither run reported workload failures.

The experiments ran on a Windows laptop using Docker. This is a detailed case study with one lifecycle per approach. It provides observations and explanations, rather than a general ranking of which approach is faster.

## Main findings

- **A settled topology does not mean all work is finished.** Data placement, background storage work, and client recovery can finish at different times.
- **Tablets achieved more even four-node placement in this experiment.** Their ranges split from 32 to 64 and merged back to 32, with some replica assignments changing along the way.
- **The larger client backlog depended on the transition.** Tablets had the larger expansion backlog; vnodes had the larger contraction backlog.
- **Vnode contraction had a delayed slowdown.** Compaction began about five minutes after receiving data and coincided with heavy disk activity and a larger client backlog.
- **The storage audit explained an apparent replica excess.** All 72,501 extra entries in the selected vnode SSTables came from repeated keys across files on the same node. Every key appeared on exactly two nodes in the audited files.
- **Monitoring counters need context.** The collected streaming counters did not cover the full tablet file-transfer path, so their totals cannot directly compare total data movement.

## What is in this repository?

| Location                                                                 | Contents                                                 |
| ------------------------------------------------------------------------ | -------------------------------------------------------- |
| [`Paper/`](Paper/)                                                       | Research paper and LaTeX sources                         |
| [`monitoring/`](monitoring/)                                             | Data-loading, workload, monitoring, and analysis scripts |
| [`docker/`](docker/)                                                     | Docker Compose configurations                            |
| [`data/analysis/primary-comparison/`](data/analysis/primary-comparison/) | Derived results and analysis checks                      |
| [`figures/`](figures/)                                                   | Figure-generation code, data, and images                 |

Start with the [research paper](Paper/paper.pdf) for the full results, explanations, and limitations.

## Data and reproducibility

The repository contains code and derived results. Approximately **949 MiB of raw observations** are stored separately and are available on request through [GitHub Issues](https://github.com/GiorgiGogsadze/Scylla-Research/issues).

The paper documents the available evidence and the additional materials needed to reproduce the full offline SSTable audit. The experiments used a shared laptop, different starting conditions, and one run per approach. Repeated experiments would help establish how consistently the performance differences occur.

## Author

**Giorgi Gogsadze**  
Kutaisi International University, Georgia
