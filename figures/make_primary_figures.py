from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path('extracted-primary-comparison')
OUT = Path('analysis_artifacts/figures')
OUT.mkdir(parents=True, exist_ok=True)

plt.rcParams.update({
    'figure.dpi': 150,
    'savefig.dpi': 300,
    'font.size': 9,
    'axes.titlesize': 10,
    'axes.labelsize': 9,
    'legend.fontsize': 8,
    'axes.spines.top': False,
    'axes.spines.right': False,
})

COLORS = {'tablets': '#2563eb', 'vnodes': '#dc2626'}
PHASES = ['baseline', 'scaleout', 'four_node', 'scalein', 'final_three']
LABELS = ['Baseline', 'Scale-out', 'Four-node', 'Scale-in', 'Final three']


def save(fig, name):
    fig.tight_layout()
    fig.savefig(OUT / f'{name}.png', bbox_inches='tight')
    fig.savefig(OUT / f'{name}.svg', bbox_inches='tight')
    plt.close(fig)


phase = pd.read_csv(ROOT / 'phase_summary.csv')

# Figure 1: request latency tails by phase.
fig, axes = plt.subplots(1, 2, figsize=(9.2, 3.5), sharey=True)
x = np.arange(len(PHASES)); width = 0.36
for ax, operation in zip(axes, ['read', 'write']):
    for offset, treatment in [(-width/2, 'tablets'), (width/2, 'vnodes')]:
        d = phase[phase.treatment == treatment].set_index('phase').loc[PHASES]
        ax.bar(x + offset, d[f'{operation}_p95_ms'], width, label=treatment.capitalize(), color=COLORS[treatment])
    ax.set_title(f'{operation.capitalize()} p95 request latency')
    ax.set_xticks(x, LABELS, rotation=25, ha='right')
    ax.set_ylabel('Latency (ms)')
    ax.grid(axis='y', alpha=.25)
axes[0].legend(frameon=False)
fig.suptitle('Request-latency tails remained in the millisecond range')
save(fig, 'figure_1_phase_latency_p95')

# Figure 2: phase-wide client scheduling backlog.
fig, axes = plt.subplots(1, 2, figsize=(9.2, 3.5), sharey=True)
for ax, metric, title in [
    (axes[0], 'schedule_lag_p95_ms', 'p95 scheduled-start lag'),
    (axes[1], 'schedule_lag_max_ms', 'Maximum scheduled-start lag'),
]:
    for offset, treatment in [(-width/2, 'tablets'), (width/2, 'vnodes')]:
        d = phase[phase.treatment == treatment].set_index('phase').loc[PHASES]
        ax.bar(x + offset, d[metric] / 1000, width, label=treatment.capitalize(), color=COLORS[treatment])
    ax.set_title(title)
    ax.set_xticks(x, LABELS, rotation=25, ha='right')
    ax.set_ylabel('Lag (s)')
    ax.grid(axis='y', alpha=.25)
axes[0].legend(frameon=False)
fig.suptitle('Transition direction reversed the dominant client backlog')
save(fig, 'figure_2_phase_scheduling_lag')

# Figure 3: 5-second scheduled-time transition traces.
bins = pd.read_csv(ROOT / 'transition_request_5s_bins.csv')
bins = bins[(bins.alignment == 'scheduled') & (bins.bin_start_s >= -120) & (bins.bin_start_s <= 450)]
fig, axes = plt.subplots(2, 1, figsize=(9.2, 6.2), sharex=True)
for ax, p, title in zip(axes, ['scaleout', 'scalein'], ['3→4 scale-out', '4→3 scale-in']):
    for treatment in ['tablets', 'vnodes']:
        d = bins[(bins.phase == p) & (bins.treatment == treatment)].sort_values('bin_start_s')
        ax.plot(d.bin_start_s, d.schedule_lag_p95_ms / 1000, lw=1.6, label=treatment.capitalize(), color=COLORS[treatment])
    ax.axvline(0, color='black', lw=.9, ls='--', label='Topology command' if p == 'scaleout' else None)
    ax.set_title(title)
    ax.set_ylabel('p95 lag (s)')
    ax.grid(alpha=.25)
axes[-1].set_xlabel('Scheduled seconds relative to topology command')
axes[0].legend(frameon=False, ncol=3)
fig.suptitle('Scheduling backlog was event-local for joins but delayed during vnode departure')
save(fig, 'figure_3_transition_lag_timeline')

# Figure 4: conservative onset of verified 60-second stability window.
conv = pd.DataFrame([
    ('Scale-out', 'tablets', 51.264793),
    ('Scale-out', 'vnodes', 22.566426),
    ('Scale-in', 'tablets', 17.691389),
    ('Scale-in', 'vnodes', 11.323197),
], columns=['transition', 'treatment', 'seconds'])
fig, ax = plt.subplots(figsize=(6.8, 3.6))
for offset, treatment in [(-width/2, 'tablets'), (width/2, 'vnodes')]:
    d = conv[conv.treatment == treatment].set_index('transition').loc[['Scale-out', 'Scale-in']]
    bars = ax.bar(np.arange(2) + offset, d.seconds, width, label=treatment.capitalize(), color=COLORS[treatment])
    ax.bar_label(bars, fmt='%.1f s', padding=3, fontsize=8)
ax.set_xticks(np.arange(2), ['3→4 scale-out', '4→3 scale-in'])
ax.set_ylabel('Seconds after command start')
ax.set_title('Earliest observed start of verified 60-second stable window')
ax.grid(axis='y', alpha=.25)
ax.legend(frameon=False)
save(fig, 'figure_4_convergence_onset')

# Figure 5: node-4 directional deltas over its observed measured-phase window.
stream = pd.read_csv(ROOT / 'streaming_timeseries.csv')
resource = pd.read_csv(ROOT / 'resource_timeseries.csv')
movement_rows = []
for transition, phase_name, direction, net_column in [
    ('Scale-out', 'scaleout', 'incoming', 'net_rx_bytes'),
    ('Scale-in', 'scalein', 'outgoing', 'net_tx_bytes'),
]:
    for treatment in ['tablets', 'vnodes']:
        s = stream[(stream.treatment == treatment) & (stream.phase == phase_name) &
                   (stream.node == 'r-scylla4') & (stream.direction == direction)].sort_values('sample')
        r = resource[(resource.treatment == treatment) & (resource.phase == phase_name) &
                     (resource.node == 'r-scylla4')].sort_values('sample')
        assert len(s) > 1 and len(r) > 1
        assert not s.counter_reset.any() and not r.counter_reset.any()
        movement_rows.append((transition, treatment.capitalize(), 'Scylla stream counter',
                              (s.counter_bytes.iloc[-1] - s.counter_bytes.iloc[0]) / 1e6))
        movement_rows.append((transition, treatment.capitalize(), 'Docker gross direction',
                              (r[net_column].iloc[-1] - r[net_column].iloc[0]) / 1e6))
movement = pd.DataFrame(movement_rows, columns=['transition','treatment','metric','megabytes'])
fig, axes = plt.subplots(1, 2, figsize=(9.2, 3.7), sharey=True)
for ax, transition in zip(axes, ['Scale-out','Scale-in']):
    d=movement[movement.transition==transition]
    pos=np.arange(2)
    for off, metric, color in [(-width/2,'Scylla stream counter','#64748b'),(width/2,'Docker gross direction','#0f766e')]:
        v=d[d.metric==metric].set_index('treatment').loc[['Tablets','Vnodes']]
        ax.bar(pos+off,v.megabytes,width,label=metric,color=color)
    ax.set_yscale('log')
    ax.set_xticks(pos,['Tablets','Vnodes'])
    ax.set_title(transition)
    ax.set_ylabel('Observed bytes (MB, log scale)')
    ax.grid(axis='y',alpha=.25)
handles, labels = axes[0].get_legend_handles_labels()
fig.legend(handles, labels, loc='lower center', bbox_to_anchor=(.5, -.04), ncol=2, frameon=False)
fig.suptitle('Node-4 measured-window counters and gross interface traffic')
save(fig, 'figure_5_movement_observability')

# Figure 6: endpoint balance using each mechanism's native placement measure.
tablet_values = {
    'Baseline': [550974418, 525354438, 525720788],
    'Four-node': [401861023, 400458341, 400686101, 399372064],
    'Final three': [551974438, 525773936, 526231881],
}
vnode_values = {
    'Baseline': [63.78, 65.48, 70.74],
    'Four-node': [47.08, 53.40, 50.86, 48.67],
    'Final three': [63.78, 65.48, 70.74],
}
fig, axes = plt.subplots(1, 2, figsize=(9.2, 3.8))
for i, (checkpoint, vals) in enumerate(tablet_values.items()):
    for j, value in enumerate(vals):
        axes[0].scatter(i + (j-(len(vals)-1)/2)*.10, value/1e6, s=38, color=COLORS['tablets'])
axes[0].set_xticks(range(3), tablet_values.keys(), rotation=20, ha='right')
axes[0].set_ylabel('Reported replica bytes per node (MB)')
axes[0].set_title('Tablets: node-level replica-size observations')
axes[0].grid(axis='y',alpha=.25)
for i, (checkpoint, vals) in enumerate(vnode_values.items()):
    for j, value in enumerate(vals):
        axes[1].scatter(i + (j-(len(vals)-1)/2)*.10, value, s=38, color=COLORS['vnodes'])
axes[1].set_xticks(range(3), vnode_values.keys(), rotation=20, ha='right')
axes[1].set_ylabel('Effective RF=2 ownership per node (%)')
axes[1].set_title('Vnodes: ring-reported effective ownership')
axes[1].grid(axis='y',alpha=.25)
fig.suptitle('Endpoint balance must be compared using mechanism-appropriate measures')
save(fig, 'figure_6_endpoint_balance')

print('RESULTS')
for path in sorted(OUT.iterdir()):
    print(f'{path.name} bytes={path.stat().st_size}')
