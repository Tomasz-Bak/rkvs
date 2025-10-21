import json
import os
import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import re
import matplotlib.patheffects as pe


def parse_generic_results(filepath):
    """
    Parses a benchmark results JSON file.
    """
    with open(filepath, 'r') as f:
        raw_data = json.load(f)

    parsed_data = []
    for entry in raw_data:
        name = entry['scenario_name']

        try:
            if name.startswith("Batch "): # Batch operations benchmark format
                # e.g., "Batch Set (Size 50) (AllOrNothing) on 100k-key Namespace"
                match = re.match(r"Batch (Set|Get|Delete) \(Size (\d+)\) \((AllOrNothing|BestEffort)\) on (.*)", name)
                if match:
                    operation = match.group(1)
                    batch_size = int(match.group(2))
                    batch_mode = match.group(3)

                    parsed_data.append({
                        'bench_type': 'Batch',
                        'operation': operation,
                        'batch_size': batch_size,
                        'batch_mode': batch_mode,
                        'avg_latency_per_item_us': entry['avg_latency_per_item_ns'] / 1000,
                        'avg_latency_per_op_us': entry['avg_latency_per_op_ns'] / 1000,
                    })
                else:
                    print(f"⚠️  Could not parse batch scenario name: '{name}'. Skipping.")
                    continue
            elif name.startswith("Concurrent Batch "): # Concurrent Batch operations benchmark format
                # e.g., "Concurrent Batch Set (Size 50) (AllOrNothing) @ 8 Concurrency (1 Shard(s))"
                match = re.match(r"Concurrent Batch (Set|Get|Delete) \(Size (\d+)\) \((AllOrNothing|BestEffort)\) @ (\d+) Concurrency \((\d+) Shard\(s\)\)", name)
                if match:
                    operation = match.group(1)
                    batch_size = int(match.group(2))
                    batch_mode = match.group(3)
                    concurrency = int(match.group(4))
                    shard_count = int(match.group(5))

                    parsed_data.append({
                        'bench_type': 'Concurrent Batch', 'operation': operation, 'batch_size': batch_size,
                        'batch_mode': batch_mode, 'concurrency': concurrency, 'shard_count': shard_count,
                        'avg_latency_per_item_us': entry['avg_latency_per_item_ns'] / 1000,
                        'avg_latency_per_op_us': entry['avg_latency_per_op_ns'] / 1000,
                    })

            elif " on " in name: # Operations benchmark format
                # e.g., "Sequential Set (Insert) on 10k-key Namespace"
                op_part, context_part = name.split(" on ")

                op_part_split = op_part.split(' ')
                bench_type = op_part_split[0]
                operation_type = ' '.join(op_part_split[1:])

                context_split = context_part.split(' ')
                size_str = context_split[0]

                if 'k-key' in size_str:
                    num = int(size_str.replace('k-key', ''))
                    size_sort_key = num * 1000
                    size_label = f"{num}k Keys"
                elif 'M-key' in size_str:
                    num = int(size_str.replace('M-key', ''))
                    size_sort_key = num * 1_000_000
                    size_label = f"{num}M Keys"
                else:
                    size_sort_key = 0
                    size_label = "Unknown"

                    shard_count_match = re.search(r'\((\d+)\s+Shard\(s\)\)', context_part)
                    shard_count = int(shard_count_match.group(1)) if shard_count_match else 1

                    parsed_data.append({
                        'bench_type': bench_type,
                        'operation': operation_type,
                        'size_label': size_label,
                        'shard_count': shard_count,
                        'avg_latency_us': entry['avg_latency_ns'] / 1000,
                        'avg_deviation_percent': entry.get('avg_deviation_percent'),
                    })

            elif " @ " in name: # Concurrent benchmark format
                # e.g., "80-20 Read-Heavy Workload @ 8 Concurrency (1 Shard(s))"
                workload_part, rest = name.split(" @ ")
                workload_name = workload_part.replace(" Workload", "")

                concurrency_part, shard_part = rest.split(" Concurrency ")
                concurrency = int(concurrency_part.strip())

                shard_count = int(shard_part.replace("(", "").replace(" Shard(s))", ""))

                parsed_data.append({
                    'bench_type': 'Concurrent',
                    'workload': workload_name,
                    'concurrency': concurrency,
                    'shard_count': shard_count,
                    'avg_latency_us': entry['avg_latency_ns'] / 1000,
                    'ops_per_sec': entry.get('ops_per_sec', 0), # Use .get for backward compatibility
                })

        except ValueError:
            print(f"⚠️  Could not parse scenario name: '{name}'. Skipping.")
            continue

    return pd.DataFrame(parsed_data)

def parse_operations_results(filepath):
    """Parses results from operations_bench_results.json."""
    with open(filepath, 'r') as f:
        raw_data = json.load(f)

    parsed_data = []
    for entry in raw_data:
        name = entry['scenario_name']
        # e.g., "Sequential Set (Insert) on 10k-key Namespace"
        match = re.match(r"Sequential (.*) on ((\d+)[kM]-key) Namespace", name)
        if not match:
            print(f"⚠️  Could not parse operations scenario name: '{name}'. Skipping.")
            continue

        operation_type = match.group(1)
        size_label_full = match.group(2)
        num = int(match.group(3))

        if 'k-key' in size_label_full:
            size_sort_key = num * 1000
            size_label = f"{num}k Keys"
        elif 'M-key' in size_label_full:
            size_sort_key = num * 1_000_000
            size_label = f"{num}M Keys"
        else:
            size_sort_key = 0
            size_label = "Unknown"

        parsed_data.append({
            'operation': operation_type,
            'size_label': size_label,
            'size_sort_key': size_sort_key,
            'shard_count': 1, # This benchmark is always single-shard
            'avg_latency_us': entry['avg_latency_ns'] / 1000,
        })
    return pd.DataFrame(parsed_data)

def parse_sharding_overhead_results(filepath):
    """Parses results from sharding_overhead_bench_results.json."""
    with open(filepath, 'r') as f:
        raw_data = json.load(f)

    parsed_data = []
    for entry in raw_data:
        name = entry['scenario_name']
        # e.g., "Sequential Get on 100k-key Namespace (8 Shard(s))"
        match = re.match(r"Sequential (.*) on .* \((\d+) Shard\(s\)\)", name)
        if not match:
            print(f"⚠️  Could not parse sharding overhead scenario name: '{name}'. Skipping.")
            continue

        operation_type = match.group(1)
        shard_count = int(match.group(2))

        parsed_data.append({
            'operation': operation_type,
            'shard_count': shard_count,
            'avg_latency_us': entry['avg_latency_ns'] / 1000,
            'avg_deviation_percent': entry.get('avg_deviation_percent'),
        })
    return pd.DataFrame(parsed_data)

def parse_concurrent_results(filepath):
    """Parses results from concurrent_bench_results.json."""
    with open(filepath, 'r') as f:
        raw_data = json.load(f)

    parsed_data = []
    for entry in raw_data:
        name = entry['scenario_name']
        # e.g., "80-20 Read-Heavy Workload @ 8 Concurrency (1 Shard(s))"
        match = re.match(r"(.*) Workload @ (\d+) Concurrency \((\d+) Shard\(s\)\)", name)
        if not match:
            print(f"⚠️  Could not parse concurrent scenario name: '{name}'. Skipping.")
            continue

        workload_name = match.group(1)
        concurrency = int(match.group(2))
        shard_count = int(match.group(3))

        parsed_data.append({
            'workload': workload_name,
            'concurrency': concurrency,
            'shard_count': shard_count,
            'avg_latency_us': entry['avg_latency_ns'] / 1000,
            'ops_per_sec': entry.get('ops_per_sec', 0),
        })
    return pd.DataFrame(parsed_data)

def parse_batch_operations_results(filepath):
    """Parses results from batch_operations_bench_results.json."""
    with open(filepath, 'r') as f:
        raw_data = json.load(f)

    parsed_data = []
    for entry in raw_data:
        name = entry['scenario_name']
        # e.g., "Batch Set (Size 50) (AllOrNothing) on 100k-key Namespace"
        match = re.match(r"Batch (Set|Get|Delete) \(Size (\d+)\) \((AllOrNothing|BestEffort)\) on .*", name)
        if not match:
            print(f"⚠️  Could not parse batch scenario name: '{name}'. Skipping.")
            continue

        parsed_data.append({
            'operation': match.group(1),
            'batch_size': int(match.group(2)),
            'batch_mode': match.group(3),
            'avg_latency_per_item_us': entry['avg_latency_per_item_ns'] / 1000,
            'avg_latency_per_op_us': entry['avg_latency_per_op_ns'] / 1000,
        })
    return pd.DataFrame(parsed_data)

def parse_concurrent_batch_results(filepath):
    """Parses results from batch_concurrent_bench_results.json."""
    with open(filepath, 'r') as f:
        raw_data = json.load(f)

    parsed_data = []
    for entry in raw_data:
        name = entry['scenario_name']
        # e.g., "Concurrent Batch Set (Size 50) (AllOrNothing) @ 8 Concurrency (1 Shard(s))"
        match = re.match(r"Concurrent Batch (Set|Get|Delete) \(Size (\d+)\) \((AllOrNothing|BestEffort)\) @ (\d+) Concurrency \((\d+) Shard\(s\)\)", name)
        if not match:
            print(f"⚠️  Could not parse concurrent batch scenario name: '{name}'. Skipping.")
            continue

        parsed_data.append({
            'operation': match.group(1), 'batch_size': int(match.group(2)),
            'batch_mode': match.group(3), 'concurrency': int(match.group(4)), 'shard_count': int(match.group(5)),
            'avg_latency_per_item_us': entry['avg_latency_per_item_ns'] / 1000,
            'avg_latency_per_op_us': entry['avg_latency_per_op_ns'] / 1000,
        })
    return pd.DataFrame(parsed_data)

def plot_operations_results(df, file_prefix, output_dir):
    """
    Generates plots for sequential operations benchmarks.
    """
    if df is None or df.empty:
        return

    for shard_count, group_df in df.groupby('shard_count'):
        # Sort by operation and then by size to ensure correct plotting order
        group_df = group_df.sort_values(['operation', 'size_sort_key'])

        # Pivot the data for plotting
        pivot_df = group_df.pivot(index='operation', columns='size_label', values='avg_latency_us')

        # Ensure columns (sizes) are in a logical order
        def sort_key(label):
            num_str, _ = label.split(' ')
            unit = num_str[-1]
            num = int(num_str[:-1])
            if unit == 'M':
                return num * 1_000_000
            elif unit == 'k':
                return num * 1000
            return num
        pivot_df = pivot_df.reindex(sorted(pivot_df.columns, key=sort_key), axis=1)

        # --- Plotting ---
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(16, 9))

        pivot_df.plot(kind='bar', ax=ax, width=0.8, colormap='plasma', alpha=0.9)

        ax.set_title(f'Sequential Operations Benchmark ({shard_count} Shard(s))', fontsize=20, pad=20)
        ax.set_ylabel('Average Latency (microseconds) - Lower is Better', fontsize=12)
        ax.set_xlabel('Database Operation', fontsize=12)
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        ax.grid(axis='y', linestyle='--', alpha=0.5)
        ax.legend(title='Namespace Size', loc='upper left')

        # Add annotations on top of each bar
        for patch in ax.patches:
            height = patch.get_height()
            if height > 0:
                label = f'{height:g}'
                ax.annotate(label, (patch.get_x() + patch.get_width() / 2., height),
                            ha='center', va='bottom', xytext=(0, 5), textcoords='offset points',
                            fontsize=8, rotation=90)

        plt.tight_layout()

        # Sanitize filename
        output_path = os.path.join(output_dir, f'{file_prefix}_{shard_count}_shards_latency.png')
        plt.savefig(output_path, dpi=150)
        print(f"✅ Plot for Sequential ({shard_count} Shards) saved to {output_path}")
        plt.close(fig)

def plot_sharding_overhead_results(df, file_prefix, output_dir):
    """
    Generates a plot for the sharding overhead benchmark.
    """
    if df is None or df.empty:
        return

    df = df.sort_values(['operation', 'shard_count'])

    plt.style.use('dark_background')
    fig, ax1 = plt.subplots(figsize=(16, 9))

    # --- Primary Y-axis: Latency ---
    ax1.set_xlabel('Number of Shards', fontsize=12)
    ax1.set_ylabel('Average Latency (microseconds) - Lower is Better', fontsize=12)
    ax1.grid(axis='y', linestyle='--', alpha=0.3)

    # Plot latency for each operation type
    operations = df['operation'].unique()
    colors = plt.cm.viridis(np.linspace(0.5, 1, len(operations)))

    for i, op in enumerate(operations):
        op_df = df[df['operation'] == op]
        color = colors[i]
        ax1.plot(op_df['shard_count'], op_df['avg_latency_us'], color=color, marker='o', linestyle='-', label=f'Avg Latency (µs) - {op}')
        
        # Annotate latency points
        y_offset = 35 + (i * 15) # Stagger annotations
        for j, txt in enumerate(op_df['avg_latency_us']):
            ax1.annotate(
                f'{txt:g} µs', (op_df['shard_count'].iloc[j], op_df['avg_latency_us'].iloc[j]),
                textcoords="offset points", xytext=(0, y_offset), ha='center', fontsize=10, color=color,
                path_effects=[pe.withStroke(linewidth=3, foreground='black')]
            )

    ax1.tick_params(axis='y', labelcolor='cyan')
    ax1.legend(loc='upper left')

    # --- Secondary Y-axis: Deviation ---
    ax2 = ax1.twinx()
    color2 = 'magenta'
    ax2.set_ylabel('Average Deviation from Perfect Distribution (%)', color=color2, fontsize=12)
    
    # Deviation is the same for all operations at a given shard count, so we can just plot one
    deviation_df = df.drop_duplicates(subset=['shard_count'])
    ax2.plot(deviation_df['shard_count'], deviation_df['avg_deviation_percent'], color=color2, marker='x', linestyle='--', label='Avg Deviation (%)')
    ax2.tick_params(axis='y', labelcolor=color2)
    ax2.legend(loc='upper right')

    # Annotate deviation points
    for i, txt in enumerate(deviation_df['avg_deviation_percent']):
        ax2.annotate(
            f'{txt:.2f}%', (deviation_df['shard_count'].iloc[i], deviation_df['avg_deviation_percent'].iloc[i]),
            textcoords="offset points", xytext=(0,-20), ha='center', fontsize=10, color=color2,
            path_effects=[pe.withStroke(linewidth=3, foreground='black')]
        )

    # --- General Plot Settings ---
    ax1.set_xscale('log', base=2)
    ax1.set_xticks(df['shard_count'].unique())
    ax1.get_xaxis().set_major_formatter(plt.ScalarFormatter()) # Show numbers as is, not 2^n
    ax1.set_title('Operation Latency vs. Shard Count (Sharding Overhead)', fontsize=20, pad=20)
    fig.tight_layout()

    output_path = os.path.join(output_dir, f'{file_prefix}_latency_vs_shards.png')
    plt.savefig(output_path, dpi=150)
    print(f"✅ Sharding overhead plot saved to {output_path}")
    plt.close(fig)

def plot_concurrent_results(df, file_prefix, output_dir):
    """
    Generates plots for concurrent workload benchmarks.
    """
    if df is None or df.empty:
        return

    for workload, group_df in df.groupby('workload'):
        # Sort by concurrency to ensure the x-axis is in order
        group_df = group_df.sort_values('concurrency')

        # Pivot the data for plotting
        pivot_df = group_df.pivot(index='concurrency', columns='shard_count', values='avg_latency_us')

        # --- Latency Plotting ---
        plt.style.use('dark_background')
        fig, ax = plt.subplots(figsize=(16, 9))

        pivot_df.plot(kind='bar', ax=ax, width=0.8, colormap='viridis', alpha=0.9)

        ax.set_title(f'Concurrent Benchmark: {workload} Workload', fontsize=20, pad=20)
        ax.set_ylabel('Average Latency (microseconds) - Lower is Better', fontsize=12)
        ax.set_xlabel('Number of Concurrent Tasks', fontsize=12)
        plt.setp(ax.get_xticklabels(), rotation=0)
        ax.grid(axis='y', linestyle='--', alpha=0.5)
        ax.legend(title='Shard Count', loc='upper left')

        # Add annotations on top of each bar
        for patch in ax.patches:
            height = patch.get_height()
            if height > 0:
                label = f'{height:g}'
                ax.annotate(label, (patch.get_x() + patch.get_width() / 2., height),
                            ha='center', va='bottom', xytext=(0, 5), textcoords='offset points',
                            fontsize=8, rotation=90)

        plt.tight_layout()

        filename_workload = workload.lower().replace(' ', '_').replace('-', '_')
        output_path = os.path.join(output_dir, f'{file_prefix}_{filename_workload}_latency.png')
        plt.savefig(output_path, dpi=150)
        print(f"✅ Plot for {workload} saved to {output_path}")
        plt.close(fig)

        # --- Throughput Plotting ---
        pivot_df_throughput = group_df.pivot(index='concurrency', columns='shard_count', values='ops_per_sec')

        fig, ax = plt.subplots(figsize=(16, 9))

        pivot_df_throughput.plot(kind='bar', ax=ax, width=0.8, colormap='cividis', alpha=0.9)

        ax.set_title(f'Concurrent Benchmark: {workload} Throughput', fontsize=20, pad=20)
        ax.set_ylabel('Operations per Second - Higher is Better', fontsize=12)
        ax.set_xlabel('Number of Concurrent Tasks', fontsize=12)
        plt.setp(ax.get_xticklabels(), rotation=0)
        ax.grid(axis='y', linestyle='--', alpha=0.5)
        ax.legend(title='Shard Count', loc='upper left')

        # Format y-axis to have commas for thousands
        ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))

        # Add annotations on top of each bar
        for patch in ax.patches:
            height = patch.get_height()
            if height > 0:
                label = f'{int(height):,}'
                ax.annotate(label, (patch.get_x() + patch.get_width() / 2., height),
                            ha='center', va='bottom', xytext=(0, 5), textcoords='offset points',
                            fontsize=8, rotation=90)

        plt.tight_layout()

        output_path_throughput = os.path.join(output_dir, f'{file_prefix}_{filename_workload}_throughput.png')
        plt.savefig(output_path_throughput, dpi=150)
        print(f"✅ Throughput Plot for {workload} saved to {output_path_throughput}")
        plt.close(fig)

def plot_simple_batch_operations_results(df, file_prefix, output_dir):
    """
    Generates plots for batch operations benchmarks.
    """
    if df is None or df.empty:
        return

    # --- Plot 1: Per-Item Latency ---
    pivot_item = df.pivot_table(
        index='operation',
        columns=['batch_mode', 'batch_size'],
        values='avg_latency_per_item_us'
    ).reindex(['Set', 'Get', 'Delete']) # Ensure consistent order

    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(18, 10))
    pivot_item.plot(kind='bar', ax=ax, width=0.8, colormap='plasma', alpha=0.9)

    ax.set_title(f'Batch Operations: Average Latency Per Item', fontsize=20, pad=20)
    ax.set_ylabel('Average Latency (microseconds) - Lower is Better', fontsize=12)
    ax.set_xlabel('Batch Operation Type', fontsize=12)
    plt.setp(ax.get_xticklabels(), rotation=0)
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    ax.legend(title='Mode & Batch Size', loc='upper left')

    for patch in ax.patches:
        height = patch.get_height()
        if height > 0:
            ax.annotate(f'{height:.2f}', (patch.get_x() + patch.get_width() / 2., height),
                        ha='center', va='bottom', xytext=(0, 5), textcoords='offset points',
                        fontsize=8, rotation=90)

    plt.tight_layout()
    output_path_item = os.path.join(output_dir, f'{file_prefix}_per_item_latency.png')
    plt.savefig(output_path_item, dpi=150)
    print(f"✅ Plot for Batch (Per Item) saved to {output_path_item}")
    plt.close(fig)

    # --- Plot 2: Per-Operation (Full Batch) Latency ---
    pivot_op = df.pivot_table(
        index='operation',
        columns=['batch_mode', 'batch_size'],
        values='avg_latency_per_op_us'
    ).reindex(['Set', 'Get', 'Delete'])

    fig, ax = plt.subplots(figsize=(18, 10))
    pivot_op.plot(kind='bar', ax=ax, width=0.8, colormap='viridis', alpha=0.9)

    ax.set_title(f'Batch Operations: Average Latency Per Operation (Full Batch)', fontsize=20, pad=20)
    ax.set_ylabel('Average Latency (microseconds) - Lower is Better', fontsize=12)
    ax.set_xlabel('Batch Operation Type', fontsize=12)
    plt.setp(ax.get_xticklabels(), rotation=0)
    ax.grid(axis='y', linestyle='--', alpha=0.5)
    ax.legend(title='Mode & Batch Size', loc='upper left')

    plt.tight_layout()
    output_path_op = os.path.join(output_dir, f'{file_prefix}_per_op_latency.png')
    plt.savefig(output_path_op, dpi=150)
    print(f"✅ Plot for Batch (Per Op) saved to {output_path_op}")
    plt.close(fig)

def plot_concurrent_batch_results(df: pd.DataFrame, file_prefix: str, output_dir):
    """
    Generates two summary plots for concurrent batch operations benchmarks.
    1. Latency vs. Concurrency, faceted by operation.
    2. Latency vs. Batch Configuration, faceted by operation.
    """
    if df is None or df.empty:
        return

    os.makedirs(output_dir, exist_ok=True)
    plt.style.use('dark_background')

    # --- Plot 1: Latency vs. Concurrency (faceted by operation) ---
    # Average across batch sizes and modes to see the high-level trend
    plot1_df = df.groupby(['operation', 'concurrency', 'shard_count'])['avg_latency_per_item_us'].mean().reset_index()

    operations = sorted(plot1_df['operation'].unique())
    fig1, axes1 = plt.subplots(1, len(operations), figsize=(20, 7), sharey=True)
    fig1.suptitle('Concurrent Batch: Latency vs. Concurrency (Averaged)', fontsize=22, y=1.02)

    for i, operation in enumerate(operations):
        ax = axes1[i]
        op_df = plot1_df[plot1_df['operation'] == operation]
        pivot_df = op_df.pivot(index='concurrency', columns='shard_count', values='avg_latency_per_item_us')

        pivot_df.plot(kind='line', marker='o', ax=ax, colormap='viridis')

        ax.set_title(f'Operation: {operation}', fontsize=16)
        ax.set_xlabel('Concurrency Level', fontsize=12)
        if i == 0:
            ax.set_ylabel('Avg Latency per Item (µs) - Lower is Better', fontsize=12)
        ax.grid(True, linestyle='--', alpha=0.5)
        ax.legend(title='Shard Count', loc='upper left')
        ax.set_xticks(df['concurrency'].unique())

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    output_path1 = os.path.join(output_dir, f'{file_prefix}_vs_concurrency.png')
    plt.savefig(output_path1, dpi=150, bbox_inches='tight')
    print(f"✅ Summary plot for Concurrent Batch (vs Concurrency) saved to {output_path1}")
    plt.close(fig1)


    # --- Plot 2: Latency vs. Batch Config (faceted by operation) ---
    # Average across concurrency levels to see the impact of batch config
    df['batch_config'] = "Size " + df['batch_size'].astype(str) + " (" + df['batch_mode'] + ")"
    plot2_df = df.groupby(['operation', 'batch_config', 'shard_count'])['avg_latency_per_item_us'].mean().reset_index()

    # Define a categorical order for the x-axis
    batch_size_order = sorted(df['batch_size'].unique())
    batch_mode_order = ['BestEffort', 'AllOrNothing']
    config_order = [f"Size {s} ({m})" for s in batch_size_order for m in batch_mode_order]
    plot2_df['batch_config'] = pd.Categorical(plot2_df['batch_config'], categories=config_order, ordered=True)
    plot2_df = plot2_df.sort_values('batch_config')

    fig2, axes2 = plt.subplots(1, len(operations), figsize=(22, 8), sharey=True)
    fig2.suptitle('Concurrent Batch: Latency vs. Batch Configuration (Averaged)', fontsize=22, y=1.02)

    for i, operation in enumerate(operations):
        ax = axes2[i]
        op_df = plot2_df[plot2_df['operation'] == operation]
        pivot_df = op_df.pivot(index='batch_config', columns='shard_count', values='avg_latency_per_item_us')

        pivot_df.plot(kind='bar', ax=ax, width=0.8, colormap='plasma', alpha=0.9)

        ax.set_title(f'Operation: {operation}', fontsize=16)
        ax.set_xlabel('Batch Size & Mode', fontsize=12)
        if i == 0:
            ax.set_ylabel('Avg Latency per Item (µs) - Lower is Better', fontsize=12)
        
        ax.grid(axis='y', linestyle='--', alpha=0.5)
        ax.legend(title='Shard Count', loc='upper left')
        ax.tick_params(axis='x', rotation=45, labelsize=10)

        # Add annotations
        for patch in ax.patches:
            height = patch.get_height()
            if not pd.isna(height) and height > 0:
                label = f'{height:.1f}'
                ax.annotate(label, (patch.get_x() + patch.get_width() / 2., height),
                            ha='center', va='bottom', xytext=(0, 5), textcoords='offset points',
                            fontsize=8, rotation=90)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    output_path2 = os.path.join(output_dir, f'{file_prefix}_vs_batch_config.png')
    plt.savefig(output_path2, dpi=150, bbox_inches='tight')
    print(f"✅ Summary plot for Concurrent Batch (vs Batch Config) saved to {output_path2}")
    plt.close(fig2)

if __name__ == '__main__':
    # The script is in /scripts, so the project root is one level up.
    project_root = os.path.join(os.path.dirname(__file__), '..')
    results_dir = os.path.join(project_root, 'assets', 'benchmarks')
    result_files = glob.glob(os.path.join(results_dir, '*_bench_results.json'))

    # The output directory for plots is in /assets/benchmarks
    os.makedirs(results_dir, exist_ok=True)

    if not result_files:
        print(f"❌ No benchmark result files (*_bench_results.json) found in '{results_dir}'.")
        print("Please run the benchmarks first (e.g., 'cargo bench --bench operations_bench').")

    for result_file in result_files:
        filename = os.path.basename(result_file)
        file_prefix = filename.replace('_bench_results.json', '')
        print(f"\nProcessing '{filename}'...")

        df = None
        plot_function = None

        if filename == 'operations_bench_results.json':
            df = parse_operations_results(result_file)
            plot_function = plot_operations_results
        elif filename == 'sharding_overhead_bench_results.json':
            df = parse_sharding_overhead_results(result_file)
            plot_function = plot_sharding_overhead_results
        elif filename == 'concurrent_bench_results.json':
            df = parse_concurrent_results(result_file)
            plot_function = plot_concurrent_results
        elif filename == 'batch_operations_bench_results.json':
            df = parse_batch_operations_results(result_file)
            plot_function = plot_simple_batch_operations_results
        elif filename == 'batch_concurrent_bench_results.json':
            df = parse_concurrent_batch_results(result_file)
            plot_function = plot_concurrent_batch_results

        if df is not None and not df.empty:
            plot_function(df, file_prefix, results_dir)
        else:
            print(f"No data parsed from '{filename}'. Skipping plot generation.")