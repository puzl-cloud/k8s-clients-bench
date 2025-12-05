from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from .benchmark import Benchmark


def benchmarks_to_df(benchmarks: list[Benchmark]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    bench_order: list[str] = []
    client_order: list[str] = []

    for bench in benchmarks:
        client = bench.client
        if client not in client_order:
            client_order.append(client)
        for res in bench.results:
            if res.bench_name not in bench_order:
                bench_order.append(res.bench_name)
            rps = res.requests / res.seconds if res.seconds else 0.0
            rows.append({
                "Client": client,
                "Benchmark": res.bench_name,
                "Objects": res.requests,
                "Obj/s": rps,
            })

    df = pd.DataFrame(rows)
    obj_per_client = df.groupby("Client")["Objects"].max()

    wide = df.pivot(index="Client", columns="Benchmark", values="Obj/s")
    wide = wide.reindex(columns=bench_order)
    wide = wide.reindex(index=client_order)  # Keep the original order
    wide.insert(0, "Objects", obj_per_client)
    wide.columns.name = None
    return wide


def print_combined_results(benchmarks: list[Benchmark]) -> None:
    df = benchmarks_to_df(benchmarks)
    print("Combined results (objects per second)")
    with pd.option_context("display.float_format", lambda x: f"{x:.1f}"):
        print(df.to_string())
    print("-" * 60)


PALETTE = [
    "#4E79A7",
    "#F28E2B",
    "#E15759",
    "#76B7B2",
]


def plot_benchmarks_histogram(benchmarks: list[Benchmark], output_dir: str | Path | None = None) -> None:
    df = benchmarks_to_df(benchmarks)
    if "Client" in df.columns:
        df = df.set_index("Client")
    if "Objects" in df.columns:
        df = df.drop(columns=["Objects"])

    # Figure height scales with #clients
    num_clients = len(df.index)
    fig_height = max(7, num_clients + 2)
    fig, ax = plt.subplots(figsize=(10, fig_height))

    # Horizontal grouped bars, thinner for spacing within client
    df.plot(kind="barh", ax=ax, width=0.6, color=PALETTE)

    ax.set_xlabel("Objects per second")
    ax.set_ylabel("")
    ax.set_title("Python Kubernetes clients benchmark", pad=12)

    # Vertical grid for readability
    ax.grid(axis="x", linestyle="--", linewidth=0.5, alpha=0.5)
    ax.set_axisbelow(True)

    # Legend clearly below the chart
    ax.legend(
        title="",
        loc="upper center",
        ncol=len(df.columns),
        frameon=False,
        bbox_to_anchor=(0.5, -0.18),
    )

    # Thousands separator on x axis
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{int(x):,}"))

    # Value labels on bars
    for container in ax.containers:
        ax.bar_label(container, fmt="%.0f", padding=3, fontsize=8)

    fig.tight_layout()
    if output_dir is None:
        plt.show()
    else:
        output_dir = Path(output_dir).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "python_kubernetes_clients_benchmark.png"
        print(f"Saving chart {output_file}")
        fig.savefig(output_file, dpi=900, bbox_inches="tight")
        plt.show()

    plt.close(fig)
