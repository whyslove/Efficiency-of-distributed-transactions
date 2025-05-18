import pandas as pd
import matplotlib.pyplot as plt
import argparse
import numpy as np

PROTOCOL_COLORS = {"paxos": "#1f77b4", "raft": "#ff7f0e", "accord": "#2ca02c"}
PROBLEM_LABELS = {0: "Без сбоев", 1: "1 сбой", 2: "2 сбоя"}
WORKLOAD_TITLES = {
    "a": "50% обновлений, 50% чтений",
    "b": "95% чтений, 5% обновлений",
    "g": "95% обновлений, 5% чтений",
    "h": "95% обновлений, высокая конкуренция",
}
PROBLEM_COLORS = {
    0: "#4c72b0",
    1: "#dd8452",
    2: "#55a868",
}


def extract_protocol_and_workload(exp):
    proto = exp.split("_worloads_")[0]
    proto = "raft" if proto == "cockroach" else proto
    workload = exp.split("_worloads_")[1].split("_")[0]
    return proto, workload


def load_data(filenames):
    dfs = []
    for i, filename in enumerate(filenames):
        df = pd.read_csv(filename)
        df["problem_level"] = i
        df[["protocol", "workload"]] = df["experiment"].apply(
            lambda x: pd.Series(extract_protocol_and_workload(x))
        )
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def plot_simple_comparison(combined_df):
    for workload in sorted(combined_df["workload"].unique()):
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        fig.suptitle(
            WORKLOAD_TITLES.get(workload, f"Workload {workload.upper()}"),
            fontsize=14,
        )

        w_df = combined_df[combined_df["workload"] == workload]
        protocols = sorted(w_df["protocol"].unique())

        grouped = w_df.groupby(["protocol", "problem_level"])

        plot_operations(axes[0], grouped, protocols)

        plot_latency(axes[1], grouped, protocols)

        plot_errors(axes[2], grouped, protocols)

        plt.tight_layout()
        plt.show()


def plot_operations(ax, grouped, protocols):
    ops_data = {proto: [] for proto in protocols}
    for (proto, plvl), grp in grouped:
        mod_ops = grp[
            grp["operation"].isin(["UPDATE"])  # "INSERT", "READ_MODIFY_WRITE"
        ]
        ops_data[proto].append(mod_ops["ops"].sum())

    width = 0.25
    x = np.arange(len(protocols))
    for i, plvl in enumerate(PROBLEM_LABELS):
        ax.bar(
            x + i * width,
            [ops_data[proto][i] for proto in protocols],
            width=width,
            label=PROBLEM_LABELS[plvl],
            color=PROBLEM_COLORS[plvl],
        )

    ax.set_xticks(x + width)
    ax.set_xticklabels(protocols)
    ax.set_title("Операции/сек")
    ax.legend(title="Уровень сбоев")


def plot_latency(ax, grouped, protocols):
    # Средняя p99 латентность
    lat_data = {proto: [] for proto in protocols}
    for (proto, plvl), grp in grouped:
        total = grp[grp["operation"] == "UPDATE"]
        lat_data[proto].append(total["p95_us"].mean())

    width = 0.25
    x = np.arange(len(protocols))
    for i, plvl in enumerate(PROBLEM_LABELS):
        ax.bar(
            x + i * width,
            [lat_data[proto][i] for proto in protocols],
            width=width,
            label=PROBLEM_LABELS[plvl],
            color=PROBLEM_COLORS[plvl],
        )

    ax.set_xticks(x + width)
    ax.set_xticklabels(protocols)
    ax.set_title("Уровень задержек p95 (мкс)")
    ax.set_yscale("log")


def plot_errors(ax, grouped, protocols):
    err_data = {proto: [] for proto in protocols}
    for (proto, plvl), grp in grouped:
        errors = grp[grp["operation"].str.contains("ERROR", na=False)]
        err_data[proto].append(errors["count"].sum())

    width = 0.25
    x = np.arange(len(protocols))
    for i, plvl in enumerate(PROBLEM_LABELS):
        ax.bar(
            x + i * width,
            [err_data[proto][i] for proto in protocols],
            width=width,
            label=PROBLEM_LABELS[plvl],
            color=PROBLEM_COLORS[plvl],
        )

    ax.set_xticks(x + width)
    ax.set_xticklabels(protocols)
    ax.set_title("Количество ошибок")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple benchmark comparison")
    parser.add_argument(
        "files", nargs=3, help="CSV files for problem levels 0,1,2"
    )
    args = parser.parse_args()

    df = load_data(args.files)
    plot_simple_comparison(df)
