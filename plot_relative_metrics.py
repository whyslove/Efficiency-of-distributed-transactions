import pandas as pd
import matplotlib.pyplot as plt
import argparse
import numpy as np

PROTOCOL_COLORS = {"paxos": "#1f77b4", "raft": "#ff7f0e", "accord": "#2ca02c"}
PROBLEM_LABELS = {1: "1 сбой", 2: "2 сбоя"}
WORKLOAD_TITLES = {
    "a": "50% обновлений, 50% чтений",
    "b": "95% чтений, 5% обновлений",
    "g": "95% обновлений, 5% чтений",
    "h": "95% обновлений, высокая конкуренция",
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


def calculate_degradation(combined_df):
    # Фильтруем только UPDATE операции
    update_df = combined_df[combined_df["operation"] == "UPDATE"]

    base_df = update_df[update_df["problem_level"] == 0]
    degradation_data = []

    for (proto, workload), group in update_df.groupby(["protocol", "workload"]):
        base_group = group[group["problem_level"] == 0]

        if base_group.empty:
            continue

        base_ops = base_group["ops"].values[0]
        base_latency = base_group["p95_us"].values[0]

        for plvl in [1, 2]:
            current = group[group["problem_level"] == plvl]
            if not current.empty:
                degradation_data.append(
                    {
                        "protocol": proto,
                        "workload": workload,
                        "problem_level": plvl,
                        "ops_ratio": (
                            current["ops"].values[0] / base_ops
                            if base_ops != 0
                            else 0
                        ),
                        "latency_ratio": (
                            current["p95_us"].values[0] / base_latency
                            if base_latency != 0
                            else 0
                        ),
                    }
                )

    return pd.DataFrame(degradation_data)


def plot_separate_degradation(degradation_df):
    for workload in sorted(degradation_df["workload"].unique()):
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        fig.suptitle(
            f"Ухудшение метрик при отказах в % - {WORKLOAD_TITLES[workload]}",
            fontsize=14,
            # y=1.02,
        )

        w_df = degradation_df[degradation_df["workload"] == workload]
        protocols = sorted(w_df["protocol"].unique())

        width = 0.35
        x = np.arange(len(protocols))

        # График пропускной способности
        for i, plvl in enumerate([1, 2]):
            values = w_df[w_df["problem_level"] == plvl]["ops_ratio"]
            ax1.bar(
                x + i * width,
                values,
                width=width,
                label=PROBLEM_LABELS[plvl],
                color=list(PROTOCOL_COLORS.values())[i],
                alpha=0.8,
            )

        ax1.set_xticks(x + width / 2)
        ax1.set_xticklabels(protocols)
        ax1.set_ylabel("Относительная пропускная способность")
        ax1.axhline(1, color="grey", linestyle="--")
        ax1.legend()
        ax1.grid(axis="y", alpha=0.3)

        # График латентности
        for i, plvl in enumerate([1, 2]):
            values = w_df[w_df["problem_level"] == plvl]["latency_ratio"]
            ax2.bar(
                x + i * width,
                values,
                width=width,
                label=PROBLEM_LABELS[plvl],
                color=list(PROTOCOL_COLORS.values())[i],
                alpha=0.8,
            )

        ax2.set_xticks(x + width / 2)
        ax2.set_xticklabels(protocols)
        ax2.set_ylabel("Относительный уровень задержек (p95)")
        ax2.axhline(1, color="grey", linestyle="--")
        ax2.legend()
        ax2.grid(axis="y", alpha=0.3)

        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Plot UPDATE metrics degradation"
    )
    parser.add_argument(
        "files", nargs=3, help="CSV files for problem levels 0,1,2"
    )
    args = parser.parse_args()

    df = load_data(args.files)
    degradation_df = calculate_degradation(df)

    print("\nАбсолютные значения p95 латентности для операций UPDATE:\n")
    update_df = df[df["operation"] == "UPDATE"]
    for _, row in update_df.iterrows():
        print(
            f"Протокол: {row['protocol']}, Нагрузка: {row['workload']}, Сбой:"
            f" {PROBLEM_LABELS.get(row['problem_level'], row['problem_level'])},"
            f" p95_us: {int(row['p95_us'])}"
        )
    plot_separate_degradation(degradation_df)
