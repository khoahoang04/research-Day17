"""
plot_results.py
===============
Vai trò của file: VẼ 2 BIỂU ĐỒ BẮT BUỘC của báo cáo (phần của Thành viên B), đọc từ CSV
do experiment_runner.py xuất ra (mỗi dòng = 1 lần chạy).

  Biểu đồ 1 (throughput.png)  : correct commits/giây theo số writer, mỗi strategy 1 đường,
                                mỗi pattern 1 ô (dải mờ = ±1 độ lệch chuẩn giữa các lần chạy)
  Biểu đồ 2 (p95_latency.png) : boxplot p95 commit latency theo pattern, tách theo strategy

Cần matplotlib (chưa có trong requirements.txt):  pip install matplotlib

Chạy:
    python -m src.plot_results --csv results/experiment_results.csv --outdir results
"""

import argparse
import os

import matplotlib
matplotlib.use("Agg")          # vẽ ra file, không cần cửa sổ
import matplotlib.pyplot as plt
import pandas as pd


def _title_suffix(df: pd.DataFrame) -> str:
    """Gắn nhãn cảnh báo nếu dữ liệu là giả, để không nhầm với kết quả thật."""
    return "  [SYNTHETIC DATA - NOT A RESULT]" if (df["source"] == "synthetic").any() else ""


def plot_throughput(df: pd.DataFrame, out_path: str) -> None:
    """Biểu đồ 1: thông lượng ĐÚNG (run sai đã bị ép về 0) theo số writer."""
    patterns = sorted(df["pattern"].unique())
    fig, axes = plt.subplots(1, len(patterns), figsize=(5 * len(patterns), 4), sharey=True, squeeze=False)

    for ax, pattern in zip(axes[0], patterns):
        sub = df[df["pattern"] == pattern]
        for strategy, g in sub.groupby("strategy"):
            stats = g.groupby("num_writers")["correct_commits_per_sec"].agg(["mean", "std"]).fillna(0)
            ax.plot(stats.index, stats["mean"], marker="o", label=strategy)
            ax.fill_between(stats.index, stats["mean"] - stats["std"],
                            stats["mean"] + stats["std"], alpha=0.15)
        ax.set_title(f"Pattern {pattern}")
        ax.set_xlabel("num_writers")
        ax.grid(alpha=0.3)
    axes[0][0].set_ylabel("correct commits / second")
    axes[0][-1].legend(fontsize=8)
    fig.suptitle("Throughput (Baseline vs Strategies)" + _title_suffix(df))
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def plot_p95_latency(df: pd.DataFrame, out_path: str) -> None:
    """Biểu đồ 2: boxplot p95 latency theo pattern, mỗi strategy 1 màu."""
    patterns = sorted(df["pattern"].unique())
    strategies = sorted(df["strategy"].unique())
    width = 0.8 / len(strategies)

    fig, ax = plt.subplots(figsize=(7, 4))
    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    for s_idx, strategy in enumerate(strategies):
        data = [df[(df["pattern"] == p) & (df["strategy"] == strategy)]["p95_latency_ms"].values
                for p in patterns]
        positions = [i + s_idx * width - 0.4 + width / 2 for i in range(len(patterns))]
        box = ax.boxplot(data, positions=positions, widths=width * 0.9, patch_artist=True)
        for patch in box["boxes"]:
            patch.set_facecolor(colors[s_idx % len(colors)])
            patch.set_alpha(0.6)
        ax.plot([], [], color=colors[s_idx % len(colors)], label=strategy)   # chỉ để có legend
    ax.set_xticks(range(len(patterns)))
    ax.set_xticklabels([f"Pattern {p}" for p in patterns])
    ax.set_ylabel("p95 commit latency (ms)")
    ax.set_title("p95 latency by pattern" + _title_suffix(df))
    # Legend đặt DƯỚI trục để không che các hộp
    ax.legend(fontsize=8, loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=len(strategies))
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Vẽ 2 biểu đồ từ CSV kết quả.")
    parser.add_argument("--csv", default="results/experiment_results.csv")
    parser.add_argument("--outdir", default="results")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    os.makedirs(args.outdir, exist_ok=True)
    plot_throughput(df, os.path.join(args.outdir, "throughput.png"))
    plot_p95_latency(df, os.path.join(args.outdir, "p95_latency.png"))
    print(f"✅ Đã vẽ: {args.outdir}/throughput.png, {args.outdir}/p95_latency.png")


if __name__ == "__main__":
    main()
