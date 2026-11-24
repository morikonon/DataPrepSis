"""Exploratory data analysis of the questions stored in SQLite, with Matplotlib/Seaborn figures."""

import json
import logging
import sqlite3

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

from src import config  # noqa: E402
from src.cleaner import REPUTATION_BANDS, STATUS_ORDER, WEEKDAYS  # noqa: E402

log = logging.getLogger("analysis")

INK, INK_SECONDARY, INK_MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]  # validated categorical slots 1-4, fixed order
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
NUMERIC_COLUMNS = ["score", "view_count", "answer_count", "hours_to_first_answer", "hours_to_accepted_answer",
                   "views_per_day", "owner_reputation", "title_length", "tag_count"]
CATEGORICAL_COLUMNS = ["status", "closed_reason", "owner_user_type", "reputation_band", "creation_month",
                       "creation_weekday"]

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "sans-serif", "font.size": 10, "text.color": INK,
    "axes.edgecolor": AXIS, "axes.labelcolor": INK_SECONDARY, "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.titlelocation": "left", "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "legend.frameon": False,
})


def load_from_db(db_path=config.DB_PATH) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(one row per question, one row per question and tracked tool)."""
    with sqlite3.connect(db_path) as connection:
        questions = pd.read_sql_query("SELECT * FROM questions", connection, parse_dates=["creation_at"])
        per_tool = pd.read_sql_query("SELECT * FROM v_question_tools", connection, parse_dates=["creation_at"])
    connection.close()
    for df in (questions, per_tool):
        df["has_answer"] = df["answer_count"] > 0
    return questions, per_tool


def describe_dataset(df: pd.DataFrame) -> dict:
    log.info("Dataset: %d rows x %d columns", *df.shape)
    log.info("Data types:\n%s", df.dtypes.value_counts().to_string())
    numeric = df[NUMERIC_COLUMNS].describe().T.round(2)
    numeric["skew"] = df[NUMERIC_COLUMNS].skew().round(2)
    log.info("Numerical statistics:\n%s", numeric.to_string())

    categorical = pd.DataFrame({
        "non_null": df[CATEGORICAL_COLUMNS].notna().sum(),
        "unique": df[CATEGORICAL_COLUMNS].nunique(),
        "most_frequent": df[CATEGORICAL_COLUMNS].mode().iloc[0],
        "frequency": [df[col].value_counts().iloc[0] for col in CATEGORICAL_COLUMNS],
    })
    log.info("Categorical statistics:\n%s", categorical.to_string())
    log.info("Status shares (%%):\n%s", df["status"].value_counts(normalize=True).mul(100).round(1).to_string())
    return {"shape": list(df.shape), "numeric": numeric, "categorical": categorical}


def grouped_analysis(questions: pd.DataFrame, per_tool: pd.DataFrame) -> dict:
    tools = (per_tool.groupby("tool")
                     .agg(questions=("question_id", "count"),
                          answered_pct=("has_answer", "mean"),
                          accepted_pct=("has_accepted_answer", "mean"),
                          answered_24h_pct=("answered_within_24h", "mean"),
                          median_hours=("hours_to_first_answer", "median"),
                          median_views=("view_count", "median"))
                     .sort_values("questions", ascending=False))
    tools[["answered_pct", "accepted_pct", "answered_24h_pct"]] *= 100
    tools = tools.round(1)

    by_reputation = (questions[questions["reputation_band"].isin(REPUTATION_BANDS)]
                     .groupby("reputation_band")[["has_answer", "has_accepted_answer"]].mean()
                     .mul(100).round(1).reindex(REPUTATION_BANDS))
    by_edit = questions.groupby("is_edited")[["has_answer", "has_accepted_answer"]].mean().mul(100).round(1)

    quarter = questions["creation_at"].dt.quarter
    per_quarter = questions.groupby(quarter).size()
    weekend = questions["creation_weekday"].isin(["Saturday", "Sunday"])
    answered = questions[questions["has_answer"]]

    stats = {
        "questions": len(questions),
        "answered_pct": round(questions["has_answer"].mean() * 100, 1),
        "accepted_pct": round(questions["has_accepted_answer"].mean() * 100, 1),
        "closed_pct": round(questions["is_closed"].mean() * 100, 1),
        "median_hours_to_first_answer": round(answered["hours_to_first_answer"].median(), 2),
        "answered_within_1h_pct_of_answered": round((answered["hours_to_first_answer"] <= 1).mean() * 100, 1),
        "q1_to_q4_change_pct": round((per_quarter.iloc[-1] / per_quarter.iloc[0] - 1) * 100, 1),
        "weekend_share_pct": round(weekend.mean() * 100, 1),
        "median_hours_weekday": round(answered.loc[~weekend[answered.index], "hours_to_first_answer"].median(), 2),
        "median_hours_weekend": round(answered.loc[weekend[answered.index], "hours_to_first_answer"].median(), 2),
        "spearman_views_vs_score": round(questions["view_count"].corr(questions["score"], method="spearman"), 3),
        "closed_reasons": questions["closed_reason"].value_counts().to_dict(),
        "tools": tools,
        "by_reputation": by_reputation,
        "by_edit": by_edit,
    }
    log.info("Answered: %.1f%%, accepted: %.1f%%, closed: %.1f%%",
             stats["answered_pct"], stats["accepted_pct"], stats["closed_pct"])
    log.info("Median time to the first answer: %.2f h; %.1f%% of answered questions got it within 1 h",
             stats["median_hours_to_first_answer"], stats["answered_within_1h_pct_of_answered"])
    log.info("Questions per quarter: %s (Q1 -> Q4: %+.1f%%)", per_quarter.to_dict(), stats["q1_to_q4_change_pct"])
    log.info("Weekend questions: %.1f%%; median hours to answer weekday %.2f vs weekend %.2f",
             stats["weekend_share_pct"], stats["median_hours_weekday"], stats["median_hours_weekend"])
    log.info("Spearman correlation views vs score: %.3f", stats["spearman_views_vs_score"])
    log.info("Per tool:\n%s", tools.to_string())
    log.info("Answered / accepted %% by asker reputation:\n%s", by_reputation.to_string())
    log.info("Answered / accepted %% by edited flag:\n%s", by_edit.to_string())
    log.info("Close reasons: %s", stats["closed_reasons"])
    return stats


def save(fig: plt.Figure, name: str) -> None:
    fig.savefig(config.FIGURES_DIR / name, dpi=200, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved figure %s", name)


def plot_monthly_volume(per_tool: pd.DataFrame, tool_order: list[str]) -> None:
    monthly = per_tool.pivot_table(index="creation_month", columns="tool", values="question_id", aggfunc="count")
    months = pd.to_datetime(monthly.index).strftime("%b")
    fig, axes = plt.subplots(2, 3, figsize=(11, 5.6), sharex=True)
    for ax, tool in zip(axes.flat, tool_order):
        ax.plot(range(len(months)), monthly[tool], color=SERIES[0], linewidth=2, marker="o", markersize=4)
        first, last = monthly[tool].iloc[0], monthly[tool].iloc[-1]
        ax.set_title(f"{tool}  ·  {first:.0f} → {last:.0f}", fontsize=10.5)
        ax.set_ylim(0, monthly[tool].max() * 1.15)
        ax.set_xticks(range(0, len(months), 2), months[::2])
    for ax in axes[:, 0]:
        ax.set_ylabel("Questions per month")
    fig.suptitle("New questions per month in 2025, by tool (January → December)", x=0.01, ha="left",
                 fontsize=12, fontweight="bold")
    fig.tight_layout()
    save(fig, "fig1_questions_per_month.png")


def plot_outcome_by_tool(per_tool: pd.DataFrame, tool_order: list[str]) -> None:
    shares = (pd.crosstab(per_tool["tool"], per_tool["status"], normalize="index").mul(100)
                .reindex(index=tool_order[::-1], columns=STATUS_ORDER))
    fig, ax = plt.subplots(figsize=(9, 4.6))
    left = np.zeros(len(shares))
    for status, color in zip(STATUS_ORDER, SERIES):
        ax.barh(shares.index, shares[status], left=left, height=0.62, color=color, label=status,
                edgecolor=SURFACE, linewidth=2)
        if status == "Accepted":
            for y, value in enumerate(shares[status]):
                ax.text(value - 1.5, y, f"{value:.0f}%", ha="right", va="center", fontsize=9, color="white")
        left += shares[status].to_numpy()
    ax.set_xlim(0, 100)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Share of the tool's questions (%)")
    ax.set_title("What happened to the questions: outcome by tool", pad=28)
    ax.legend(ncol=4, loc="lower left", bbox_to_anchor=(0, 1.0), fontsize=9)
    save(fig, "fig2_outcome_by_tool.png")


def plot_time_to_answer(per_tool: pd.DataFrame, tool_order: list[str]) -> None:
    answered = per_tool[per_tool["has_answer"]]
    fig, ax = plt.subplots(figsize=(9, 4.8))
    sns.boxplot(data=answered, y="tool", x="hours_to_first_answer", order=tool_order, color=BLUE_RAMP[2],
                width=0.55, linewidth=0.9, log_scale=True, fliersize=2,
                flierprops={"markerfacecolor": INK_MUTED, "markeredgecolor": "none"},
                medianprops={"color": BLUE_RAMP[6], "linewidth": 1.8}, ax=ax)
    medians = answered.groupby("tool")["hours_to_first_answer"].median()
    ax.set_yticks(range(len(tool_order)), [f"{tool}\nmedian {medians[tool]:.1f} h" for tool in tool_order])
    for hours, label in [(1, "1 hour"), (24, "1 day"), (24 * 7, "1 week")]:
        ax.axvline(hours, color=INK_MUTED, linewidth=0.8, linestyle="--")
        ax.text(hours, -0.75, label, fontsize=8.5, color=INK_MUTED, ha="center")
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda value, _: f"{value:g}"))
    ax.set_xlabel("Hours from question to first answer (log scale)")
    ax.set_ylabel("")
    ax.set_title("How long the first answer takes (answered questions only)", pad=18)
    save(fig, "fig3_time_to_first_answer.png")


def plot_reputation_effect(by_reputation: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.4))
    x = np.arange(len(by_reputation))
    for offset, (column, label, color) in zip((-0.21, 0.21), [("has_answer", "Got an answer", SERIES[0]),
                                                              ("has_accepted_answer", "Accepted an answer", SERIES[1])]):
        bars = ax.bar(x + offset, by_reputation[column], width=0.4, color=color, label=label)
        ax.bar_label(bars, fmt="%.0f%%", padding=2, fontsize=9, color=INK_SECONDARY)
    ax.set_xticks(x, by_reputation.index)
    ax.grid(axis="x", visible=False)
    ax.set_ylim(0, 100)
    ax.set_xlabel("Reputation of the asker")
    ax.set_ylabel("Share of questions (%)")
    ax.set_title("Experienced askers accept answers more often")
    ax.legend(loc="upper left")
    save(fig, "fig4_reputation_effect.png")


def plot_posting_heatmap(questions: pd.DataFrame) -> None:
    counts = pd.crosstab(questions["creation_weekday"], questions["creation_hour_utc"]).reindex(WEEKDAYS)
    counts = counts.reindex(columns=range(24), fill_value=0)
    fig, ax = plt.subplots(figsize=(11, 3.8))
    sns.heatmap(counts, cmap=LinearSegmentedColormap.from_list("blue", BLUE_RAMP), linewidths=1, linecolor=SURFACE,
                cbar_kws={"label": "Questions", "pad": 0.01}, ax=ax)
    ax.set_xlabel("Hour of day (UTC)")
    ax.set_ylabel("")
    ax.set_title("When questions are posted: weekday × hour (UTC)")
    save(fig, "fig5_posting_heatmap.png")


def run() -> dict:
    config.FIGURES_DIR.mkdir(exist_ok=True)
    questions, per_tool = load_from_db()
    overview = describe_dataset(questions)
    stats = grouped_analysis(questions, per_tool)

    tool_order = stats["tools"].index.tolist()
    plot_monthly_volume(per_tool, tool_order)
    plot_outcome_by_tool(per_tool, tool_order)
    plot_time_to_answer(per_tool, tool_order)
    plot_reputation_effect(stats["by_reputation"])
    plot_posting_heatmap(questions)

    summary = {key: value for key, value in stats.items() if not isinstance(value, pd.DataFrame)}
    summary["tools"] = stats["tools"].to_dict(orient="index")
    summary["rows"], summary["columns"] = overview["shape"]
    with open(config.DATA_DIR / "eda_summary.json", "w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, default=str)
    return summary


if __name__ == "__main__":
    config.setup_logging()
    run()
