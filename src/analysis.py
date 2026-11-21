"""Exploratory data analysis of the results stored in SQLite, with Matplotlib/Seaborn figures."""

import json
import logging
import sqlite3

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

from src import config  # noqa: E402

log = logging.getLogger("analysis")

INK, INK_SECONDARY, INK_MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]  # validated categorical slots 1-3 (CVD-safe as a set)
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
NUMERIC_COLUMNS = ["grid", "position", "points", "laps", "positions_gained", "race_time_min", "gap_to_winner_s",
                   "fastest_lap_s", "lap_pace_gap_pct", "driver_age"]
CATEGORICAL_COLUMNS = ["season", "team_name", "driver_name", "country", "status_group", "result_category"]

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "sans-serif", "font.size": 10, "text.color": INK,
    "axes.edgecolor": AXIS, "axes.labelcolor": INK_SECONDARY, "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.titlelocation": "left", "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "xtick.color": INK_MUTED, "ytick.color": INK_MUTED, "legend.frameon": False,
})


def load_from_db(db_path=config.DB_PATH) -> pd.DataFrame:
    with sqlite3.connect(db_path) as connection:
        df = pd.read_sql_query("SELECT * FROM v_results", connection, parse_dates=["race_date"])
    connection.close()
    df["is_classified"] = df["is_classified"].astype(bool)
    df["race_time_min"] = (df["race_time_ms"] / 60_000).round(2)
    return df


def describe_dataset(df: pd.DataFrame) -> dict:
    log.info("Dataset: %d rows x %d columns", *df.shape)
    log.info("Data types:\n%s", df.dtypes.value_counts().to_string())
    numeric = df[NUMERIC_COLUMNS].describe().T.round(2)
    numeric["skew"] = df[NUMERIC_COLUMNS].skew().round(2)
    log.info("Numerical statistics:\n%s", numeric.to_string())

    categorical = pd.DataFrame({
        "unique": df[CATEGORICAL_COLUMNS].nunique(),
        "most_frequent": df[CATEGORICAL_COLUMNS].astype(str).mode().iloc[0],
        "frequency": [df[col].value_counts().iloc[0] for col in CATEGORICAL_COLUMNS],
    })
    log.info("Categorical statistics:\n%s", categorical.to_string())
    log.info("Result status shares:\n%s", df["status_group"].value_counts(normalize=True).mul(100).round(1).to_string())
    return {"shape": list(df.shape), "numeric": numeric, "categorical": categorical}


def grouped_analysis(df: pd.DataFrame) -> dict:
    finishers = df[df["is_classified"]]
    rho = finishers["grid_effective"].corr(finishers["position"], method="spearman")
    winners = df[df["position"] == 1]

    by_grid = (df[df["grid_effective"] <= 10]
               .assign(win=lambda d: d["position"].eq(1), podium=lambda d: d["position"].le(3))
               .groupby("grid_effective")[["win", "podium"]].mean().mul(100).round(1))

    teams = (df.assign(dnf=df["status_group"].eq("DNF"))
               .groupby("team_name")
               .agg(points=("points", "sum"), dnf_rate=("dnf", "mean"), median_lap_gap=("lap_pace_gap_pct", "median"))
               .sort_values("points", ascending=False))
    team_season = (finishers.groupby(["season", "team_name"])
                   .agg(points=("points", "sum"), median_lap_gap=("lap_pace_gap_pct", "median")))
    pace_points_rho = team_season["median_lap_gap"].corr(team_season["points"], method="spearman")

    drivers = (df.groupby(["season", "driver_name"])["points"].sum()
                 .groupby(level="season", group_keys=False).nlargest(2))
    title_margins = drivers.groupby(level="season").agg(lambda pts: pts.iloc[0] - pts.iloc[1])

    movers = (finishers.groupby("driver_name")
                       .agg(races=("position", "size"), avg_gained=("positions_gained", "mean"))
                       .query("races >= 20").sort_values("avg_gained", ascending=False).round(2))

    stats = {
        "spearman_grid_vs_finish": round(rho, 3),
        "wins_from_pole_pct": round((winners["grid_effective"] == 1).mean() * 100, 1),
        "wins_from_front_row_pct": round((winners["grid_effective"] <= 2).mean() * 100, 1),
        "wins_from_outside_top5_pct": round((winners["grid_effective"] > 5).mean() * 100, 1),
        "overall_dnf_rate_pct": round(df["status_group"].eq("DNF").mean() * 100, 1),
        "spearman_team_pace_vs_points": round(pace_points_rho, 3),
        "title_margin_race_points": title_margins.to_dict(),
        "win_podium_rate_by_grid": by_grid,
        "teams": teams.round(3),
        "best_movers": movers.head(5),
    }
    log.info("Spearman correlation grid vs finish (classified finishers): %.3f", stats["spearman_grid_vs_finish"])
    log.info("Wins from pole: %.1f%%, from the front row: %.1f%%, from outside the top 5: %.1f%%",
             stats["wins_from_pole_pct"], stats["wins_from_front_row_pct"], stats["wins_from_outside_top5_pct"])
    log.info("Win / podium rate by grid slot (%%):\n%s", by_grid.to_string())
    log.info("Team totals 2023-2025:\n%s", teams.round(3).to_string())
    log.info("Spearman team median lap gap vs team points (per season): %.3f", pace_points_rho)
    log.info("Title margin in race points: %s", stats["title_margin_race_points"])
    log.info("Most positions gained on average (>= 20 classified finishes):\n%s", movers.head(5).to_string())
    return stats


def save(fig: plt.Figure, name: str) -> None:
    fig.savefig(config.FIGURES_DIR / name, dpi=200, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved figure %s", name)


def plot_team_points(df: pd.DataFrame) -> None:
    points = df.pivot_table(index="team_name", columns="season", values="points", aggfunc="sum")
    points = points.sort_values(points.columns[-1])
    fig, ax = plt.subplots(figsize=(8, 6))
    y = np.arange(len(points))
    height = 0.26
    for offset, (season, color) in zip((-1, 0, 1), zip(points.columns, SERIES)):
        ax.barh(y + offset * (height + 0.02), points[season], height=height, color=color, label=str(season))
    ax.set_yticks(y, points.index)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Race points (sprints excluded)")
    ax.set_title("Constructor points by season, 2023–2025")
    ax.legend(title="Season", loc="lower right", reverse=True)
    save(fig, "fig1_team_points_by_season.png")


def plot_grid_vs_finish(df: pd.DataFrame, rho: float) -> None:
    finishers = df[df["is_classified"] & (df["grid_effective"] <= 20)]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    sns.boxplot(data=finishers, x="grid_effective", y="position", color=BLUE_RAMP[2], width=0.6, linewidth=0.9,
                fliersize=2.5, flierprops={"markerfacecolor": INK_MUTED, "markeredgecolor": "none"},
                medianprops={"color": BLUE_RAMP[6], "linewidth": 1.6}, ax=ax)
    ax.plot(range(20), range(1, 21), color=INK_MUTED, linewidth=1)
    ax.set_yticks([1, 5, 10, 15, 20])
    ax.invert_yaxis()
    ax.set_xlabel("Starting grid position (pit-lane start = last)")
    ax.set_ylabel("Finishing position")
    ax.set_title("Where drivers finish by where they start", pad=22)
    ax.text(0, 1.015, f"Classified finishers, 2023–2025 · Spearman ρ = {rho:.2f} · grey line: finish = start",
            transform=ax.transAxes, fontsize=9.5, color=INK_SECONDARY)
    save(fig, "fig2_grid_vs_finish.png")


def plot_win_podium_by_grid(by_grid: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.5))
    x = np.arange(len(by_grid))
    ax.bar(x - 0.21, by_grid["win"], width=0.4, color=SERIES[0], label="Win")
    ax.bar(x + 0.21, by_grid["podium"], width=0.4, color=SERIES[1], label="Podium")
    for xi, value in zip(x[:3], by_grid["win"].iloc[:3]):
        ax.annotate(f"{value:.0f}%", (xi - 0.21, value), xytext=(0, 3), textcoords="offset points",
                    ha="center", fontsize=9, color=INK_SECONDARY)
    ax.set_xticks(x, by_grid.index)
    ax.grid(axis="x", visible=False)
    ax.set_ylim(0, 100)
    ax.set_xlabel("Starting grid position")
    ax.set_ylabel("Share of starts (%)")
    ax.set_title("Chance of a win or podium by starting position")
    ax.legend()
    save(fig, "fig3_win_podium_rate_by_grid.png")


def plot_dnf_rate(teams: pd.DataFrame) -> None:
    dnf = teams["dnf_rate"].mul(100).sort_values()
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(dnf.index, dnf, height=0.6, color=SERIES[0])
    for team, value in dnf.items():
        ax.annotate(f"{value:.1f}%", (value, team), xytext=(4, 0), textcoords="offset points",
                    va="center", fontsize=9, color=INK_SECONDARY)
    ax.axvline(dnf.mean(), color=INK_MUTED, linewidth=1)
    ax.annotate(f"mean {dnf.mean():.1f}%", (dnf.mean(), 0), xytext=(4, 0), textcoords="offset points",
                va="center", fontsize=9, color=INK_MUTED)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Entries ending in a retirement (DNF), %")
    ax.set_title("Reliability: retirement rate by team, 2023–2025")
    save(fig, "fig4_dnf_rate_by_team.png")


def spread_labels(values: pd.Series, min_gap: float) -> pd.Series:
    """Label y-positions for line ends, pushed apart so that neighbouring labels never overlap."""
    positions = {}
    for key, value in values.sort_values(ascending=False).items():
        positions[key] = value if not positions else min(value, list(positions.values())[-1] - min_gap)
    return pd.Series(positions)


def plot_championship_progress(progress: pd.DataFrame) -> None:
    seasons = sorted(progress["season"].unique())
    fig, axes = plt.subplots(1, len(seasons), figsize=(12, 4.2), sharey=True)
    for ax, season in zip(np.atleast_1d(axes), seasons):
        season_df = progress[progress["season"] == season]
        final = season_df.groupby("driver_code")["cumulative_points"].max().nlargest(3)
        label_y = spread_labels(final, min_gap=progress["cumulative_points"].max() * 0.06)
        for rank, driver in enumerate(final.index):
            line = season_df[season_df["driver_code"] == driver]
            color, width = (SERIES[0], 2.2) if rank == 0 else ((INK_SECONDARY, 1.4) if rank == 1 else (INK_MUTED, 1.4))
            ax.plot(line["round"], line["cumulative_points"], color=color, linewidth=width)
            ax.annotate(f"{driver} {final[driver]:.0f}", (line["round"].iloc[-1] + 0.6, label_y[driver]),
                        va="center", fontsize=9, color=INK)
        ax.set_title(str(season))
        ax.set_xlabel("Round")
        ax.set_xlim(1, season_df["round"].max() + 5)
    axes[0].set_ylabel("Cumulative race points")
    fig.suptitle("Title fights: champion (blue) vs the next two drivers", x=0.01, ha="left",
                 fontsize=12, fontweight="bold")
    fig.tight_layout()
    save(fig, "fig5_championship_progress.png")


def plot_pace_heatmap(df: pd.DataFrame) -> None:
    finishers = df[df["is_classified"]]
    pace = finishers.pivot_table(index="team_name", columns="season", values="lap_pace_gap_pct", aggfunc="median")
    pace = pace.loc[pace.mean(axis=1).sort_values().index]
    fig, ax = plt.subplots(figsize=(6, 5.5))
    sns.heatmap(pace, annot=True, fmt=".2f", cmap=LinearSegmentedColormap.from_list("blue", BLUE_RAMP),
                linewidths=2, linecolor=SURFACE, cbar_kws={"label": "Median gap to race fastest lap, %"}, ax=ax)
    ax.set_xlabel("Season")
    ax.set_ylabel("")
    ax.set_title("Race pace: gap to the fastest lap\n(lower = faster)")
    save(fig, "fig6_lap_pace_heatmap.png")


def run() -> dict:
    config.FIGURES_DIR.mkdir(exist_ok=True)
    df = load_from_db()
    overview = describe_dataset(df)
    stats = grouped_analysis(df)

    progress = pd.read_csv(config.AGGREGATES_DIR / "championship_progress.csv")
    plot_team_points(df)
    plot_grid_vs_finish(df, stats["spearman_grid_vs_finish"])
    plot_win_podium_by_grid(stats["win_podium_rate_by_grid"])
    plot_dnf_rate(stats["teams"])
    plot_championship_progress(progress)
    plot_pace_heatmap(df)

    summary = {key: value for key, value in stats.items() if not isinstance(value, pd.DataFrame)}
    summary["rows"], summary["columns"] = overview["shape"]
    with open(config.DATA_DIR / "eda_summary.json", "w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, default=str)
    return summary


if __name__ == "__main__":
    config.setup_logging()
    run()
