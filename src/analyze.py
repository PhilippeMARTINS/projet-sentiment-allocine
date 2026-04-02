"""
analyze.py
----------
Module d'analyse : visualisations des avis Allociné
et de l'analyse de sentiment.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from pathlib import Path


PROCESSED_PATH = Path("data/processed")
OUTPUT_PATH    = Path("outputs")
OUTPUT_PATH.mkdir(exist_ok=True)
sns.set_theme(style="whitegrid")

COULEURS = {
    "film":   "#2563EB",
    "series": "#16A34A",
}


def load_data() -> pd.DataFrame:
    """Charge les données transformées."""
    df = pd.read_csv(PROCESSED_PATH / "reviews_clean.csv", encoding="utf-8-sig")
    print(f"✅ {len(df)} avis chargés")
    return df


def plot_note_vs_sentiment(df: pd.DataFrame) -> None:
    """
    Comparaison note Allociné vs score sentiment NLP par contenu.
    C'est le graphique star du projet.
    """
    stats = df.groupby("content_name").agg(
        note_moyenne=("note", "mean"),
        sentiment_moyen=("sentiment_score", "mean"),
    ).round(2).reset_index()

    x = range(len(stats))
    width = 0.35

    fig, ax = plt.subplots(figsize=(14, 6))
    bars1 = ax.bar([i - width/2 for i in x], stats["note_moyenne"],
                   width, label="Note Allociné", color="#2563EB", alpha=0.85)
    bars2 = ax.bar([i + width/2 for i in x], stats["sentiment_moyen"],
                   width, label="Score Sentiment NLP", color="#DC2626", alpha=0.85)

    ax.set_xticks(list(x))
    ax.set_xticklabels(stats["content_name"], rotation=20, ha="right", fontsize=10)
    ax.set_ylabel("Score (/5)")
    ax.set_ylim(0, 5.5)
    ax.set_title("Note Allociné vs Score Sentiment NLP par contenu",
                 fontsize=14, fontweight="bold")
    ax.legend()

    # Annotations valeurs
    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                f"{bar.get_height():.2f}", ha="center", va="bottom", fontsize=8)
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.05,
                f"{bar.get_height():.2f}", ha="center", va="bottom", fontsize=8)

    plt.tight_layout()
    plt.savefig(OUTPUT_PATH / "note_vs_sentiment.png", dpi=150)
    plt.close()
    print("✅ note_vs_sentiment.png sauvegardé")


def plot_coherence(df: pd.DataFrame) -> None:
    """Distribution de la cohérence note/sentiment."""
    coherence_counts = df["coherence"].value_counts()

    couleurs_coherence = {
        "coherent":    "#16A34A",
        "sous-estime": "#D97706",
        "sur-estime":  "#DC2626",
    }

    fig, ax = plt.subplots(figsize=(8, 6))
    bars = ax.bar(
        coherence_counts.index,
        coherence_counts.values,
        color=[couleurs_coherence.get(c, "#6B7280") for c in coherence_counts.index],
        alpha=0.85, edgecolor="white"
    )

    for bar, val in zip(bars, coherence_counts.values):
        pct = val / len(df) * 100
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5,
                f"{val}\n({pct:.1f}%)", ha="center", va="bottom", fontsize=11)

    ax.set_title("Cohérence entre note Allociné et sentiment NLP",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Catégorie")
    ax.set_ylabel("Nombre d'avis")
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH / "coherence_note_sentiment.png", dpi=150)
    plt.close()
    print("✅ coherence_note_sentiment.png sauvegardé")


def plot_distribution_notes(df: pd.DataFrame) -> None:
    """Distribution des notes par type de contenu."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    for ax, content_type in zip(axes, ["film", "series"]):
        subset = df[df["content_type"] == content_type]
        ax.hist(subset["note"], bins=[0.5, 1.5, 2.5, 3.5, 4.5, 5.5],
                color=COULEURS[content_type], alpha=0.85, edgecolor="white")
        ax.set_title(f"Distribution des notes — {content_type.capitalize()}s",
                     fontsize=12, fontweight="bold")
        ax.set_xlabel("Note (/5)")
        ax.set_ylabel("Nombre d'avis")
        ax.set_xticks([1, 2, 3, 4, 5])

    plt.tight_layout()
    plt.savefig(OUTPUT_PATH / "distribution_notes.png", dpi=150)
    plt.close()
    print("✅ distribution_notes.png sauvegardé")


def plot_sentiment_par_type(df: pd.DataFrame) -> None:
    """Score sentiment moyen par type de contenu."""
    stats = df.groupby(["content_name", "content_type"])["sentiment_score"].mean().round(2)
    stats = stats.reset_index().sort_values("sentiment_score", ascending=False)

    couleurs = [COULEURS.get(t, "#6B7280") for t in stats["content_type"]]

    fig, ax = plt.subplots(figsize=(12, 5))
    bars = ax.barh(stats["content_name"], stats["sentiment_score"],
                   color=couleurs, alpha=0.85)

    for bar, val in zip(bars, stats["sentiment_score"]):
        ax.text(bar.get_width() + 0.03, bar.get_y() + bar.get_height()/2,
                f"{val:.2f}", va="center", fontsize=10, fontweight="bold")

    ax.set_xlabel("Score sentiment moyen (/5)")
    ax.set_title("Score sentiment NLP moyen par contenu",
                 fontsize=14, fontweight="bold")
    ax.set_xlim(0, 5.5)

    # Légende manuelle
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor="#2563EB", label="Film"),
        Patch(facecolor="#16A34A", label="Série"),
    ]
    ax.legend(handles=legend_elements, loc="lower right")
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH / "sentiment_par_contenu.png", dpi=150)
    plt.close()
    print("✅ sentiment_par_contenu.png sauvegardé")


def plot_heatmap_coherence(df: pd.DataFrame) -> None:
    """Heatmap cohérence par contenu."""
    pivot = df.groupby(["content_name", "coherence"]).size().unstack(fill_value=0)
    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.heatmap(pivot_pct, annot=True, fmt=".1f", cmap="RdYlGn",
                linewidths=0.5, ax=ax, cbar_kws={"label": "%"})
    ax.set_title("Cohérence note/sentiment par contenu (%)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Cohérence")
    ax.set_ylabel("Contenu")
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH / "heatmap_coherence.png", dpi=150)
    plt.close()
    print("✅ heatmap_coherence.png sauvegardé")


def run_analysis() -> None:
    """Lance toutes les visualisations."""
    print("=== ANALYSE & VISUALISATION ===")
    df = load_data()
    plot_note_vs_sentiment(df)
    plot_coherence(df)
    plot_distribution_notes(df)
    plot_sentiment_par_type(df)
    plot_heatmap_coherence(df)
    print("\n✅ Toutes les visualisations sont dans outputs/")


if __name__ == "__main__":
    run_analysis()