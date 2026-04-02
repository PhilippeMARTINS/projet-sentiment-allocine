"""
app.py
------
Dashboard Streamlit — Analyse de sentiment Allociné.
Lancer avec : streamlit run app.py
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import streamlit as st
from pathlib import Path
from google.cloud import bigquery


PROJECT_ID = "project-b4d81ada-e874-4f3f-82e"
DATASET_ID = "sentiment_allocine"
TABLE_ID   = "reviews"

sns.set_theme(style="whitegrid")

st.set_page_config(
    page_title="Sentiment Allociné Dashboard",
    page_icon="🎬",
    layout="wide",
)

COULEURS = {
    "film":   "#2563EB",
    "series": "#16A34A",
}


# ── Helpers ────────────────────────────────────────────────────────────────────
@st.cache_data
def load_data() -> pd.DataFrame:
    """Charge les données depuis BigQuery."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query  = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        df     = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Erreur BigQuery : {e}")
        # Fallback sur le CSV local
        return pd.read_csv("data/processed/reviews_clean.csv", encoding="utf-8-sig")


# ── Chargement ─────────────────────────────────────────────────────────────────
df_full = load_data()

# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.title("🔧 Filtres")

types = ["Tous"] + sorted(df_full["content_type"].unique().tolist())
selected_type = st.sidebar.selectbox("Type de contenu", types)

contenus = sorted(df_full["content_name"].unique().tolist())
selected_contenus = st.sidebar.multiselect(
    "Contenu(s)", options=contenus, default=contenus
)

coherences = sorted(df_full["coherence"].unique().tolist())
selected_coherences = st.sidebar.multiselect(
    "Cohérence", options=coherences, default=coherences
)

st.sidebar.markdown("---")
st.sidebar.markdown("**💡 Astuce** : laisse vide pour tout afficher.")

# Filtrage
df = df_full.copy()
if selected_type != "Tous":
    df = df[df["content_type"] == selected_type]
if selected_contenus:
    df = df[df["content_name"].isin(selected_contenus)]
if selected_coherences:
    df = df[df["coherence"].isin(selected_coherences)]

if df.empty:
    df = df_full.copy()


# ── Titre ──────────────────────────────────────────────────────────────────────
st.title("🎬 Sentiment Allociné Dashboard")
st.caption("Scraping · NLP · GCP Cloud Storage · BigQuery · Streamlit")
st.markdown("---")


# ── KPIs ───────────────────────────────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("📝 Avis analysés",     f"{len(df):,}".replace(",", " "))
col2.metric("⭐ Note moyenne",       f"{df['note'].mean():.2f}/5")
col3.metric("🤖 Sentiment moyen",   f"{df['sentiment_score'].mean():.2f}/5")
col4.metric("✅ Avis cohérents",    f"{(df['coherence']=='coherent').sum():,}".replace(",", " "))
pct = (df['coherence'] == 'coherent').mean() * 100
col5.metric("📊 Taux cohérence",    f"{pct:.1f}%")

st.markdown("---")


# ── Graphique 1 — Note vs Sentiment ───────────────────────────────────────────
st.subheader("⭐ Note Allociné vs Score Sentiment NLP")
st.caption("Un écart révèle un décalage entre ce que les spectateurs notent et ce qu'ils écrivent.")

stats = df.groupby("content_name").agg(
    note_moyenne=("note", "mean"),
    sentiment_moyen=("sentiment_score", "mean"),
).round(2).reset_index().sort_values("note_moyenne", ascending=False)

x     = range(len(stats))
width = 0.35

fig1, ax1 = plt.subplots(figsize=(14, 5))
ax1.bar([i - width/2 for i in x], stats["note_moyenne"],
        width, label="Note Allociné", color="#2563EB", alpha=0.85)
ax1.bar([i + width/2 for i in x], stats["sentiment_moyen"],
        width, label="Score Sentiment NLP", color="#DC2626", alpha=0.85)
ax1.set_xticks(list(x))
ax1.set_xticklabels(stats["content_name"], rotation=20, ha="right", fontsize=10)
ax1.set_ylabel("Score (/5)")
ax1.set_ylim(0, 5.5)
ax1.legend()
plt.tight_layout()
st.pyplot(fig1)
plt.close()

st.markdown("---")


# ── Graphique 2 — Cohérence ────────────────────────────────────────────────────
st.subheader("🔍 Cohérence note / sentiment")

col_a, col_b = st.columns(2)

with col_a:
    coherence_counts = df["coherence"].value_counts()
    couleurs_c = {"coherent": "#16A34A", "sous-estime": "#D97706", "sur-estime": "#DC2626"}

    fig2, ax2 = plt.subplots(figsize=(6, 4))
    ax2.bar(coherence_counts.index, coherence_counts.values,
            color=[couleurs_c.get(c, "#6B7280") for c in coherence_counts.index],
            alpha=0.85, edgecolor="white")
    for i, (idx, val) in enumerate(coherence_counts.items()):
        pct_val = val / len(df) * 100
        ax2.text(i, val + 3, f"{val}\n({pct_val:.1f}%)", ha="center", fontsize=10)
    ax2.set_title("Répartition de la cohérence", fontweight="bold")
    ax2.set_ylabel("Nombre d'avis")
    plt.tight_layout()
    st.pyplot(fig2)
    plt.close()

with col_b:
    pivot = df.groupby(["content_name", "coherence"]).size().unstack(fill_value=0)
    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100

    fig3, ax3 = plt.subplots(figsize=(6, 4))
    sns.heatmap(pivot_pct, annot=True, fmt=".1f", cmap="RdYlGn",
                linewidths=0.5, ax=ax3, cbar_kws={"label": "%"})
    ax3.set_title("Cohérence par contenu (%)", fontweight="bold")
    ax3.set_xlabel("")
    ax3.set_ylabel("")
    plt.tight_layout()
    st.pyplot(fig3)
    plt.close()

st.markdown("---")


# ── Graphique 3 — Distribution des notes ──────────────────────────────────────
st.subheader("📊 Distribution des notes et sentiments")

col_c, col_d = st.columns(2)

with col_c:
    fig4, ax4 = plt.subplots(figsize=(6, 4))
    ax4.hist(df["note"], bins=[0.5, 1.5, 2.5, 3.5, 4.5, 5.5],
             color="#2563EB", alpha=0.85, edgecolor="white")
    ax4.set_title("Distribution des notes Allociné", fontweight="bold")
    ax4.set_xlabel("Note (/5)")
    ax4.set_ylabel("Nombre d'avis")
    ax4.set_xticks([1, 2, 3, 4, 5])
    plt.tight_layout()
    st.pyplot(fig4)
    plt.close()

with col_d:
    fig5, ax5 = plt.subplots(figsize=(6, 4))
    ax5.hist(df["sentiment_score"], bins=[0.5, 1.5, 2.5, 3.5, 4.5, 5.5],
             color="#DC2626", alpha=0.85, edgecolor="white")
    ax5.set_title("Distribution des scores sentiment NLP", fontweight="bold")
    ax5.set_xlabel("Score sentiment (/5)")
    ax5.set_ylabel("Nombre d'avis")
    ax5.set_xticks([1, 2, 3, 4, 5])
    plt.tight_layout()
    st.pyplot(fig5)
    plt.close()

st.markdown("---")


# ── Section SQL ────────────────────────────────────────────────────────────────
st.subheader("🧮 Requête BigQuery personnalisée")
st.caption(f"Table disponible : `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`")

default_sql = f"""SELECT
  content_name,
  content_type,
  ROUND(AVG(note), 2)            AS note_moyenne,
  ROUND(AVG(sentiment_score), 2) AS sentiment_moyen,
  COUNT(*)                       AS nb_avis
FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
GROUP BY content_name, content_type
ORDER BY note_moyenne DESC"""

custom_sql = st.text_area("Requête SQL", value=default_sql, height=150)

if st.button("▶️ Exécuter"):
    try:
        client    = bigquery.Client(project=PROJECT_ID)
        df_custom = client.query(custom_sql).to_dataframe()
        st.success(f"{len(df_custom)} ligne(s) retournée(s)")
        st.dataframe(df_custom, use_container_width=True)
    except Exception as e:
        st.error(f"Erreur BigQuery : {e}")

st.markdown("---")
st.caption("Projet réalisé par **Philippe Morais Martins** · M2 Data Engineering · Paris Ynov Campus")