"""
app.py
------
Dashboard Streamlit — Analyse de sentiment Allociné.
Lancer avec : streamlit run app.py
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
import seaborn as sns
import numpy as np
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

# ── Variables d'environnement ──────────────────────────────────────────────────
load_dotenv()
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID", "sentiment_allocine")
TABLE_ID   = os.getenv("TABLE_ID",   "reviews")

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

COULEURS_COHERENCE = {
    "coherent":    "#16A34A",
    "sous-estime": "#D97706",
    "sur-estime":  "#DC2626",
}


# ── Helpers ────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    """
    Charge les données depuis BigQuery (avec cache 1h).
    Fallback automatique sur le CSV local si BigQuery est indisponible.
    """
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
        query  = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        df     = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.warning(
            f"⚠️ BigQuery indisponible ({type(e).__name__}) — "
            "les données affichées proviennent du CSV local et ne sont pas live."
        )
        return pd.read_csv(
            "data/processed/reviews_clean.csv", encoding="utf-8-sig"
        )


# ── Chargement ─────────────────────────────────────────────────────────────────
df_full = load_data()

# Nettoyage colonne date pour les graphiques temporels
df_full["date_clean"] = pd.to_datetime(df_full["date_clean"], errors="coerce")

# Longueur du texte (Graph 6)
df_full["longueur_avis"] = df_full["texte"].fillna("").str.len()


# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.title("🔧 Filtres")

types = ["Tous"] + sorted(df_full["content_type"].dropna().unique().tolist())
selected_type = st.sidebar.selectbox("Type de contenu", types)

contenus = sorted(df_full["content_name"].dropna().unique().tolist())
selected_contenus = st.sidebar.multiselect(
    "Contenu(s)", options=contenus, default=contenus
)

coherences = sorted(df_full["coherence"].dropna().unique().tolist())
selected_coherences = st.sidebar.multiselect(
    "Cohérence", options=coherences, default=coherences
)

st.sidebar.markdown("---")
st.sidebar.markdown("**💡 Astuce** : laisse vide pour tout afficher.")


# ── Filtrage ───────────────────────────────────────────────────────────────────
df = df_full.copy()
if selected_type != "Tous":
    df = df[df["content_type"] == selected_type]
if selected_contenus:
    df = df[df["content_name"].isin(selected_contenus)]
if selected_coherences:
    df = df[df["coherence"].isin(selected_coherences)]

# Gestion propre des filtres vides
if df.empty:
    st.info("ℹ️ Aucun résultat pour cette combinaison de filtres. Réinitialise les filtres dans la barre latérale.")
    st.stop()


# ── Titre ──────────────────────────────────────────────────────────────────────
st.title("🎬 Sentiment Allociné Dashboard")
st.caption("Scraping · NLP · GCP Cloud Storage · BigQuery · Streamlit")
st.markdown("---")


# ── KPIs ───────────────────────────────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("📝 Avis analysés",   f"{len(df):,}".replace(",", " "))
col2.metric("⭐ Note moyenne",     f"{df['note'].mean():.2f}/5")
col3.metric("🤖 Sentiment moyen", f"{df['sentiment_score'].mean():.2f}/5")
col4.metric("✅ Avis cohérents",  f"{(df['coherence'] == 'coherent').sum():,}".replace(",", " "))
pct = (df["coherence"] == "coherent").mean() * 100
col5.metric("📊 Taux cohérence",  f"{pct:.1f}%")

st.markdown("---")

# ── Graphique 1 — Note vs Sentiment ───────────────────────────────────────────
st.subheader("⭐ Note Allociné vs Score Sentiment NLP")
st.caption("Un écart révèle un décalage entre ce que les spectateurs notent et ce qu'ils écrivent.")

stats = (
    df.groupby("content_name")
    .agg(note_moyenne=("note", "mean"), sentiment_moyen=("sentiment_score", "mean"))
    .round(2)
    .reset_index()
    .sort_values("note_moyenne", ascending=False)
)

x     = range(len(stats))
width = 0.35

fig1, ax1 = plt.subplots(figsize=(14, 5))
ax1.bar([i - width / 2 for i in x], stats["note_moyenne"],
        width, label="Note Allociné", color="#2563EB", alpha=0.85)
ax1.bar([i + width / 2 for i in x], stats["sentiment_moyen"],
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

    fig2, ax2 = plt.subplots(figsize=(6, 4))
    ax2.bar(
        coherence_counts.index,
        coherence_counts.values,
        color=[COULEURS_COHERENCE.get(c, "#6B7280") for c in coherence_counts.index],
        alpha=0.85,
        edgecolor="white",
    )
    for i, (idx, val) in enumerate(coherence_counts.items()):
        pct_val = val / len(df) * 100
        ax2.text(i, val + 3, f"{val}\n({pct_val:.1f}%)", ha="center", fontsize=10)
    ax2.set_title("Répartition de la cohérence", fontweight="bold")
    ax2.set_ylabel("Nombre d'avis")
    plt.tight_layout()
    st.pyplot(fig2)
    plt.close()

with col_b:
    pivot     = df.groupby(["content_name", "coherence"]).size().unstack(fill_value=0)
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


# ── Graphique 3 — Distribution des notes et sentiments ────────────────────────
st.subheader("📊 Distribution des notes et sentiments")

col_c, col_d = st.columns(2)

with col_c:
    fig4, ax4 = plt.subplots(figsize=(6, 4))
    ax4.hist(df["note"].dropna(), bins=[0.5, 1.5, 2.5, 3.5, 4.5, 5.5],
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
    ax5.hist(df["sentiment_score"].dropna(), bins=[0.5, 1.5, 2.5, 3.5, 4.5, 5.5],
             color="#DC2626", alpha=0.85, edgecolor="white")
    ax5.set_title("Distribution des scores sentiment NLP", fontweight="bold")
    ax5.set_xlabel("Score sentiment (/5)")
    ax5.set_ylabel("Nombre d'avis")
    ax5.set_xticks([1, 2, 3, 4, 5])
    plt.tight_layout()
    st.pyplot(fig5)
    plt.close()

st.markdown("---")


# ── Graphique 6 — Longueur d'avis par sentiment ───────────────────────────────
st.subheader("📝 Longueur des avis par label de sentiment")
st.caption(
    "Les avis négatifs sont-ils plus détaillés que les positifs ? "
    "Un avis long et négatif révèle souvent une déception argumentée."
)

df_len = df[(df["longueur_avis"] > 0) & (df["longueur_avis"] <= 5000)].copy()

if not df_len.empty:
    # Palette adaptée aux labels bruts HuggingFace (1 star → rouge, 5 stars → vert)
    PALETTE_STARS = {
        "1 star":  "#DC2626",
        "2 stars": "#F97316",
        "3 stars": "#D97706",
        "4 stars": "#65A30D",
        "5 stars": "#16A34A",
    }
    ordre_labels = ["1 star", "2 stars", "3 stars", "4 stars", "5 stars"]
    # Filtrer les labels présents dans les données filtrées
    ordre_labels = [l for l in ordre_labels if l in df_len["sentiment_label"].values]

    fig6, ax6 = plt.subplots(figsize=(10, 5))
    sns.violinplot(
        data=df_len,
        x="sentiment_label",
        y="longueur_avis",
        order=ordre_labels,
        palette={k: v for k, v in PALETTE_STARS.items() if k in ordre_labels},
        inner="quartile",
        ax=ax6,
    )
    ax6.set_title("Distribution de la longueur des avis par sentiment", fontweight="bold", fontsize=13)
    ax6.set_xlabel("Label sentiment")
    ax6.set_ylabel("Longueur de l'avis (caractères)")
    plt.tight_layout()
    st.pyplot(fig6)
    plt.close()
else:
    st.info("ℹ️ Pas assez de données textuelles pour ce graphique avec les filtres actuels.")

st.markdown("---")


# ── Graphique 7 — Évolution temporelle du sentiment ───────────────────────────
st.subheader("📅 Évolution temporelle du sentiment")
st.caption(
    "Le sentiment des spectateurs évolue-t-il dans le temps ? "
    "Un film peut être réévalué des années après sa sortie."
)

df_time = df[df["date_clean"].notna()].copy()

if len(df_time) >= 10:
    df_time["periode"] = df_time["date_clean"].dt.to_period("M").astype(str)
    evolution = (
        df_time.groupby("periode")
        .agg(sentiment_moyen=("sentiment_score", "mean"), nb_avis=("sentiment_score", "count"))
        .reset_index()
        .sort_values("periode")
    )

    fig7, ax7 = plt.subplots(figsize=(14, 5))

    # Courbe sentiment moyen
    ax7.plot(
        evolution["periode"],
        evolution["sentiment_moyen"],
        color="#2563EB",
        linewidth=2,
        marker="o",
        markersize=5,
        label="Sentiment moyen",
    )

    # Zone de confiance (±0.2 autour de la courbe)
    ax7.fill_between(
        evolution["periode"],
        evolution["sentiment_moyen"] - 0.2,
        evolution["sentiment_moyen"] + 0.2,
        alpha=0.15,
        color="#2563EB",
    )

    # Ligne de référence : sentiment global moyen
    global_mean = df["sentiment_score"].mean()
    ax7.axhline(global_mean, color="#DC2626", linestyle="--", linewidth=1.5,
                label=f"Moyenne globale ({global_mean:.2f})")

    # Formatage de l'axe X : 1 tick sur 3 pour éviter la surcharge
    ticks = evolution["periode"].tolist()
    step  = max(1, len(ticks) // 10)
    ax7.set_xticks(range(0, len(ticks), step))
    ax7.set_xticklabels([ticks[i] for i in range(0, len(ticks), step)],
                         rotation=30, ha="right", fontsize=9)

    ax7.set_ylabel("Score sentiment moyen (/5)")
    ax7.set_ylim(1, 5)
    ax7.legend()
    plt.tight_layout()
    st.pyplot(fig7)
    plt.close()
else:
    st.info("ℹ️ Pas assez de données datées pour tracer l'évolution temporelle avec les filtres actuels.")

st.markdown("---")


# ── Graphique 8 — Score de confiance NLP ──────────────────────────────────────
st.subheader("🎯 Score de confiance du modèle NLP")
st.caption(
    "La confiance indique à quel point le modèle HuggingFace est certain de son label. "
    "Un score proche de 1 = prédiction très fiable."
)

df_conf = df[df["sentiment_confidence"].notna()].copy()

if not df_conf.empty:
    col_e, col_f = st.columns(2)

    with col_e:
        # Histogramme global de la confiance
        fig8a, ax8a = plt.subplots(figsize=(6, 4))
        ax8a.hist(df_conf["sentiment_confidence"], bins=20,
                  color="#7C3AED", alpha=0.85, edgecolor="white")

        mean_conf = df_conf["sentiment_confidence"].mean()
        ax8a.axvline(mean_conf, color="#DC2626", linestyle="--", linewidth=2,
                     label=f"Moyenne : {mean_conf:.2f}")

        ax8a.set_title("Distribution de la confiance NLP", fontweight="bold")
        ax8a.set_xlabel("Score de confiance")
        ax8a.set_ylabel("Nombre d'avis")
        ax8a.legend()
        plt.tight_layout()
        st.pyplot(fig8a)
        plt.close()

    with col_f:
        # Confiance moyenne par label sentiment
        conf_par_label = (
            df_conf.groupby("sentiment_label")["sentiment_confidence"]
            .mean()
            .reset_index()
            .sort_values("sentiment_confidence", ascending=False)
        )

        couleurs_labels = {
            "positive": "#16A34A",
            "negative": "#DC2626",
            "neutral":  "#D97706",
        }

        fig8b, ax8b = plt.subplots(figsize=(6, 4))
        ax8b.barh(
            conf_par_label["sentiment_label"],
            conf_par_label["sentiment_confidence"],
            color=[couleurs_labels.get(l, "#6B7280") for l in conf_par_label["sentiment_label"]],
            alpha=0.85,
            edgecolor="white",
        )
        for i, (_, row) in enumerate(conf_par_label.iterrows()):
            ax8b.text(row["sentiment_confidence"] + 0.005, i,
                      f"{row['sentiment_confidence']:.3f}", va="center", fontsize=10)
        ax8b.set_title("Confiance moyenne par label", fontweight="bold")
        ax8b.set_xlabel("Confiance moyenne")
        ax8b.set_xlim(0, 1.1)
        plt.tight_layout()
        st.pyplot(fig8b)
        plt.close()

    # KPI fiabilité : part des avis avec confiance > 0.8
    fiables = (df_conf["sentiment_confidence"] >= 0.8).mean() * 100
    st.info(f"🔬 **{fiables:.1f}%** des prédictions ont un score de confiance ≥ 0.80 — le modèle est très fiable sur ce corpus.")
else:
    st.info("ℹ️ La colonne `sentiment_confidence` est vide pour les filtres actuels.")

st.markdown("---")


# ── Graphique 9 — Scatter Note vs Sentiment individuel ────────────────────────
st.subheader("🔵 Note vs Sentiment — vue par avis individuel")
st.caption(
    "Chaque point = un avis. La droite diagonale représente la cohérence parfaite. "
    "Les points éloignés de la diagonale sont les avis incohérents."
)

df_scatter = df[df["note"].notna() & df["sentiment_score"].notna()].copy()

if len(df_scatter) >= 5:
    fig9, ax9 = plt.subplots(figsize=(9, 7))

    # Points colorés par cohérence
    for coh, color in COULEURS_COHERENCE.items():
        subset = df_scatter[df_scatter["coherence"] == coh]
        if not subset.empty:
            # Jitter léger pour éviter la superposition des points entiers
            jitter = np.random.uniform(-0.08, 0.08, size=len(subset))
            ax9.scatter(
                subset["note"] + jitter,
                subset["sentiment_score"] + jitter,
                c=color,
                alpha=0.45,
                s=30,
                label=coh,
            )

    # Droite de cohérence parfaite (y = x)
    ax9.plot([1, 5], [1, 5], color="#111827", linewidth=1.5, linestyle="--",
             label="Cohérence parfaite")

    # Ligne de régression
    z   = np.polyfit(df_scatter["note"], df_scatter["sentiment_score"], 1)
    p   = np.poly1d(z)
    x_  = np.linspace(1, 5, 100)
    ax9.plot(x_, p(x_), color="#6B7280", linewidth=1.5, linestyle="-",
             label=f"Régression (pente={z[0]:.2f})")

    ax9.set_xlabel("Note Allociné (/5)", fontsize=12)
    ax9.set_ylabel("Score Sentiment NLP (/5)", fontsize=12)
    ax9.set_title("Corrélation note / sentiment — avis individuels", fontweight="bold", fontsize=13)
    ax9.set_xlim(0.5, 5.5)
    ax9.set_ylim(0.5, 5.5)
    ax9.set_xticks([1, 2, 3, 4, 5])
    ax9.set_yticks([1, 2, 3, 4, 5])
    ax9.legend(title="Cohérence", loc="upper left")

    # Corrélation de Pearson
    corr = df_scatter["note"].corr(df_scatter["sentiment_score"])
    ax9.text(0.98, 0.05, f"r = {corr:.2f}", transform=ax9.transAxes,
             ha="right", fontsize=11, color="#374151",
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#d1d5db"))

    plt.tight_layout()
    st.pyplot(fig9)
    plt.close()
else:
    st.info("ℹ️ Pas assez de données pour afficher le scatter avec les filtres actuels.")

st.markdown("---")

# ── Explorateur d'avis ────────────────────────────────────────────────────────
st.subheader("🎲 Explorateur d'avis — Note × Sentiment")
st.caption(
    "Sélectionne un contenu et une note pour voir un avis réel "
    "avec le score de sentiment que le modèle NLP lui a attribué."
)

col_sel1, col_sel2 = st.columns(2)

with col_sel1:
    contenus_dispo = sorted(df_full["content_name"].dropna().unique().tolist())
    contenu_choisi = st.selectbox("🎬 Contenu", contenus_dispo, key="explorer_contenu")

with col_sel2:
    notes_dispo = sorted(
        df_full[df_full["content_name"] == contenu_choisi]["note"]
        .dropna()
        .unique()
        .tolist()
    )
    note_choisie = st.selectbox(
        "⭐ Note Allociné",
        notes_dispo,
        format_func=lambda x: f"{x:.1f} / 5",
        key="explorer_note",
    )

# Filtrage des avis correspondants
df_explorer = df_full[
    (df_full["content_name"] == contenu_choisi) &
    (df_full["note"] == note_choisie)
].reset_index(drop=True)

if df_explorer.empty:
    st.info("ℹ️ Aucun avis trouvé pour cette combinaison.")
else:
    # Initialisation ou rerandomisation
    if st.button("🔀 Autre avis", key="btn_autre_avis"):
        st.session_state["explorer_idx"] = int(
            __import__("random").randint(0, len(df_explorer) - 1)
        )

    if "explorer_idx" not in st.session_state or \
       st.session_state["explorer_idx"] >= len(df_explorer):
        st.session_state["explorer_idx"] = 0

    avis = df_explorer.iloc[st.session_state["explorer_idx"]]

    # Code couleur de cohérence
    couleur_coh = COULEURS_COHERENCE.get(avis["coherence"], "#6B7280")
    emoji_coh   = {"coherent": "✅", "sur-estime": "⚠️", "sous-estime": "⬇️"}.get(
        avis["coherence"], "❓"
    )

    # Affichage de la carte avis
    with st.container():
        st.markdown(
            f"""
            <div style="
                border: 1.5px solid {couleur_coh};
                border-radius: 10px;
                padding: 20px 24px;
                background-color: #f9fafb;
                margin-top: 12px;
            ">
                <div style="font-size: 14px; color: #6B7280; margin-bottom: 8px;">
                    🎬 <strong>{avis['content_name']}</strong>
                    &nbsp;·&nbsp; {avis['content_type'].capitalize()}
                    &nbsp;·&nbsp; Note donnée : <strong>{avis['note']:.1f} / 5</strong>
                </div>
                <div style="
                    font-size: 16px;
                    color: #111827;
                    line-height: 1.6;
                    margin-bottom: 16px;
                    font-style: italic;
                ">
                    « {avis['texte']} »
                </div>
                <hr style="border: none; border-top: 1px solid #e5e7eb; margin: 12px 0;">
                <div style="display: flex; gap: 32px; font-size: 14px;">
                    <div>
                        🤖 <strong>Sentiment NLP</strong><br>
                        <span style="font-size: 18px; font-weight: bold; color: #2563EB;">
                            {avis['sentiment_label']}
                        </span>
                        &nbsp;({avis['sentiment_score']} / 5)
                    </div>
                    <div>
                        📊 <strong>Confiance</strong><br>
                        <span style="font-size: 18px; font-weight: bold; color: #7C3AED;">
                            {avis['sentiment_confidence']:.0%}
                        </span>
                    </div>
                    <div>
                        {emoji_coh} <strong>Cohérence</strong><br>
                        <span style="
                            font-size: 15px;
                            font-weight: bold;
                            color: {couleur_coh};
                        ">
                            {avis['coherence'].replace('-', ' ').capitalize()}
                        </span>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Explication de la cohérence
        diff = abs(avis["note"] - avis["sentiment_score"])
        if avis["coherence"] == "coherent":
            st.success(
                f"L'utilisateur note **{avis['note']:.1f}/5** et le modèle détecte "
                f"un sentiment de **{avis['sentiment_score']}/5** — "
                f"écart de {diff:.1f} point(s), cohérent ✅"
            )
        elif avis["coherence"] == "sur-estime":
            st.warning(
                f"L'utilisateur note **{avis['note']:.1f}/5** mais écrit comme quelqu'un "
                f"qui ressent **{avis['sentiment_score']}/5** — "
                f"il sur-estime sa note de {diff:.1f} point(s) ⚠️"
            )
        else:
            st.warning(
                f"L'utilisateur note **{avis['note']:.1f}/5** mais écrit comme quelqu'un "
                f"qui ressent **{avis['sentiment_score']}/5** — "
                f"il sous-estime sa note de {diff:.1f} point(s) ⬇️"
            )

        # Message discret si écart très fort — possible ironie non détectée
        if diff >= 3:
            st.caption(
                "ℹ️ Écart important — possible ironie ou sarcasme non détecté par le modèle."
            )

        st.caption(
            f"Avis {st.session_state['explorer_idx'] + 1} / {len(df_explorer)} "
            f"pour cette combinaison · Date : {avis.get('date_clean', 'N/A')}"
        )

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
ORDER BY note_moyenne DESC;"""

sql_input = st.text_area("Requête SQL", value=default_sql, height=180)

if st.button("▶️ Exécuter"):
    try:
        from google.cloud import bigquery
        client    = bigquery.Client(project=PROJECT_ID)
        result_df = client.query(sql_input).to_dataframe()
        st.dataframe(result_df, use_container_width=True)
    except Exception as e:
        st.error(f"Erreur BigQuery : {e}")