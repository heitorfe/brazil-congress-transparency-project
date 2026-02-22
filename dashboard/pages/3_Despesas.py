import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from queries import (
    get_ceaps_summary_by_year,
    get_ceaps_all_senators_totals,
    get_ceaps_top_categories,
    get_ceaps_categories_by_year,
)

st.set_page_config(
    page_title="Despesas — CEAPS",
    page_icon="💰",
    layout="wide",
)

st.title("💰 Transparência de Despesas — CEAPS")
st.caption(
    "Cota para o Exercício da Atividade Parlamentar (CEAPS): reembolsos de despesas "
    "do exercício do mandato. Dados do ADM — Sistema de Dados Abertos do Senado."
)

# ── Load data ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_summary():
    return get_ceaps_summary_by_year()

@st.cache_data(ttl=3600)
def load_all_senators():
    return get_ceaps_all_senators_totals()

@st.cache_data(ttl=3600)
def load_top_categories():
    return get_ceaps_top_categories(n=12)

@st.cache_data(ttl=3600)
def load_categories_by_year():
    return get_ceaps_categories_by_year()

summary_df   = load_summary()
senators_df  = load_all_senators()
top_cat_df   = load_top_categories()
cat_year_df  = load_categories_by_year()

# ── Global KPIs ───────────────────────────────────────────────────────────
total_gasto   = summary_df["total_gasto"].sum()
total_recibos = int(summary_df["num_recibos"].sum())
media_anual   = total_gasto / summary_df["ano"].n_unique() if summary_df["ano"].n_unique() > 0 else 0
n_senadores   = senators_df["senador_id"].n_unique()

k1, k2, k3, k4 = st.columns(4)
k1.metric(
    "Total reembolsado (2019–2026)",
    f"R$ {total_gasto / 1_000_000:.1f} mi",
    help="Soma de todos os reembolsos CEAPS registrados",
)
k2.metric(
    "Média anual do Senado",
    f"R$ {media_anual / 1_000_000:.1f} mi",
)
k3.metric("Total de recibos", f"{total_recibos:,}".replace(",", "."))
k4.metric("Senadores com registros", n_senadores)

st.divider()

# ── Yearly evolution ───────────────────────────────────────────────────────
st.subheader("Evolução anual do total reembolsado")
fig_ts = px.bar(
    summary_df.with_columns(pl.col("ano").cast(pl.Utf8)).to_pandas(),
    x="ano",
    y="total_gasto",
    labels={"ano": "Ano", "total_gasto": ""},
    color_discrete_sequence=["#c0392b"],
)
fig_ts.update_traces(
    texttemplate="R$ %{y:,.0f}",
    textposition="outside",
)
fig_ts.update_layout(
    height=320,
    margin=dict(t=30, b=10),
    yaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
)
st.plotly_chart(fig_ts, use_container_width=True)

st.divider()

# ── Category breakdown — no year selection needed ─────────────────────────
st.subheader("Despesas por categoria")

cat_col1, cat_col2 = st.columns([2, 3])

with cat_col1:
    st.caption("**Total acumulado (2019–2026)**")
    fig_cat_total = px.bar(
        top_cat_df.sort("total_gasto").to_pandas(),
        x="total_gasto",
        y="tipo_despesa",
        orientation="h",
        labels={"total_gasto": "", "tipo_despesa": ""},
        color_discrete_sequence=["#e67e22"],
    )
    fig_cat_total.update_traces(
        texttemplate="R$ %{x:,.0f}",
        textposition="outside",
    )
    fig_cat_total.update_layout(
        height=max(280, len(top_cat_df) * 32),
        margin=dict(t=10, b=10, r=180),
        xaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
    )
    st.plotly_chart(fig_cat_total, use_container_width=True)

with cat_col2:
    st.caption("**Evolução por categoria e ano** (top 8 categorias)")
    top_8_cats = top_cat_df.head(8)["tipo_despesa"].to_list()
    cat_trend = cat_year_df.filter(pl.col("tipo_despesa").is_in(top_8_cats))

    fig_trend = px.bar(
        cat_trend.with_columns(pl.col("ano").cast(pl.Utf8)).to_pandas(),
        x="ano",
        y="total_gasto",
        color="tipo_despesa",
        barmode="stack",
        labels={"ano": "Ano", "total_gasto": "", "tipo_despesa": "Categoria"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_trend.update_layout(
        height=max(280, len(top_cat_df) * 32),
        margin=dict(t=10, b=10),
        yaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
        legend=dict(orientation="h", yanchor="bottom", y=-0.4, x=0),
    )
    st.plotly_chart(fig_trend, use_container_width=True)

st.divider()

# ── Senators ranking ───────────────────────────────────────────────────────
st.subheader("Ranking de senadores por gasto total (2019–2026)")

rank_col1, rank_col2 = st.columns([1, 3])

with rank_col1:
    ordem = st.radio(
        "Ordenar por",
        ["🔴 Maiores gastos", "🟢 Menores gastos"],
        index=0,
    )
    n_exibir = st.slider("Quantidade de senadores", min_value=5, max_value=40, value=15, step=5)

ascending = (ordem == "🟢 Menores gastos")
ranked = senators_df.sort("total_gasto", descending=not ascending).head(n_exibir)

with rank_col2:
    color_scale = "RdYlGn_r" if not ascending else "RdYlGn"
    fig_rank = px.bar(
        ranked.sort("total_gasto", descending=ascending).to_pandas(),
        x="total_gasto",
        y="nome_parlamentar",
        orientation="h",
        color="total_gasto",
        color_continuous_scale=color_scale,
        labels={"total_gasto": "Total reembolsado (R$)", "nome_parlamentar": ""},
        hover_data=["partido_sigla", "estado_sigla", "num_recibos"],
    )
    fig_rank.update_traces(
        texttemplate="R$ %{x:,.0f}",
        textposition="outside",
    )
    fig_rank.update_layout(
        coloraxis_showscale=False,
        height=max(320, n_exibir * 28),
        margin=dict(t=10, b=10, r=180),
        xaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
    )
    st.plotly_chart(fig_rank, use_container_width=True)

# ── Detailed table ─────────────────────────────────────────────────────────
with st.expander("Ver tabela completa"):
    st.dataframe(
        senators_df.rename({
            "nome_parlamentar": "Nome",
            "partido_sigla":    "Partido",
            "estado_sigla":     "UF",
            "total_gasto":      "Total (R$)",
            "num_recibos":      "Nº Recibos",
        }).drop("senador_id"),
        use_container_width=True,
        hide_index=True,
    )

st.divider()
st.caption(
    "Fonte: ADM — Sistema de Dados Abertos do Senado Federal — "
    "adm.senado.gov.br/adm-dadosabertos"
)
