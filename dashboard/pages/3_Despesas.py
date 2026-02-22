import streamlit as st
import plotly.express as px
import polars as pl

from queries import (
    get_ceaps_summary_by_year,
    get_ceaps_top_spenders,
    get_ceaps_by_year_and_category,
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
def load_top_spenders():
    return get_ceaps_top_spenders(n=20)

summary_df = load_summary()
top_df     = load_top_spenders()

# ── Global KPIs ───────────────────────────────────────────────────────────
total_gasto   = summary_df["total_gasto"].sum()
total_recibos = int(summary_df["num_recibos"].sum())
anos_cobertos = summary_df["ano"].n_unique()

k1, k2, k3 = st.columns(3)
k1.metric(
    "Total reembolsado (todos os anos)",
    f"R$ {total_gasto:,.0f}".replace(",", "."),
)
k2.metric("Total de recibos processados", f"{total_recibos:,}".replace(",", "."))
k3.metric("Anos cobertos", f"{anos_cobertos} (2019–2026)")

st.divider()

# ── Time series: spending by year ─────────────────────────────────────────
st.subheader("Evolução anual do total reembolsado")
fig_ts = px.bar(
    summary_df.to_pandas(),
    x="ano",
    y="total_gasto",
    color="total_gasto",
    color_continuous_scale="Reds",
    labels={"ano": "Ano", "total_gasto": "Total reembolsado (R$)"},
    text_auto=".3s",
)
fig_ts.update_layout(
    coloraxis_showscale=False,
    height=320,
    margin=dict(t=10, b=10),
    xaxis=dict(type="category"),
)
fig_ts.update_traces(textposition="outside")
st.plotly_chart(fig_ts, use_container_width=True)

st.divider()

# ── Per-year breakdown ─────────────────────────────────────────────────────
st.subheader("Detalhamento por categoria")
anos_disponiveis = sorted(summary_df["ano"].to_list(), reverse=True)
sel_ano = st.selectbox("Selecione o ano", anos_disponiveis)

@st.cache_data(ttl=3600)
def load_categories(ano):
    return get_ceaps_by_year_and_category(ano)

cat_df = load_categories(sel_ano)

if cat_df.is_empty():
    st.info(f"Nenhuma despesa registrada para o ano {sel_ano}.")
else:
    total_ano = cat_df["total_gasto"].sum()
    st.caption(f"Total em {sel_ano}: **R$ {total_ano:,.0f}**".replace(",", "."))

    fig_cat = px.bar(
        cat_df.to_pandas(),
        x="total_gasto",
        y="tipo_despesa",
        orientation="h",
        color="total_gasto",
        color_continuous_scale="Oranges",
        labels={"total_gasto": "Total (R$)", "tipo_despesa": ""},
        text_auto=".3s",
    )
    fig_cat.update_layout(
        coloraxis_showscale=False,
        height=400,
        margin=dict(t=10, b=10),
        yaxis=dict(autorange="reversed"),
    )
    fig_cat.update_traces(textposition="inside")
    st.plotly_chart(fig_cat, use_container_width=True)

st.divider()

# ── Top spenders leaderboard ───────────────────────────────────────────────
st.subheader("🏆 Maiores gastos — todos os anos")
st.caption("Senadores com maior total reembolsado pela CEAPS desde 2019.")

fig_top = px.bar(
    top_df.sort("total_gasto").to_pandas(),
    x="total_gasto",
    y="nome_parlamentar",
    orientation="h",
    color="total_gasto",
    color_continuous_scale="RdYlGn_r",
    labels={"total_gasto": "Total reembolsado (R$)", "nome_parlamentar": ""},
    text_auto=".3s",
    hover_data=["partido_sigla", "estado_sigla", "num_recibos"],
)
fig_top.update_layout(
    coloraxis_showscale=False,
    height=520,
    margin=dict(t=10, b=10),
)
fig_top.update_traces(textposition="inside")
st.plotly_chart(fig_top, use_container_width=True)

# Detailed table
with st.expander("Ver tabela detalhada"):
    st.dataframe(
        top_df.rename({
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
