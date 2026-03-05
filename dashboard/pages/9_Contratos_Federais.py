import streamlit as st
import plotly.express as px
import polars as pl

from queries import (
    get_contratos_kpis,
    get_contratos_por_ano,
    get_contratos_por_orgao,
    get_top_fornecedores,
    get_fornecedor_emenda_crossref,
)

st.set_page_config(
    page_title="Contratos Federais",
    page_icon="📄",
    layout="wide",
)

st.title("📄 Contratos Federais")
st.caption(
    "Contratos de compras públicas do governo federal — Portal da Transparência "
    "(portaldatransparencia.gov.br). Dados de 2023–2025. "
    "Valores em BRL antes de parsing; capped em R$10B por contrato."
)

tab_geral, tab_ministerios, tab_fornecedores = st.tabs([
    "📊 Visão Geral",
    "🏛️ Por Ministério",
    "🏢 Fornecedores",
])


# ═══════════════════════════════════════════════════════════════════════════
# CACHED LOADERS
# ═══════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600)
def load_kpis(year):
    return get_contratos_kpis(year=year)

@st.cache_data(ttl=3600)
def load_trend():
    return get_contratos_por_ano()

@st.cache_data(ttl=3600)
def load_orgaos(year, n):
    return get_contratos_por_orgao(year=year, n=n)

@st.cache_data(ttl=3600)
def load_fornecedores(year, n):
    return get_top_fornecedores(year=year, n=n)

@st.cache_data(ttl=3600)
def load_crossref(year):
    return get_fornecedor_emenda_crossref(year=year)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 1 — VISÃO GERAL
# ═══════════════════════════════════════════════════════════════════════════

with tab_geral:
    year_all = st.selectbox(
        "Filtrar por ano",
        [None, 2023, 2024, 2025],
        format_func=lambda x: "Todos os anos" if x is None else str(x),
        key="geral_year",
    )

    kpis = load_kpis(year_all)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total de Contratos", f"{kpis['total_contratos']:,}")
    col2.metric("Valor Total Contratado", f"R$ {kpis['total_valor']/1e9:.1f}B")
    col3.metric("Fornecedores Distintos", f"{kpis['num_fornecedores']:,}")

    st.divider()
    st.subheader("Evolução Anual")

    trend = load_trend()
    if not trend.is_empty():
        fig = px.bar(
            trend.to_pandas(),
            x="ano_contrato",
            y="total_contratado",
            labels={"ano_contrato": "Ano", "total_contratado": "Total Contratado (R$)"},
            text_auto=".2s",
        )
        fig.update_layout(yaxis_tickformat=",.0f", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Top Ministérios (todos os anos)")
    top_orgaos = load_orgaos(year=None, n=10)
    if not top_orgaos.is_empty():
        fig2 = px.bar(
            top_orgaos.sort("total_contratado").to_pandas(),
            x="total_contratado",
            y="nome_orgao_superior",
            orientation="h",
            labels={"total_contratado": "Total Contratado (R$)", "nome_orgao_superior": "Ministério"},
            text_auto=".2s",
        )
        fig2.update_layout(yaxis_title="", showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2 — POR MINISTÉRIO
# ═══════════════════════════════════════════════════════════════════════════

with tab_ministerios:
    col_y, col_n = st.columns([1, 1])
    with col_y:
        year_min = st.selectbox(
            "Ano",
            [None, 2023, 2024, 2025],
            format_func=lambda x: "Todos" if x is None else str(x),
            key="min_year",
        )
    with col_n:
        top_n_min = st.slider("Top N ministérios", 5, 20, 15, key="min_n")

    orgaos_df = load_orgaos(year=year_min, n=top_n_min)

    if orgaos_df.is_empty():
        st.info("Nenhum dado disponível.")
    else:
        fig3 = px.bar(
            orgaos_df.sort("total_contratado").to_pandas(),
            x="total_contratado",
            y="nome_orgao_superior",
            orientation="h",
            color="num_contratos",
            labels={
                "total_contratado": "Total (R$)",
                "nome_orgao_superior": "",
                "num_contratos": "Nº Contratos",
            },
            text_auto=".2s",
        )
        fig3.update_layout(showlegend=True)
        st.plotly_chart(fig3, use_container_width=True)

        st.dataframe(
            orgaos_df.select([
                "nome_orgao_superior", "num_contratos",
                "num_fornecedores", "total_contratado", "media_valor_contrato",
            ]).rename({
                "nome_orgao_superior": "Ministério",
                "num_contratos": "Contratos",
                "num_fornecedores": "Fornecedores",
                "total_contratado": "Total (R$)",
                "media_valor_contrato": "Média (R$)",
            }).to_pandas(),
            use_container_width=True,
            hide_index=True,
        )


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3 — FORNECEDORES
# ═══════════════════════════════════════════════════════════════════════════

with tab_fornecedores:
    col_fy, col_fn = st.columns([1, 1])
    with col_fy:
        year_forn = st.selectbox(
            "Ano",
            [None, 2023, 2024, 2025],
            format_func=lambda x: "Todos" if x is None else str(x),
            key="forn_year",
        )
    with col_fn:
        top_n_forn = st.slider("Top N fornecedores", 10, 50, 25, key="forn_n")

    forn_df = load_fornecedores(year=year_forn, n=top_n_forn)

    st.subheader(f"Top {top_n_forn} Fornecedores por Valor Contratado")
    if not forn_df.is_empty():
        fig4 = px.bar(
            forn_df.sort("total_contratado").tail(20).to_pandas(),
            x="total_contratado",
            y="nome_contratado",
            orientation="h",
            color="tipo_contratado",
            labels={
                "total_contratado": "Total (R$)",
                "nome_contratado": "",
                "tipo_contratado": "Tipo",
            },
            text_auto=".2s",
        )
        fig4.update_layout(showlegend=True)
        st.plotly_chart(fig4, use_container_width=True)

        # PJ/CPF distribution
        tipo_counts = (
            forn_df.group_by("tipo_contratado")
            .agg(pl.len().alias("n"))
            .to_pandas()
        )
        fig5 = px.pie(tipo_counts, values="n", names="tipo_contratado",
                      title="Distribuição PJ/PF entre top fornecedores")
        st.plotly_chart(fig5, use_container_width=True)

    st.divider()
    st.subheader("🔗 Fornecedores que também receberam Emendas Parlamentares")
    st.caption(
        "Empresas (CNPJ) que aparecem TANTO em contratos federais QUANTO "
        "como beneficiárias de emendas parlamentares (TransfereGov)."
    )

    crossref_df = load_crossref(year=None)
    if crossref_df.is_empty():
        st.info("Nenhum cruzamento encontrado para os filtros selecionados.")
    else:
        st.metric("Empresas no cruzamento", len(crossref_df))
        st.dataframe(
            crossref_df.rename({
                "cnpj": "CNPJ",
                "nome_empresa": "Empresa",
                "total_contratos": "Total Contratos (R$)",
                "total_emendas": "Total Emendas (R$)",
                "num_contratos": "Nº Contratos",
                "num_emendas": "Nº Emendas",
            }).to_pandas(),
            use_container_width=True,
            hide_index=True,
        )
