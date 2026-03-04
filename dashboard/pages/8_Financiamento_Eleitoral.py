import streamlit as st
import plotly.express as px
import polars as pl

from queries import (
    get_tse_donation_summary_by_year,
    get_tse_top_candidates,
    get_tse_top_donors,
    get_tse_senators_with_donations,
    get_tse_donation_origin_breakdown,
)

st.set_page_config(
    page_title="Financiamento Eleitoral — TSE",
    page_icon="🗳️",
    layout="wide",
)

st.title("🗳️ Financiamento de Campanhas Eleitorais")
st.caption(
    "Receitas declaradas ao TSE (Tribunal Superior Eleitoral) — prestação de contas "
    "de candidatos. Anos federais: 2018, 2022. Municipal: 2024. "
    "Fonte: cdn.tse.jus.br (prestação_de_contas_eleitorais_candidatos)."
)

tab_geral, tab_candidatos, tab_doadores, tab_cruzamento = st.tabs([
    "📊 Visão Geral",
    "🏆 Candidatos",
    "💼 Doadores",
    "🔗 Cruzamento Senado/Câmara",
])


# ═══════════════════════════════════════════════════════════════════════════
# CACHED LOADERS
# ═══════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600)
def load_summary():
    return get_tse_donation_summary_by_year()

@st.cache_data(ttl=3600)
def load_top_candidates(year, cargo, n=30):
    return get_tse_top_candidates(year=year, cargo=cargo, n=n)

@st.cache_data(ttl=3600)
def load_top_donors(year, n=25):
    return get_tse_top_donors(year=year, n=n)

@st.cache_data(ttl=3600)
def load_senators():
    return get_tse_senators_with_donations()

@st.cache_data(ttl=3600)
def load_origin(year):
    return get_tse_donation_origin_breakdown(year=year)


summary = load_summary()

if summary.is_empty():
    st.info(
        "Dados TSE ainda não extraídos. Execute:\n\n"
        "```bash\n"
        "PYTHONPATH=src/extraction .venv/Scripts/python.exe src/extraction/extract_tse.py\n"
        "cd dbt_project && ./../.venv/Scripts/dbt.exe run --profiles-dir . -s tag:tse\n"
        "```"
    )
    st.stop()

available_years = sorted(summary["ano"].to_list())
election_labels = {
    2018: "2018 (Federal)",
    2022: "2022 (Federal)",
    2024: "2024 (Municipal)",
}


# ═══════════════════════════════════════════════════════════════════════════
# TAB 1 — VISÃO GERAL
# ═══════════════════════════════════════════════════════════════════════════
with tab_geral:
    total_all = float(summary["total_arrecadado"].sum())
    total_doacoes = int(summary["num_doacoes"].sum())
    total_candidatos = int(summary["num_candidatos"].sum())

    col1, col2, col3 = st.columns(3)
    col1.metric("Total arrecadado (todos os anos)", f"R$ {total_all/1e9:.2f} bi")
    col2.metric("Total de doações", f"{total_doacoes:,}")
    col3.metric("Candidatos com receitas", f"{total_candidatos:,}")

    st.subheader("Arrecadação total por ano eleitoral")
    fig_year = px.bar(
        summary.to_pandas(),
        x="ano",
        y="total_arrecadado",
        text_auto=".2s",
        color_discrete_sequence=["#1f77b4"],
        labels={"ano": "Ano", "total_arrecadado": "Total arrecadado (R$)"},
    )
    fig_year.update_traces(textposition="outside")
    fig_year.update_layout(
        xaxis=dict(tickmode="array", tickvals=available_years),
        yaxis_tickformat=",.0f",
        plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig_year, use_container_width=True)

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Doações por ano**")
        st.dataframe(
            summary.rename({
                "ano": "Ano",
                "total_arrecadado": "Total (R$)",
                "num_candidatos": "Candidatos",
                "num_doacoes": "Doações",
            }).to_pandas().style.format({"Total (R$)": "R$ {:,.2f}"}),
            use_container_width=True,
            hide_index=True,
        )

    with col_b:
        st.markdown("**Notas metodológicas**")
        st.info(
            "• Os dados incluem apenas receitas declaradas ao TSE (doações formais).  \n"
            "• 2018 e 2022 = eleições federais (Senadores e Deputados Federais).  \n"
            "• 2024 = eleições municipais (Prefeitos e Vereadores).  \n"
            "• Pessoas jurídicas (CNPJ) não podem mais doar diretamente a candidatos "
            "desde a ADI 4650/2015 — mas doações a partidos e fundos ainda aparecem.  \n"
            "• Ligação com Senadores/Deputados feita por nome parlamentar."
        )


# ═══════════════════════════════════════════════════════════════════════════
# TAB 2 — CANDIDATOS
# ═══════════════════════════════════════════════════════════════════════════
with tab_candidatos:
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        year_sel = st.selectbox(
            "Ano eleitoral",
            options=[None] + available_years,
            format_func=lambda y: "Todos os anos" if y is None else election_labels.get(y, str(y)),
            key="cand_year",
        )
    with col_f2:
        cargo_opts = [None, "SENADOR", "DEPUTADO FEDERAL", "GOVERNADOR",
                      "PRESIDENTE", "DEPUTADO ESTADUAL", "PREFEITO"]
        cargo_sel = st.selectbox(
            "Cargo",
            options=cargo_opts,
            format_func=lambda c: "Todos os cargos" if c is None else c,
            key="cand_cargo",
        )
    with col_f3:
        n_sel = st.slider("Top N candidatos", min_value=5, max_value=50, value=20, key="cand_n")

    df_cand = load_top_candidates(year=year_sel, cargo=cargo_sel, n=n_sel)

    if df_cand.is_empty():
        st.info("Nenhum candidato encontrado com esses filtros.")
    else:
        # Horizontal bar chart
        fig_cand = px.bar(
            df_cand.to_pandas().sort_values("total_arrecadado"),
            x="total_arrecadado",
            y="nome_candidato",
            orientation="h",
            color="partido_sigla",
            hover_data=["cargo", "uf", "eleito", "num_doacoes"],
            text_auto=".2s",
            labels={
                "total_arrecadado": "Total arrecadado (R$)",
                "nome_candidato": "Candidato",
                "partido_sigla": "Partido",
            },
        )
        fig_cand.update_layout(
            height=max(400, n_sel * 22),
            yaxis=dict(autorange="reversed"),
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_cand, use_container_width=True)

        st.markdown("**Tabela detalhada**")
        st.dataframe(
            df_cand.select([
                "nome_candidato", "cargo", "uf", "partido_sigla",
                "eleito", "total_arrecadado", "num_doacoes", "num_doadores",
            ]).rename({
                "nome_candidato": "Candidato",
                "cargo": "Cargo",
                "uf": "UF",
                "partido_sigla": "Partido",
                "eleito": "Eleito",
                "total_arrecadado": "Total (R$)",
                "num_doacoes": "Doações",
                "num_doadores": "Doadores",
            }).to_pandas().style.format({"Total (R$)": "R$ {:,.2f}"}),
            use_container_width=True,
            hide_index=True,
        )


# ═══════════════════════════════════════════════════════════════════════════
# TAB 3 — DOADORES
# ═══════════════════════════════════════════════════════════════════════════
with tab_doadores:
    col_d1, col_d2 = st.columns([1, 2])
    with col_d1:
        year_don = st.selectbox(
            "Ano eleitoral",
            options=[None] + available_years,
            format_func=lambda y: "Todos os anos" if y is None else election_labels.get(y, str(y)),
            key="don_year",
        )
        n_don = st.slider("Top N doadores", min_value=5, max_value=50, value=20, key="don_n")

    df_don = load_top_donors(year=year_don, n=n_don)
    df_origin = load_origin(year=year_don)

    if df_don.is_empty():
        st.info("Nenhuma doação encontrada.")
    else:
        with col_d2:
            # PJ vs PF pie chart from origin data
            if not df_origin.is_empty():
                pj_pf = (
                    df_origin.group_by("tipo_doador")
                    .agg(pl.col("total_doado").sum())
                    .sort("total_doado", descending=True)
                )
                fig_pie = px.pie(
                    pj_pf.to_pandas(),
                    values="total_doado",
                    names="tipo_doador",
                    title="Distribuição: Pessoa Jurídica vs Física",
                    color_discrete_map={
                        "CNPJ": "#e15759",
                        "CPF":  "#4e79a7",
                        "unknown": "#bab0ab",
                    },
                )
                st.plotly_chart(fig_pie, use_container_width=True)

        st.markdown(f"**Top {n_don} doadores por valor total doado**")
        st.dataframe(
            df_don.rename({
                "nome_doador": "Doador",
                "tipo_doador": "Tipo",
                "cnae_descricao": "Setor (CNAE)",
                "total_doado": "Total doado (R$)",
                "num_candidatos": "Candidatos",
                "num_doacoes": "Doações",
            }).drop("cpf_cnpj_doador_raw")
            .to_pandas()
            .style.format({"Total doado (R$)": "R$ {:,.2f}"}),
            use_container_width=True,
            hide_index=True,
        )

        # Top 10 sectors (CNAE) for PJ donors
        pj_donors = df_don.filter(pl.col("tipo_doador") == "CNPJ")
        if not pj_donors.is_empty() and "cnae_descricao" in pj_donors.columns:
            top_cnae = (
                pj_donors.filter(pl.col("cnae_descricao").is_not_null())
                .group_by("cnae_descricao")
                .agg(pl.col("total_doado").sum())
                .sort("total_doado", descending=True)
                .head(10)
            )
            if not top_cnae.is_empty():
                st.markdown("**Top setores (CNAE) — doadores pessoa jurídica**")
                fig_cnae = px.bar(
                    top_cnae.to_pandas().sort_values("total_doado"),
                    x="total_doado",
                    y="cnae_descricao",
                    orientation="h",
                    text_auto=".2s",
                    labels={"total_doado": "Total doado (R$)", "cnae_descricao": "Setor"},
                )
                fig_cnae.update_layout(
                    height=350,
                    plot_bgcolor="rgba(0,0,0,0)",
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig_cnae, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════
# TAB 4 — CRUZAMENTO SENADO / CÂMARA
# ═══════════════════════════════════════════════════════════════════════════
with tab_cruzamento:
    st.markdown(
        "Senadores atuais cujo nome parlamentar foi encontrado em registros de candidatura TSE. "
        "A ligação é feita por **nome parlamentar normalizado** — não por CPF."
    )

    df_sen = load_senators()

    if df_sen.is_empty():
        st.info(
            "Nenhum senador encontrado nos registros TSE. "
            "Isso pode indicar que o extrator ainda não foi executado para os anos federais (2018/2022)."
        )
    else:
        years_found = sorted(df_sen["ano"].unique().to_list())
        year_cross = st.selectbox(
            "Ano eleitoral",
            options=[None] + years_found,
            format_func=lambda y: "Todos os anos" if y is None else election_labels.get(y, str(y)),
            key="cross_year",
        )
        df_cross = df_sen if year_cross is None else df_sen.filter(pl.col("ano") == year_cross)

        col_x1, col_x2, col_x3 = st.columns(3)
        col_x1.metric("Senadores com registro TSE", df_cross["senador_id"].n_unique())
        col_x2.metric("Total arrecadado", f"R$ {float(df_cross['total_arrecadado'].sum()):,.0f}")
        col_x3.metric("Total de doações", f"{int(df_cross['num_doacoes'].sum()):,}")

        # Bar chart: top senators by amount raised
        top_sen = (
            df_cross.group_by(["senador_id", "nome_parlamentar", "partido_sigla", "estado_sigla"])
            .agg([
                pl.col("total_arrecadado").sum(),
                pl.col("num_doacoes").sum(),
            ])
            .sort("total_arrecadado", descending=True)
            .head(30)
        )

        fig_sen = px.bar(
            top_sen.to_pandas().sort_values("total_arrecadado"),
            x="total_arrecadado",
            y="nome_parlamentar",
            orientation="h",
            color="partido_sigla",
            hover_data=["estado_sigla", "num_doacoes"],
            text_auto=".2s",
            labels={
                "total_arrecadado": "Total arrecadado (R$)",
                "nome_parlamentar": "Senador",
                "partido_sigla": "Partido",
            },
            title="Senadores atuais — arrecadação em campanhas eleitorais",
        )
        fig_sen.update_layout(
            height=max(400, len(top_sen) * 22),
            yaxis=dict(autorange="reversed"),
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_sen, use_container_width=True)

        st.markdown("**Histórico por senador e ano**")
        st.dataframe(
            df_cross.rename({
                "nome_parlamentar": "Senador",
                "partido_sigla": "Partido",
                "estado_sigla": "Estado",
                "ano": "Ano",
                "total_arrecadado": "Total arrecadado (R$)",
                "num_doacoes": "Doações",
                "eleito": "Eleito (TSE)",
            }).drop("senador_id")
            .sort(["Senador", "Ano"])
            .to_pandas()
            .style.format({"Total arrecadado (R$)": "R$ {:,.2f}"}),
            use_container_width=True,
            hide_index=True,
        )
