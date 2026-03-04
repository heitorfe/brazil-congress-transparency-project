import streamlit as st
import plotly.express as px
import polars as pl

from queries import (
    get_ceaps_bulk_summary_by_year,
    get_ceaps_bulk_all_senators,
    get_ceaps_bulk_top_categories,
    get_ceaps_bulk_categories_by_year,
    get_ceaps_bulk_raw_receipts,
    get_ceap_camara_bulk_summary_by_year,
    get_ceap_camara_bulk_all_deputies,
    get_ceap_camara_bulk_top_categories,
    get_ceap_camara_bulk_categories_by_year,
)

st.set_page_config(
    page_title="Despesas — CEAPS & CEAP",
    page_icon="💰",
    layout="wide",
)

st.title("💰 Transparência de Despesas Parlamentares")
st.caption(
    "CEAPS (Senado, 2008–presente) e CEAP (Câmara, 2009–presente): reembolsos de despesas "
    "do exercício do mandato. Fonte: csvs bulk do portal de transparência."
)

tab_senado, tab_camara = st.tabs(["🏛️ Senado Federal — CEAPS", "🏢 Câmara dos Deputados — CEAP"])

# ═══════════════════════════════════════════════════════════════════════════
# TAB 1 — SENATE CEAPS (2008–present)
# ═══════════════════════════════════════════════════════════════════════════
with tab_senado:
    @st.cache_data(ttl=3600)
    def load_ceaps_summary():
        return get_ceaps_bulk_summary_by_year()

    @st.cache_data(ttl=3600)
    def load_ceaps_senators():
        return get_ceaps_bulk_all_senators()

    @st.cache_data(ttl=3600)
    def load_ceaps_top_cats():
        return get_ceaps_bulk_top_categories(n=12)

    @st.cache_data(ttl=3600)
    def load_ceaps_cats_by_year():
        return get_ceaps_bulk_categories_by_year()

    summary_s   = load_ceaps_summary()
    senators_s  = load_ceaps_senators()
    top_cat_s   = load_ceaps_top_cats()
    cat_year_s  = load_ceaps_cats_by_year()

    if summary_s.is_empty():
        st.info("Dados CEAPS ainda não carregados. Execute o backfill e o dbt.")
    else:
        ano_min_s = int(summary_s["ano"].min())
        ano_max_s = int(summary_s["ano"].max())

        total_s   = summary_s["total_gasto"].sum()
        recibos_s = int(summary_s["num_recibos"].sum())
        media_s   = total_s / summary_s["ano"].n_unique()

        k1, k2, k3, k4 = st.columns(4)
        k1.metric(
            f"Total reembolsado ({ano_min_s}–{ano_max_s})",
            f"R$ {total_s / 1_000_000:.1f} mi",
        )
        k2.metric("Média anual", f"R$ {media_s / 1_000_000:.1f} mi")
        k3.metric("Total de recibos", f"{recibos_s:,}".replace(",", "."))
        k4.metric("Anos cobertos", f"{ano_max_s - ano_min_s + 1}")

        st.divider()

        # ── Yearly evolution ─────────────────────────────────────────────
        st.subheader("Evolução anual do total reembolsado")
        fig_ts = px.bar(
            summary_s.with_columns(pl.col("ano").cast(pl.Utf8)).to_pandas(),
            x="ano",
            y="total_gasto",
            labels={"ano": "Ano", "total_gasto": ""},
            color_discrete_sequence=["#c0392b"],
        )
        fig_ts.update_traces(texttemplate="R$ %{y:,.0f}", textposition="outside")
        fig_ts.update_layout(
            height=320,
            margin=dict(t=30, b=10),
            yaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
        )
        st.plotly_chart(fig_ts, use_container_width=True)

        st.divider()

        # ── Category breakdown ────────────────────────────────────────────
        st.subheader("Despesas por categoria")
        cat_col1, cat_col2 = st.columns([2, 3])

        with cat_col1:
            st.caption(f"**Total acumulado ({ano_min_s}–{ano_max_s})**")
            fig_cat = px.bar(
                top_cat_s.sort("total_gasto").to_pandas(),
                x="total_gasto", y="tipo_despesa", orientation="h",
                labels={"total_gasto": "", "tipo_despesa": ""},
                color_discrete_sequence=["#e67e22"],
            )
            fig_cat.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
            fig_cat.update_layout(
                height=max(280, len(top_cat_s) * 32),
                margin=dict(t=10, b=10, r=180),
                xaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
            )
            st.plotly_chart(fig_cat, use_container_width=True)

        with cat_col2:
            st.caption("**Evolução por categoria e ano** (top 8 categorias)")
            top_8 = top_cat_s.head(8)["tipo_despesa"].to_list()
            trend_data = cat_year_s.filter(pl.col("tipo_despesa").is_in(top_8))
            fig_trend = px.bar(
                trend_data.with_columns(pl.col("ano").cast(pl.Utf8)).to_pandas(),
                x="ano", y="total_gasto", color="tipo_despesa", barmode="stack",
                labels={"ano": "Ano", "total_gasto": "", "tipo_despesa": "Categoria"},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig_trend.update_layout(
                height=max(280, len(top_cat_s) * 32),
                margin=dict(t=10, b=10),
                yaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
                legend=dict(orientation="h", yanchor="bottom", y=-0.4, x=0),
            )
            st.plotly_chart(fig_trend, use_container_width=True)

        st.divider()

        # ── Senator ranking ────────────────────────────────────────────────
        st.subheader(f"Ranking de senadores por gasto total ({ano_min_s}–{ano_max_s})")
        rank_col1, rank_col2 = st.columns([1, 3])

        with rank_col1:
            ordem_s = st.radio(
                "Ordenar por", ["🔴 Maiores gastos", "🟢 Menores gastos"],
                index=0, key="ceaps_rank_ordem"
            )
            n_exibir_s = st.slider(
                "Quantidade", min_value=5, max_value=40, value=15, step=5,
                key="ceaps_rank_n"
            )

        asc_s = (ordem_s == "🟢 Menores gastos")
        ranked_s = senators_s.sort("total_gasto", descending=not asc_s).head(n_exibir_s)

        with rank_col2:
            fig_rank_s = px.bar(
                ranked_s.sort("total_gasto", descending=asc_s).to_pandas(),
                x="total_gasto", y="nome_parlamentar", orientation="h",
                color="total_gasto",
                color_continuous_scale="RdYlGn_r" if not asc_s else "RdYlGn",
                labels={"total_gasto": "Total reembolsado (R$)", "nome_parlamentar": ""},
                hover_data=["partido_sigla", "estado_sigla", "num_recibos"],
            )
            fig_rank_s.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
            fig_rank_s.update_layout(
                coloraxis_showscale=False,
                height=max(320, n_exibir_s * 28),
                margin=dict(t=10, b=10, r=180),
                xaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
            )
            st.plotly_chart(fig_rank_s, use_container_width=True)

        with st.expander("Ver tabela completa de senadores"):
            st.dataframe(
                senators_s.rename({
                    "nome_parlamentar": "Nome", "partido_sigla": "Partido",
                    "estado_sigla": "UF", "total_gasto": "Total (R$)",
                    "num_recibos": "Nº Recibos",
                }).drop("senador_id"),
                use_container_width=True, hide_index=True,
            )

        st.divider()

        # ── Outlier detection ─────────────────────────────────────────────
        st.subheader("🔴 Recibos de Alto Valor — Alerta de Irregularidades")
        st.caption(
            "Recibos individuais cujo valor supera 3× a mediana da categoria no período selecionado."
        )

        anos_disponiveis_s = sorted(summary_s["ano"].to_list(), reverse=True)
        ano_outlier_s = st.selectbox(
            "Ano para análise", options=anos_disponiveis_s, index=0, key="ceaps_outlier_ano"
        )

        @st.cache_data(ttl=3600)
        def load_ceaps_raw(ano: int):
            return get_ceaps_bulk_raw_receipts(ano)

        raw_s = load_ceaps_raw(ano_outlier_s)

        if not raw_s.is_empty():
            medians_s = (
                raw_s.group_by("tipo_despesa")
                .agg(pl.col("valor_reembolsado").median().alias("mediana_cat"))
            )
            flagged_s = (
                raw_s.join(medians_s, on="tipo_despesa", how="left")
                .with_columns(
                    (pl.col("valor_reembolsado") / pl.col("mediana_cat")).alias("razao_mediana")
                )
                .filter(pl.col("razao_mediana") > 3.0)
                .sort("valor_reembolsado", descending=True)
            )

            if not flagged_s.is_empty():
                fig_out_s = px.strip(
                    flagged_s.to_pandas(),
                    x="tipo_despesa", y="valor_reembolsado",
                    color_discrete_sequence=["#e74c3c"],
                    hover_data=["nome_senador", "fornecedor", "cnpj_cpf", "data"],
                    labels={"tipo_despesa": "Categoria", "valor_reembolsado": "Valor (R$)"},
                    title=f"Recibos acima de 3× a mediana — {ano_outlier_s}",
                )
                fig_out_s.update_layout(
                    height=380, margin=dict(t=50, b=10),
                    xaxis_tickangle=-30,
                    yaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
                )
                st.plotly_chart(fig_out_s, use_container_width=True)

                display_s = flagged_s.head(100).select([
                    pl.col("nome_senador").alias("Senador"),
                    pl.col("data").alias("Data"),
                    pl.col("tipo_despesa").alias("Categoria"),
                    pl.col("fornecedor").alias("Fornecedor"),
                    pl.col("cnpj_cpf").alias("CNPJ/CPF"),
                    pl.col("valor_reembolsado").map_elements(
                        lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                        return_dtype=pl.Utf8,
                    ).alias("Valor"),
                    pl.col("razao_mediana").map_elements(
                        lambda v: f"{v:.1f}×", return_dtype=pl.Utf8,
                    ).alias("× mediana"),
                ])
                st.dataframe(display_s, use_container_width=True, hide_index=True)
            else:
                st.success(f"Nenhum recibo acima de 3× a mediana em {ano_outlier_s}.")
        else:
            st.info(f"Sem recibos disponíveis para {ano_outlier_s}.")

    with st.expander("📋 Como funcionam as despesas CEAPS do Senado"):
        st.markdown("""
### O que é a CEAPS?

A **CEAPS** (Cota para o Exercício da Atividade Parlamentar do Senado) é uma verba mensal
de reembolso de despesas dos senadores, destinada exclusivamente ao exercício do mandato.
**Não é salário** — é ressarcimento de gastos comprovados com nota fiscal.

| Categoria | O que cobre |
|---|---|
| **Locomoção / Hospedagem** | Deslocamentos, hotéis, alimentação e combustíveis em serviço |
| **Passagens aéreas** | Voos domésticos para o exercício do mandato |
| **Divulgação parlamentar** | Comunicação com eleitores (vedada nos 120 dias pré-eleição) |
| **Aluguel de escritório** | Escritório político de apoio no estado de origem |
| **Consultorias técnicas** | Serviços especializados de apoio ao mandato |

Fonte: [senado.leg.br/transparencia/LAI/verba](https://www.senado.leg.br/transparencia/LAI/verba)
""")

# ═══════════════════════════════════════════════════════════════════════════
# TAB 2 — CHAMBER CEAP (2009–present)
# ═══════════════════════════════════════════════════════════════════════════
with tab_camara:
    @st.cache_data(ttl=3600)
    def load_ceap_summary():
        return get_ceap_camara_bulk_summary_by_year()

    @st.cache_data(ttl=3600)
    def load_ceap_deputies():
        return get_ceap_camara_bulk_all_deputies()

    @st.cache_data(ttl=3600)
    def load_ceap_top_cats():
        return get_ceap_camara_bulk_top_categories(n=12)

    @st.cache_data(ttl=3600)
    def load_ceap_cats_by_year():
        return get_ceap_camara_bulk_categories_by_year()

    summary_c   = load_ceap_summary()
    deputies_c  = load_ceap_deputies()
    top_cat_c   = load_ceap_top_cats()
    cat_year_c  = load_ceap_cats_by_year()

    if summary_c.is_empty():
        st.info("Dados CEAP da Câmara ainda não carregados. Execute o backfill e o dbt.")
    else:
        ano_min_c = int(summary_c["ano"].min())
        ano_max_c = int(summary_c["ano"].max())

        total_c   = summary_c["total_gasto"].sum()
        recibos_c = int(summary_c["num_recibos"].sum())
        media_c   = total_c / summary_c["ano"].n_unique()

        k1, k2, k3, k4 = st.columns(4)
        k1.metric(
            f"Total reembolsado ({ano_min_c}–{ano_max_c})",
            f"R$ {total_c / 1_000_000_000:.2f} bi",
        )
        k2.metric("Média anual", f"R$ {media_c / 1_000_000:.0f} mi")
        k3.metric("Total de recibos", f"{recibos_c:,}".replace(",", "."))
        k4.metric("Anos cobertos", f"{ano_max_c - ano_min_c + 1}")

        st.divider()

        # ── Yearly evolution ─────────────────────────────────────────────
        st.subheader("Evolução anual do total reembolsado — Câmara")
        fig_ts_c = px.bar(
            summary_c.with_columns(pl.col("ano").cast(pl.Utf8)).to_pandas(),
            x="ano", y="total_gasto",
            labels={"ano": "Ano", "total_gasto": ""},
            color_discrete_sequence=["#2980b9"],
        )
        fig_ts_c.update_traces(texttemplate="R$ %{y:,.0f}", textposition="outside")
        fig_ts_c.update_layout(
            height=320, margin=dict(t=30, b=10),
            yaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
        )
        st.plotly_chart(fig_ts_c, use_container_width=True)

        st.divider()

        # ── Category breakdown ────────────────────────────────────────────
        st.subheader("Despesas por categoria — Câmara")
        cat_col1c, cat_col2c = st.columns([2, 3])

        with cat_col1c:
            st.caption(f"**Total acumulado ({ano_min_c}–{ano_max_c})**")
            fig_cat_c = px.bar(
                top_cat_c.sort("total_gasto").to_pandas(),
                x="total_gasto", y="tipo_despesa", orientation="h",
                labels={"total_gasto": "", "tipo_despesa": ""},
                color_discrete_sequence=["#2980b9"],
            )
            fig_cat_c.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
            fig_cat_c.update_layout(
                height=max(280, len(top_cat_c) * 32),
                margin=dict(t=10, b=10, r=180),
                xaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
            )
            st.plotly_chart(fig_cat_c, use_container_width=True)

        with cat_col2c:
            st.caption("**Evolução por categoria e ano** (top 8 categorias)")
            top_8_c = top_cat_c.head(8)["tipo_despesa"].to_list()
            trend_c = cat_year_c.filter(pl.col("tipo_despesa").is_in(top_8_c))
            fig_trend_c = px.bar(
                trend_c.with_columns(pl.col("ano").cast(pl.Utf8)).to_pandas(),
                x="ano", y="total_gasto", color="tipo_despesa", barmode="stack",
                labels={"ano": "Ano", "total_gasto": "", "tipo_despesa": "Categoria"},
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            fig_trend_c.update_layout(
                height=max(280, len(top_cat_c) * 32),
                margin=dict(t=10, b=10),
                yaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
                legend=dict(orientation="h", yanchor="bottom", y=-0.4, x=0),
            )
            st.plotly_chart(fig_trend_c, use_container_width=True)

        st.divider()

        # ── Deputy ranking ────────────────────────────────────────────────
        st.subheader(f"Ranking de deputados por gasto total ({ano_min_c}–{ano_max_c})")
        rank_col1c, rank_col2c = st.columns([1, 3])

        with rank_col1c:
            ordem_c = st.radio(
                "Ordenar por", ["🔴 Maiores gastos", "🟢 Menores gastos"],
                index=0, key="ceap_rank_ordem"
            )
            n_exibir_c = st.slider(
                "Quantidade", min_value=5, max_value=40, value=15, step=5,
                key="ceap_rank_n"
            )

        asc_c = (ordem_c == "🟢 Menores gastos")
        ranked_c = deputies_c.sort("total_gasto", descending=not asc_c).head(n_exibir_c)

        with rank_col2c:
            fig_rank_c = px.bar(
                ranked_c.sort("total_gasto", descending=asc_c).to_pandas(),
                x="total_gasto", y="nome_parlamentar", orientation="h",
                color="total_gasto",
                color_continuous_scale="Blues_r" if not asc_c else "Blues",
                labels={"total_gasto": "Total reembolsado (R$)", "nome_parlamentar": ""},
                hover_data=["partido_sigla", "estado_sigla", "num_recibos"],
            )
            fig_rank_c.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
            fig_rank_c.update_layout(
                coloraxis_showscale=False,
                height=max(320, n_exibir_c * 28),
                margin=dict(t=10, b=10, r=180),
                xaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
            )
            st.plotly_chart(fig_rank_c, use_container_width=True)

        with st.expander("Ver tabela completa de deputados"):
            st.dataframe(
                deputies_c.rename({
                    "nome_parlamentar": "Nome", "partido_sigla": "Partido",
                    "estado_sigla": "UF", "total_gasto": "Total (R$)",
                    "num_recibos": "Nº Recibos",
                }).drop("deputado_id"),
                use_container_width=True, hide_index=True,
            )

    with st.expander("📋 Como funciona a CEAP da Câmara"):
        st.markdown("""
### O que é a CEAP?

A **CEAP** (Cota para o Exercício da Atividade Parlamentar) é a verba mensal de reembolso
de despesas dos deputados federais. Funciona como a CEAPS do Senado — não é salário,
é ressarcimento de gastos comprovados com nota fiscal.

O valor mensal varia por partido e estado de origem do deputado.

Fonte: [camara.leg.br/cotas](https://www.camara.leg.br/cotas)
""")

st.caption(
    "Fontes: senado.leg.br/transparencia/LAI/verba | camara.leg.br/cotas — "
    "Dados históricos 2008/2009–presente via CSV bulk."
)
