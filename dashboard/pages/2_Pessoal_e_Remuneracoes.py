import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from queries import (
    get_pessoal_kpis,
    get_remuneracao_trend,
    get_servidores_por_vinculo,
    get_top_remuneracoes,
    get_remuneracao_componentes,
    get_lotacoes_top,
    get_pensionistas_trend,
    get_top_pensionistas,
    get_horas_extras_trend,
    get_horas_extras_por_lotacao,
    get_remuneracoes_anos_disponiveis,
    get_remuneracoes_meses_disponiveis,
)

st.set_page_config(
    page_title="Pessoal e Remunerações",
    page_icon="💼",
    layout="wide",
)

MESES_PT = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}

VINCULO_LABELS = {
    "EFETIVO": "Efetivo",
    "COMISSIONADO": "Comissionado",
    "REQUISITADO": "Requisitado",
    "EXERCICIO_PROVISORIO": "Exercício Provisório",
    "PARLAMENTAR": "Parlamentar",
    "NÃO INFORMADO": "Não informado",
}

VINCULO_COLORS = {
    "EFETIVO": "#1f6cb0",
    "COMISSIONADO": "#e07b00",
    "REQUISITADO": "#6a9f3e",
    "EXERCICIO_PROVISORIO": "#8e44ad",
    "PARLAMENTAR": "#c0392b",
    "NÃO INFORMADO": "#aaa",
}


# ── Cached loaders ────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_kpis():
    return get_pessoal_kpis()

@st.cache_data(ttl=3600)
def load_trend():
    return get_remuneracao_trend()

@st.cache_data(ttl=3600)
def load_pensionistas_trend():
    return get_pensionistas_trend()

@st.cache_data(ttl=3600)
def load_horas_trend():
    return get_horas_extras_trend()

@st.cache_data(ttl=3600)
def load_anos():
    return get_remuneracoes_anos_disponiveis()

@st.cache_data(ttl=3600)
def load_meses(ano: int):
    return get_remuneracoes_meses_disponiveis(ano)

@st.cache_data(ttl=3600)
def load_vinculo(ano: int, mes: int):
    return get_servidores_por_vinculo(ano, mes)

@st.cache_data(ttl=3600)
def load_top_rem(ano: int, mes: int, n: int):
    return get_top_remuneracoes(ano, mes, n)

@st.cache_data(ttl=3600)
def load_componentes(ano: int, mes: int):
    return get_remuneracao_componentes(ano, mes)

@st.cache_data(ttl=3600)
def load_lotacoes(ano: int, mes: int, n: int):
    return get_lotacoes_top(ano, mes, n)

@st.cache_data(ttl=3600)
def load_top_pensionistas(ano: int, mes: int, n: int):
    return get_top_pensionistas(ano, mes, n)

@st.cache_data(ttl=3600)
def load_horas_lotacao(ano: int, mes: int, n: int):
    return get_horas_extras_por_lotacao(ano, mes, n)


# ── Page header ───────────────────────────────────────────────────────────

st.title("💼 Pessoal e Remunerações do Senado Federal")
st.caption(
    "Gastos com servidores, pensionistas e horas extras — "
    "fonte: API Administrativa do Senado (ADM), 2019–presente"
)

# ── Month/year filter (sidebar) ───────────────────────────────────────────

anos = load_anos()
if not anos:
    st.error("Dados de remunerações não disponíveis. Execute `extract_servidores.py` primeiro.")
    st.stop()

ano_sel = st.sidebar.selectbox("Ano de referência", anos, index=0)
meses = load_meses(ano_sel)
mes_sel = st.sidebar.selectbox(
    "Mês de referência",
    meses,
    format_func=lambda m: MESES_PT.get(m, str(m)),
    index=0,
)

st.sidebar.divider()
st.sidebar.caption(f"Mês selecionado: **{MESES_PT.get(mes_sel, mes_sel)}/{ano_sel}**")

# ── KPI header ────────────────────────────────────────────────────────────

kpis = load_kpis()

c1, c2, c3, c4 = st.columns(4)
c1.metric(
    "Servidores ativos",
    f"{kpis['num_servidores_ativos']:,}".replace(",", "."),
)
c2.metric(
    "Pensionistas",
    f"{kpis['num_pensionistas']:,}".replace(",", "."),
)
c3.metric(
    f"Folha líquida ({MESES_PT.get(kpis['mes_ref'], '')} {kpis['ano_ref']})",
    f"R$ {kpis['total_liquido_mes']:,.0f}".replace(",", "."),
)
c4.metric(
    f"Horas extras ({MESES_PT.get(kpis['mes_ref'], '')} {kpis['ano_ref']})",
    f"R$ {kpis['total_horas_extras_mes']:,.0f}".replace(",", "."),
)

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────

tab_geral, tab_rem, tab_pen, tab_horas = st.tabs(
    ["📊 Visão Geral", "👥 Remunerações", "🧓 Pensionistas", "⏱️ Horas Extras"]
)


# ══ TAB 1 — Visão Geral ══════════════════════════════════════════════════

with tab_geral:
    st.subheader("Evolução da folha de pagamento (2019–presente)")

    trend = load_trend()
    pensionistas_trend = load_pensionistas_trend()

    if not trend.is_empty():
        # Build date labels for the x-axis
        trend = trend.with_columns(
            pl.concat_str([
                pl.col("ano").cast(pl.Utf8),
                pl.lit("-"),
                pl.col("mes").cast(pl.Utf8).str.zfill(2),
            ]).alias("periodo")
        )

        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=trend["periodo"].to_list(),
            y=trend["total_liquido_servidores"].to_list(),
            mode="lines",
            name="Servidores — Líquido",
            line=dict(color="#1f6cb0", width=2),
        ))
        if "total_liquido_pensionistas" in trend.columns:
            fig_trend.add_trace(go.Scatter(
                x=trend["periodo"].to_list(),
                y=trend["total_liquido_pensionistas"].fill_null(0).to_list(),
                mode="lines",
                name="Pensionistas — Líquido",
                line=dict(color="#e07b00", width=2, dash="dot"),
            ))
        fig_trend.update_layout(
            xaxis_title="Mês",
            yaxis_title="Total líquido (R$)",
            yaxis_tickformat=",.0f",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=380,
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("Dados de tendência não disponíveis.")

    st.subheader(f"Composição por vínculo — {MESES_PT.get(mes_sel)} / {ano_sel}")

    vinculo_df = load_vinculo(ano_sel, mes_sel)
    if not vinculo_df.is_empty():
        vinculo_df = vinculo_df.with_columns(
            pl.col("vinculo").replace(VINCULO_LABELS).alias("vinculo_label")
        )

        col_donut, col_table = st.columns([1, 1])
        with col_donut:
            fig_v = px.pie(
                vinculo_df.to_pandas(),
                names="vinculo_label",
                values="num_servidores",
                hole=0.45,
                color="vinculo",
                color_discrete_map={VINCULO_LABELS.get(k, k): v for k, v in VINCULO_COLORS.items()},
                title="Servidores por vínculo",
            )
            fig_v.update_traces(textposition="inside", textinfo="percent+label")
            fig_v.update_layout(showlegend=False, height=320)
            st.plotly_chart(fig_v, use_container_width=True)

        with col_table:
            display = vinculo_df.select([
                pl.col("vinculo").replace(VINCULO_LABELS).alias("Vínculo"),
                pl.col("num_servidores").alias("Servidores"),
                pl.col("total_liquido").map_elements(
                    lambda v: f"R$ {v:,.0f}".replace(",", "."), return_dtype=pl.Utf8
                ).alias("Total Líquido"),
                pl.col("total_bruto").map_elements(
                    lambda v: f"R$ {v:,.0f}".replace(",", "."), return_dtype=pl.Utf8
                ).alias("Total Bruto"),
            ])
            st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        st.info(f"Sem dados para {MESES_PT.get(mes_sel)}/{ano_sel}.")


# ══ TAB 2 — Remunerações ═════════════════════════════════════════════════

with tab_rem:
    st.subheader(f"Maiores salários líquidos — {MESES_PT.get(mes_sel)} / {ano_sel}")

    top_n = st.slider("Quantidade de servidores", 10, 50, 20, key="top_n_slider")
    top_df = load_top_rem(ano_sel, mes_sel, top_n)

    if not top_df.is_empty():
        fig_top = px.bar(
            top_df.to_pandas(),
            x="remuneracao_liquida",
            y="nome",
            orientation="h",
            color="vinculo",
            color_discrete_map=VINCULO_COLORS,
            labels={
                "remuneracao_liquida": "Remuneração Líquida (R$)",
                "nome": "Servidor",
                "vinculo": "Vínculo",
            },
            text="remuneracao_liquida",
            height=max(350, top_n * 28),
        )
        fig_top.update_traces(
            texttemplate="R$ %{x:,.0f}",
            textposition="outside",
        )
        fig_top.update_layout(
            yaxis=dict(categoryorder="total ascending"),
            xaxis_tickformat=",.0f",
        )
        st.plotly_chart(fig_top, use_container_width=True)

        with st.expander("Ver tabela completa"):
            display = top_df.select([
                pl.col("nome").alias("Nome"),
                pl.col("lotacao_sigla").alias("Lotação"),
                pl.col("vinculo").replace(VINCULO_LABELS).alias("Vínculo"),
                pl.col("cargo_nome").alias("Cargo"),
                pl.col("tipo_folha").alias("Tipo de Folha"),
                pl.col("remuneracao_basica").map_elements(
                    lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                    return_dtype=pl.Utf8
                ).alias("Básica"),
                pl.col("remuneracao_liquida").map_elements(
                    lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                    return_dtype=pl.Utf8
                ).alias("Líquida"),
            ])
            st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        st.info(f"Sem dados de remuneração para {MESES_PT.get(mes_sel)}/{ano_sel}.")

    st.divider()
    st.subheader(f"Componentes da folha — {MESES_PT.get(mes_sel)} / {ano_sel}")

    comp_df = load_componentes(ano_sel, mes_sel)
    if not comp_df.is_empty():
        comp_row = comp_df.row(0, named=True)
        comp_labels = {
            "remuneracao_basica": "Remuneração Básica",
            "vantagens_pessoais": "Vantagens Pessoais",
            "funcao_comissionada": "Função Comissionada",
            "gratificacao_natalina": "Gratificação Natalina (13º)",
            "horas_extras": "Horas Extras",
            "outras_eventuais": "Outras Eventuais",
            "diarias": "Diárias",
            "auxilios": "Auxílios",
            "abono_permanencia": "Abono Permanência",
            "vantagens_indenizatorias": "Vantagens Indenizatórias",
        }
        comp_data = [
            {"componente": label, "valor": float(comp_row.get(col) or 0)}
            for col, label in comp_labels.items()
            if (comp_row.get(col) or 0) > 0
        ]
        if comp_data:
            comp_pl = pl.DataFrame(comp_data).sort("valor", descending=True)
            fig_comp = px.bar(
                comp_pl.to_pandas(),
                x="valor",
                y="componente",
                orientation="h",
                color_discrete_sequence=["#1f6cb0"],
                labels={"valor": "Total (R$)", "componente": "Componente"},
                text="valor",
            )
            fig_comp.update_traces(
                texttemplate="R$ %{x:,.0f}",
                textposition="outside",
            )
            fig_comp.update_layout(
                yaxis=dict(categoryorder="total ascending"),
                xaxis_tickformat=",.0f",
                showlegend=False,
                height=380,
            )
            st.plotly_chart(fig_comp, use_container_width=True)

    st.divider()
    st.subheader(f"Maiores lotações por folha — {MESES_PT.get(mes_sel)} / {ano_sel}")

    lot_df = load_lotacoes(ano_sel, mes_sel, 15)
    if not lot_df.is_empty():
        fig_lot = px.bar(
            lot_df.to_pandas(),
            x="total_bruto",
            y="lotacao_sigla",
            orientation="h",
            text="total_bruto",
            color_discrete_sequence=["#2c7bb6"],
            labels={
                "total_bruto": "Total Bruto (R$)",
                "lotacao_sigla": "Unidade (sigla)",
            },
            hover_data={"lotacao_nome": True},
            height=max(350, len(lot_df) * 30),
        )
        fig_lot.update_traces(
            texttemplate="R$ %{x:,.0f}",
            textposition="outside",
        )
        fig_lot.update_layout(
            yaxis=dict(categoryorder="total ascending"),
            xaxis_tickformat=",.0f",
            showlegend=False,
        )
        st.plotly_chart(fig_lot, use_container_width=True)


# ══ TAB 3 — Pensionistas ═════════════════════════════════════════════════

with tab_pen:
    st.subheader("Evolução da folha de pensionistas (2019–presente)")

    pen_trend = load_pensionistas_trend()
    if not pen_trend.is_empty():
        pen_trend = pen_trend.with_columns(
            pl.concat_str([
                pl.col("ano").cast(pl.Utf8),
                pl.lit("-"),
                pl.col("mes").cast(pl.Utf8).str.zfill(2),
            ]).alias("periodo")
        )

        col_l, col_r = st.columns(2)
        with col_l:
            fig_pen = px.line(
                pen_trend.to_pandas(),
                x="periodo",
                y="total_liquido",
                labels={"periodo": "Mês", "total_liquido": "Total Líquido (R$)"},
                title="Total líquido mensal",
                color_discrete_sequence=["#e07b00"],
            )
            fig_pen.update_layout(yaxis_tickformat=",.0f", height=320)
            st.plotly_chart(fig_pen, use_container_width=True)

        with col_r:
            fig_pen_n = px.line(
                pen_trend.to_pandas(),
                x="periodo",
                y="num_pensionistas",
                labels={"periodo": "Mês", "num_pensionistas": "Pensionistas"},
                title="Número de pensionistas",
                color_discrete_sequence=["#c0392b"],
            )
            fig_pen_n.update_layout(height=320)
            st.plotly_chart(fig_pen_n, use_container_width=True)
    else:
        st.info("Dados de tendência de pensionistas não disponíveis.")

    st.divider()
    st.subheader(f"Maiores pensionistas — {MESES_PT.get(mes_sel)} / {ano_sel}")

    pen_top = load_top_pensionistas(ano_sel, mes_sel, 10)
    if not pen_top.is_empty():
        display_pen = pen_top.select([
            pl.col("nome").alias("Pensionista"),
            pl.col("nome_instituidor").alias("Instituidor"),
            pl.col("vinculo").alias("Vínculo"),
            pl.col("cargo_nome").alias("Cargo"),
            pl.col("tipo_folha").alias("Tipo de Folha"),
            pl.col("remuneracao_basica").map_elements(
                lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                return_dtype=pl.Utf8
            ).alias("Básica"),
            pl.col("remuneracao_liquida").map_elements(
                lambda v: f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                return_dtype=pl.Utf8
            ).alias("Líquida"),
        ])
        st.dataframe(display_pen, use_container_width=True, hide_index=True)
    else:
        st.info(f"Sem dados de pensionistas para {MESES_PT.get(mes_sel)}/{ano_sel}.")


# ══ TAB 4 — Horas Extras ═════════════════════════════════════════════════

with tab_horas:
    st.subheader("Evolução de horas extras pagas (2019–presente)")

    horas_trend = load_horas_trend()
    if not horas_trend.is_empty():
        horas_trend = horas_trend.with_columns(
            pl.concat_str([
                pl.col("ano").cast(pl.Utf8),
                pl.lit("-"),
                pl.col("mes").cast(pl.Utf8).str.zfill(2),
            ]).alias("periodo")
        )

        col_l2, col_r2 = st.columns(2)
        with col_l2:
            fig_ht = px.bar(
                horas_trend.to_pandas(),
                x="periodo",
                y="total_valor",
                labels={"periodo": "Mês", "total_valor": "Total Pago (R$)"},
                title="Total de horas extras por mês",
                color_discrete_sequence=["#6a9f3e"],
            )
            fig_ht.update_layout(yaxis_tickformat=",.0f", height=320)
            st.plotly_chart(fig_ht, use_container_width=True)

        with col_r2:
            fig_hn = px.line(
                horas_trend.to_pandas(),
                x="periodo",
                y="num_servidores",
                labels={"periodo": "Mês", "num_servidores": "Servidores c/ H.E."},
                title="Servidores com horas extras",
                color_discrete_sequence=["#8e44ad"],
            )
            fig_hn.update_layout(height=320)
            st.plotly_chart(fig_hn, use_container_width=True)
    else:
        st.info("Dados de horas extras não disponíveis.")

    st.divider()
    st.subheader(f"Horas extras por unidade — {MESES_PT.get(mes_sel)} / {ano_sel}")

    horas_lot = load_horas_lotacao(ano_sel, mes_sel, 15)
    if not horas_lot.is_empty():
        fig_hl = px.bar(
            horas_lot.to_pandas(),
            x="total_valor",
            y="lotacao_sigla",
            orientation="h",
            text="total_valor",
            color_discrete_sequence=["#6a9f3e"],
            labels={
                "total_valor": "Total Pago (R$)",
                "lotacao_sigla": "Unidade (sigla)",
            },
            hover_data={"lotacao_nome": True, "num_servidores": True},
            height=max(300, len(horas_lot) * 30),
        )
        fig_hl.update_traces(
            texttemplate="R$ %{x:,.0f}",
            textposition="outside",
        )
        fig_hl.update_layout(
            yaxis=dict(categoryorder="total ascending"),
            xaxis_tickformat=",.0f",
            showlegend=False,
        )
        st.plotly_chart(fig_hl, use_container_width=True)
    else:
        st.info(f"Sem dados de horas extras para {MESES_PT.get(mes_sel)}/{ano_sel}.")

    st.divider()
    st.caption(
        "**Fonte:** API Administrativa do Senado Federal — "
        "https://adm.senado.gov.br/adm-dadosabertos | "
        "Dados disponíveis a partir de 2019."
    )
