import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from queries import (
    get_pessoal_kpis,
    get_remuneracao_por_ano,
    get_remuneracao_mensal_por_ano,
    get_vinculo_por_ano,
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
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr",
    5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Set", 10: "Out", 11: "Nov", 12: "Dez",
}
MESES_PT_FULL = {
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


def _fmt_brl(v: float) -> str:
    return f"R$ {v:,.0f}".replace(",", ".")


# ── Cached loaders ─────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_kpis():
    return get_pessoal_kpis()

@st.cache_data(ttl=3600)
def load_anual():
    return get_remuneracao_por_ano()

@st.cache_data(ttl=3600)
def load_mensal(ano: int):
    return get_remuneracao_mensal_por_ano(ano)

@st.cache_data(ttl=3600)
def load_vinculo_ano(ano: int):
    return get_vinculo_por_ano(ano)

@st.cache_data(ttl=3600)
def load_anos():
    return get_remuneracoes_anos_disponiveis()

@st.cache_data(ttl=3600)
def load_meses(ano: int):
    return get_remuneracoes_meses_disponiveis(ano)

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
def load_pensionistas_trend():
    return get_pensionistas_trend()

@st.cache_data(ttl=3600)
def load_top_pensionistas(ano: int, mes: int, n: int):
    return get_top_pensionistas(ano, mes, n)

@st.cache_data(ttl=3600)
def load_horas_trend():
    return get_horas_extras_trend()

@st.cache_data(ttl=3600)
def load_horas_lotacao(ano: int, mes: int, n: int):
    return get_horas_extras_por_lotacao(ano, mes, n)


# ── Page title ─────────────────────────────────────────────────────────────

st.title("💼 Pessoal e Remunerações do Senado Federal")
st.caption(
    "Transparência dos gastos com servidores, pensionistas e horas extras — "
    "fonte: API Administrativa do Senado (ADM), 2019–presente."
)

anos_disponiveis = load_anos()
if not anos_disponiveis:
    st.error("Dados não disponíveis. Execute `extract_servidores.py` primeiro.")
    st.stop()

# ── KPI Header ─────────────────────────────────────────────────────────────

kpis = load_kpis()
c1, c2, c3, c4 = st.columns(4)
c1.metric("Servidores ativos", f"{kpis['num_servidores_ativos']:,}".replace(",", "."))
c2.metric("Pensionistas", f"{kpis['num_pensionistas']:,}".replace(",", "."))
c3.metric(
    f"Folha líquida ({MESES_PT_FULL.get(kpis['mes_ref'], '')} {kpis['ano_ref']})",
    _fmt_brl(kpis["total_liquido_mes"]),
)
c4.metric(
    f"Horas extras ({MESES_PT_FULL.get(kpis['mes_ref'], '')} {kpis['ano_ref']})",
    _fmt_brl(kpis["total_horas_extras_mes"]),
)

st.divider()


# ══════════════════════════════════════════════════════════════════════════
# NÍVEL 1 — PANORAMA ANUAL
# ══════════════════════════════════════════════════════════════════════════

st.header("📅 Panorama Anual (2019–presente)")

anual_df = load_anual()
if not anual_df.is_empty():
    anual_pd = anual_df.to_pandas()

    fig_anual = go.Figure()
    fig_anual.add_trace(go.Bar(
        x=anual_pd["ano"].astype(str),
        y=anual_pd["total_bruto"],
        name="Folha Bruta",
        marker_color="#2c7bb6",
        text=anual_pd["total_bruto"].astype(float).apply(lambda v: f"R$ {v / 1e9:.2f}B"),
        textposition="outside",
    ))
    fig_anual.add_trace(go.Bar(
        x=anual_pd["ano"].astype(str),
        y=anual_pd["total_liquido"],
        name="Folha Líquida",
        marker_color="#74c476",
        opacity=0.85,
    ))
    fig_anual.update_layout(
        barmode="group",
        xaxis_title="Ano",
        yaxis_title="Total (R$)",
        yaxis_tickformat=",.0f",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        height=360,
        margin=dict(t=20),
    )
    st.plotly_chart(fig_anual, use_container_width=True)

    with st.expander("Ver tabela anual completa"):
        display_anual = anual_df.select([
            pl.col("ano").alias("Ano"),
            pl.col("num_meses").alias("Meses"),
            pl.col("avg_servidores").alias("Média Serv./Mês"),
            pl.col("total_bruto").map_elements(
                lambda v: _fmt_brl(v), return_dtype=pl.Utf8
            ).alias("Total Bruto"),
            pl.col("total_liquido").map_elements(
                lambda v: _fmt_brl(v), return_dtype=pl.Utf8
            ).alias("Total Líquido"),
        ])
        st.dataframe(display_anual, use_container_width=True, hide_index=True)
else:
    st.info("Dados anuais não disponíveis.")

# Year selector — drives Sections 2 and 3
st.subheader("⬇ Selecione o ano para detalhar")
ano_sel = st.selectbox(
    "Ano",
    options=anos_disponiveis,
    index=0,
    label_visibility="collapsed",
    key="ano_sel",
)

st.divider()


# ══════════════════════════════════════════════════════════════════════════
# NÍVEL 2 — DETALHE MENSAL (para o ano selecionado)
# ══════════════════════════════════════════════════════════════════════════

st.header(f"📆 Evolução Mensal — {ano_sel}")

mensal_df = load_mensal(ano_sel)
if not mensal_df.is_empty():
    mensal_pd = mensal_df.to_pandas()
    mensal_pd["mes_label"] = mensal_pd["mes"].apply(lambda m: MESES_PT.get(m, str(m)))

    col_chart, col_donut = st.columns([2, 1])

    with col_chart:
        fig_mensal = go.Figure()
        fig_mensal.add_trace(go.Bar(
            x=mensal_pd["mes_label"],
            y=mensal_pd["total_bruto"],
            name="Bruto",
            marker_color="#2c7bb6",
        ))
        fig_mensal.add_trace(go.Scatter(
            x=mensal_pd["mes_label"],
            y=mensal_pd["total_liquido"],
            mode="lines+markers",
            name="Líquido",
            line=dict(color="#74c476", width=2),
        ))
        fig_mensal.update_layout(
            title=f"Folha mensal em {ano_sel}",
            xaxis_title="Mês",
            yaxis_title="Total (R$)",
            yaxis_tickformat=",.0f",
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=320,
            margin=dict(t=40),
        )
        st.plotly_chart(fig_mensal, use_container_width=True)

    with col_donut:
        vinculo_df = load_vinculo_ano(ano_sel)
        if not vinculo_df.is_empty():
            vinculo_pd = vinculo_df.to_pandas()
            vinculo_pd["vinculo_label"] = vinculo_pd["vinculo"].map(
                lambda v: VINCULO_LABELS.get(v, v)
            )
            fig_v = px.pie(
                vinculo_pd,
                names="vinculo_label",
                values="total_bruto",
                hole=0.45,
                color="vinculo",
                color_discrete_map={VINCULO_LABELS.get(k, k): c for k, c in VINCULO_COLORS.items()},
                title=f"Por vínculo — {ano_sel}",
            )
            fig_v.update_traces(textposition="inside", textinfo="percent+label")
            fig_v.update_layout(showlegend=False, height=320, margin=dict(t=40))
            st.plotly_chart(fig_v, use_container_width=True)

    # Summary stats for the year
    avg_ser = int(mensal_df["num_servidores"].mean())
    peak = mensal_df.sort("total_bruto", descending=True).row(0, named=True)
    c_a, c_b = st.columns(2)
    c_a.metric("Média de servidores/mês", f"{avg_ser:,}".replace(",", "."))
    c_b.metric(
        "Mês de maior folha bruta",
        _fmt_brl(peak["total_bruto"]),
        delta=MESES_PT_FULL.get(peak["mes"], str(peak["mes"])),
        delta_color="off",
    )
else:
    st.info(f"Sem dados mensais para {ano_sel}.")

# Month selector — drives Section 3
meses_disponiveis = load_meses(ano_sel)
st.subheader("⬇ Selecione o mês para detalhar")
mes_sel = st.selectbox(
    "Mês",
    options=meses_disponiveis,
    format_func=lambda m: MESES_PT_FULL.get(m, str(m)),
    index=0,
    label_visibility="collapsed",
    key="mes_sel",
)

st.divider()


# ══════════════════════════════════════════════════════════════════════════
# NÍVEL 3 — MAIORES REMUNERAÇÕES (para o mês selecionado)
# ══════════════════════════════════════════════════════════════════════════

st.header(f"👤 Maiores Remunerações — {MESES_PT_FULL.get(mes_sel)} / {ano_sel}")

top_n = st.slider("Número de servidores exibidos", 10, 50, 20, key="top_n_slider")
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
        height=max(420, top_n * 28),
    )
    fig_top.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
    fig_top.update_layout(
        yaxis=dict(categoryorder="total ascending"),
        xaxis_tickformat=",.0f",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_top, use_container_width=True)

    col_comp, col_lot = st.columns(2)

    with col_comp:
        with st.expander("📊 Componentes da folha do mês", expanded=True):
            comp_df = load_componentes(ano_sel, mes_sel)
            if not comp_df.is_empty():
                comp_row = comp_df.row(0, named=True)
                comp_labels = {
                    "remuneracao_basica":       "Remuneração Básica",
                    "vantagens_pessoais":        "Vantagens Pessoais",
                    "funcao_comissionada":       "Função Comissionada",
                    "gratificacao_natalina":     "Gratificação Natalina",
                    "horas_extras":              "Horas Extras",
                    "outras_eventuais":          "Outras Eventuais",
                    "diarias":                   "Diárias",
                    "auxilios":                  "Auxílios",
                    "abono_permanencia":         "Abono Permanência",
                    "vantagens_indenizatorias":  "Vantagens Indenizatórias",
                }
                comp_data = [
                    {"componente": lbl, "valor": float(comp_row.get(col) or 0)}
                    for col, lbl in comp_labels.items()
                    if (comp_row.get(col) or 0) > 0
                ]
                if comp_data:
                    comp_pl = pl.DataFrame(comp_data).sort("valor", descending=True)
                    fig_comp = px.bar(
                        comp_pl.to_pandas(),
                        x="valor", y="componente", orientation="h",
                        color_discrete_sequence=["#2c7bb6"],
                        text="valor", height=320,
                    )
                    fig_comp.update_traces(
                        texttemplate="R$ %{x:,.0f}", textposition="outside"
                    )
                    fig_comp.update_layout(
                        yaxis=dict(categoryorder="total ascending"),
                        xaxis_tickformat=",.0f",
                        showlegend=False,
                        margin=dict(l=0, r=10, t=10, b=0),
                    )
                    st.plotly_chart(fig_comp, use_container_width=True)

    with col_lot:
        with st.expander("🏢 Maiores lotações por folha", expanded=True):
            lot_df = load_lotacoes(ano_sel, mes_sel, 15)
            if not lot_df.is_empty():
                fig_lot = px.bar(
                    lot_df.to_pandas(),
                    x="total_bruto", y="lotacao_sigla", orientation="h",
                    text="total_bruto",
                    color_discrete_sequence=["#2c7bb6"],
                    hover_data={"lotacao_nome": True},
                    height=320,
                )
                fig_lot.update_traces(
                    texttemplate="R$ %{x:,.0f}", textposition="outside"
                )
                fig_lot.update_layout(
                    yaxis=dict(categoryorder="total ascending"),
                    xaxis_tickformat=",.0f",
                    showlegend=False,
                    margin=dict(l=0, r=10, t=10, b=0),
                )
                st.plotly_chart(fig_lot, use_container_width=True)

    with st.expander("📋 Tabela completa dos maiores salários"):
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
    st.info(f"Sem dados de remuneração para {MESES_PT_FULL.get(mes_sel)}/{ano_sel}.")

st.divider()


# ══════════════════════════════════════════════════════════════════════════
# SUPLEMENTAR — PENSIONISTAS & HORAS EXTRAS
# ══════════════════════════════════════════════════════════════════════════

with st.expander("🧓 Pensionistas — Histórico e Maiores Valores"):
    pen_trend = load_pensionistas_trend()
    if not pen_trend.is_empty():
        pen_trend = pen_trend.with_columns(
            pl.concat_str([
                pl.col("ano").cast(pl.Utf8),
                pl.lit("-"),
                pl.col("mes").cast(pl.Utf8).str.zfill(2),
            ]).alias("periodo")
        )
        col_pl, col_pr = st.columns(2)
        with col_pl:
            fig_pen = px.line(
                pen_trend.to_pandas(),
                x="periodo", y="total_liquido",
                title="Folha líquida mensal — Pensionistas",
                color_discrete_sequence=["#e07b00"],
                labels={"periodo": "Mês", "total_liquido": "Total Líquido (R$)"},
            )
            fig_pen.update_layout(yaxis_tickformat=",.0f", height=300)
            st.plotly_chart(fig_pen, use_container_width=True)
        with col_pr:
            fig_pen_n = px.line(
                pen_trend.to_pandas(),
                x="periodo", y="num_pensionistas",
                title="Número de pensionistas por mês",
                color_discrete_sequence=["#c0392b"],
                labels={"periodo": "Mês", "num_pensionistas": "Pensionistas"},
            )
            fig_pen_n.update_layout(height=300)
            st.plotly_chart(fig_pen_n, use_container_width=True)
    else:
        st.info("Dados de tendência de pensionistas não disponíveis.")

    st.subheader(f"Maiores pensionistas — {MESES_PT_FULL.get(mes_sel)}/{ano_sel}")
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
        st.info(f"Sem dados de pensionistas para {MESES_PT_FULL.get(mes_sel)}/{ano_sel}.")


with st.expander("⏱️ Horas Extras — Histórico e Por Unidade"):
    horas_trend = load_horas_trend()
    if not horas_trend.is_empty():
        horas_trend = horas_trend.with_columns(
            pl.concat_str([
                pl.col("ano").cast(pl.Utf8),
                pl.lit("-"),
                pl.col("mes").cast(pl.Utf8).str.zfill(2),
            ]).alias("periodo")
        )
        col_hl, col_hr = st.columns(2)
        with col_hl:
            fig_ht = px.bar(
                horas_trend.to_pandas(),
                x="periodo", y="total_valor",
                title="Total de horas extras pagas por mês",
                color_discrete_sequence=["#6a9f3e"],
                labels={"periodo": "Mês", "total_valor": "Total Pago (R$)"},
            )
            fig_ht.update_layout(yaxis_tickformat=",.0f", height=300)
            st.plotly_chart(fig_ht, use_container_width=True)
        with col_hr:
            fig_hn = px.line(
                horas_trend.to_pandas(),
                x="periodo", y="num_servidores",
                title="Servidores com horas extras",
                color_discrete_sequence=["#8e44ad"],
                labels={"periodo": "Mês", "num_servidores": "Servidores c/ H.E."},
            )
            fig_hn.update_layout(height=300)
            st.plotly_chart(fig_hn, use_container_width=True)
    else:
        st.info("Dados de horas extras não disponíveis.")

    st.subheader(f"Por unidade — {MESES_PT_FULL.get(mes_sel)}/{ano_sel}")
    horas_lot = load_horas_lotacao(ano_sel, mes_sel, 15)
    if not horas_lot.is_empty():
        fig_hl2 = px.bar(
            horas_lot.to_pandas(),
            x="total_valor", y="lotacao_sigla", orientation="h",
            text="total_valor",
            color_discrete_sequence=["#6a9f3e"],
            hover_data={"lotacao_nome": True, "num_servidores": True},
            height=max(300, len(horas_lot) * 30),
        )
        fig_hl2.update_traces(texttemplate="R$ %{x:,.0f}", textposition="outside")
        fig_hl2.update_layout(
            yaxis=dict(categoryorder="total ascending"),
            xaxis_tickformat=",.0f",
            showlegend=False,
        )
        st.plotly_chart(fig_hl2, use_container_width=True)
    else:
        st.info(f"Sem dados de horas extras para {MESES_PT_FULL.get(mes_sel)}/{ano_sel}.")

st.caption(
    "**Fonte:** API Administrativa do Senado Federal — "
    "https://adm.senado.gov.br/adm-dadosabertos | "
    "Dados disponíveis a partir de 2019."
)
