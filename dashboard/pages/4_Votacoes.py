import streamlit as st
import plotly.express as px
import polars as pl
from datetime import date

from queries import get_recent_voting_sessions

st.set_page_config(
    page_title="Votações do Plenário",
    page_icon="🗳️",
    layout="wide",
)

st.title("🗳️ Plenário — Histórico de Votações")
st.caption(
    "Votações nominais do Plenário do Senado Federal desde 2019. "
    "Cada linha representa uma sessão de votação com o resultado consolidado."
)

# ── Load data ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_sessions():
    return get_recent_voting_sessions(n=2000)

df = load_sessions()

if df.is_empty():
    st.error("Nenhuma votação encontrada na base de dados.")
    st.stop()

# Ensure date column is Date type for filtering
df = df.with_columns(pl.col("data_sessao").cast(pl.Date))

# ── Filters ────────────────────────────────────────────────────────────────
f1, f2, f3 = st.columns(3)

resultados = ["Todos"] + sorted(
    df["resultado_votacao"].drop_nulls().unique().to_list()
)
siglas = ["Todos"] + sorted(
    df["sigla_materia"].drop_nulls().unique().to_list()
)

sel_resultado = f1.selectbox("Resultado", resultados)
sel_sigla     = f2.selectbox("Tipo de matéria", siglas)

data_min = df["data_sessao"].min()
data_max = df["data_sessao"].max()
sel_datas = f3.date_input(
    "Período",
    value=(data_min, data_max),
    min_value=data_min,
    max_value=data_max,
)

filtered = df
if sel_resultado != "Todos":
    filtered = filtered.filter(pl.col("resultado_votacao") == sel_resultado)
if sel_sigla != "Todos":
    filtered = filtered.filter(pl.col("sigla_materia") == sel_sigla)
if isinstance(sel_datas, tuple) and len(sel_datas) == 2:
    d_ini, d_fim = sel_datas
    filtered = filtered.filter(
        (pl.col("data_sessao") >= pl.lit(d_ini))
        & (pl.col("data_sessao") <= pl.lit(d_fim))
    )

# ── KPIs ───────────────────────────────────────────────────────────────────
total_sessoes = len(filtered)
aprovadas     = len(filtered.filter(pl.col("resultado_votacao").str.contains("Aprovad"))) if total_sessoes else 0
rejeitadas    = len(filtered.filter(pl.col("resultado_votacao").str.contains("Rejeitad"))) if total_sessoes else 0
taxa_aprovacao = round(100 * aprovadas / total_sessoes, 1) if total_sessoes else 0.0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Sessões de votação", total_sessoes)
k2.metric("Aprovadas",  aprovadas)
k3.metric("Rejeitadas", rejeitadas)
k4.metric("Taxa de aprovação", f"{taxa_aprovacao}%")

st.divider()

# ── Time series: sessions per month ───────────────────────────────────────
st.subheader("Sessões de votação ao longo do tempo")

monthly = (
    filtered.with_columns(
        pl.col("data_sessao").dt.strftime("%Y-%m").alias("mes")
    )
    .group_by("mes")
    .agg(pl.len().alias("count"))
    .sort("mes")
)

fig_ts = px.line(
    monthly.to_pandas(),
    x="mes",
    y="count",
    markers=True,
    labels={"mes": "Mês", "count": "Sessões"},
    title="Sessões de votação por mês",
)
fig_ts.update_layout(height=300, margin=dict(t=40, b=10))
fig_ts.update_traces(line_color="#2980b9", marker_color="#2980b9")
st.plotly_chart(fig_ts, use_container_width=True)

st.divider()

# ── Result distribution ───────────────────────────────────────────────────
col_pie, col_tipo = st.columns(2)

with col_pie:
    result_counts = (
        filtered.filter(pl.col("resultado_votacao").is_not_null())
        .group_by("resultado_votacao")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    fig_result = px.pie(
        result_counts.to_pandas(),
        names="resultado_votacao",
        values="count",
        title="Distribuição de resultados",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_result.update_layout(height=300, margin=dict(t=40, b=10))
    st.plotly_chart(fig_result, use_container_width=True)

with col_tipo:
    tipo_counts = (
        filtered.filter(pl.col("sigla_materia").is_not_null())
        .group_by("sigla_materia")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(10)
    )
    fig_tipo = px.bar(
        tipo_counts.to_pandas(),
        x="sigla_materia",
        y="count",
        color="count",
        color_continuous_scale="Blues",
        labels={"sigla_materia": "Tipo de matéria", "count": "Sessões"},
        title="Sessões por tipo de matéria (top 10)",
    )
    fig_tipo.update_layout(
        coloraxis_showscale=False,
        height=300,
        margin=dict(t=40, b=10),
    )
    st.plotly_chart(fig_tipo, use_container_width=True)

st.divider()

# ── Sessions table ─────────────────────────────────────────────────────────
st.subheader(f"Sessões de votação ({total_sessoes} encontradas)")

display = filtered.select([
    "data_sessao",
    "materia_identificacao",
    "materia_ementa",
    "sigla_materia",
    "resultado_votacao",
    "sigla_tipo_sessao",
    "total_votos_sim",
    "total_votos_nao",
    "total_votos_abstencao",
]).rename({
    "data_sessao":          "Data",
    "materia_identificacao":"Matéria",
    "materia_ementa":       "Ementa",
    "sigla_materia":        "Tipo",
    "resultado_votacao":    "Resultado",
    "sigla_tipo_sessao":    "Tipo de sessão",
    "total_votos_sim":      "Sim",
    "total_votos_nao":      "Não",
    "total_votos_abstencao":"Abs.",
})

st.dataframe(display, use_container_width=True, hide_index=True)

st.divider()

# ── Glossário ──────────────────────────────────────────────────────────────
with st.expander("📋 Glossário — Tipos de sessão e tipos de matéria"):
    g1, g2 = st.columns(2)

    with g1:
        st.markdown("""
**Tipos de sessão (`Tipo de sessão`)**

| Sigla | Descrição |
|-------|-----------|
| **D** / **Del** | **Sessão Deliberativa** — a pauta inclui votações com resultado vinculante. É o tipo onde a maioria das leis é aprovada ou rejeitada. |
| **O** / **Ord** | **Sessão Ordinária** — sessão regular prevista no calendário legislativo, geralmente com pauta deliberativa. |
| **N** / **NDel** | **Sessão Não Deliberativa** — sem votações; destina-se a discursos, homenagens e comunicações avulsas. |
| **E** / **Ext** | **Sessão Extraordinária** — convocada fora do calendário ordinário, geralmente em períodos de recesso ou para matérias urgentes. |
| **S** / **Sol** | **Sessão Solene** — caráter comemorativo ou protocolar; homenagens, recepção de autoridades, outorga de títulos. |
| **C** / **Conj** | **Sessão Conjunta / Congresso** — realizada com deputados e senadores reunidos; delibera sobre orçamento, vetos presidenciais e EMC. |
| **Esp** | **Sessão Especial** — para finalidade específica determinada pela Mesa Diretora. |
""")

    with g2:
        st.markdown("""
**Tipos de matéria (`Tipo`)**

| Sigla | Descrição |
|-------|-----------|
| **PL** | **Projeto de Lei** — proposta de nova lei ou alteração de lei ordinária. Aprovado por maioria simples. |
| **PEC** | **Proposta de Emenda à Constituição** — altera a Constituição Federal; exige aprovação em dois turnos com 3/5 dos votos (49 senadores). |
| **PLP** | **Projeto de Lei Complementar** — lei complementar à Constituição; exige maioria absoluta (41 senadores). |
| **MPV** | **Medida Provisória** — ato do Poder Executivo com força de lei imediata; precisa ser apreciada pelo Congresso em 120 dias. |
| **RES** | **Resolução** — norma interna do Senado; não precisa de sanção presidencial. |
| **DEC** | **Decreto Legislativo** — ato do Congresso sem sanção presidencial; usado para ratificar tratados internacionais e sustar decretos executivos. |
| **MSF** | **Mensagem do Senado Federal** — comunicação oficial entre poderes (ex.: indicação de autoridades). |
| **SCD** | **Substitutivo da Câmara dos Deputados** — texto aprovado pela Câmara em substituição ao projeto original do Senado. |
""")

st.caption("Fonte: API de Dados Abertos do Senado Federal — legis.senado.leg.br/dadosabertos")
