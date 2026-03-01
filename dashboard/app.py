import streamlit as st
import polars as pl
import plotly.express as px

from queries import (
    get_all_senators,
    get_party_composition,
    get_all_deputies,
    get_emendas_kpis,
    get_emendas_por_uf,
)

st.set_page_config(
    page_title="Congresso Nacional — Transparência",
    page_icon="🏛️",
    layout="wide",
)

st.title("🏛️ Congresso Nacional — Painel de Transparência")
st.caption(
    "Dados oficiais do Senado Federal e da Câmara dos Deputados. "
    "Fontes: API legis.senado.leg.br · dadosabertos.camara.leg.br · Portal da Transparência."
)

# ── Load data ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data() -> pl.DataFrame:
    return get_all_senators()

@st.cache_data(ttl=3600)
def load_party_composition() -> pl.DataFrame:
    return get_party_composition()

@st.cache_data(ttl=3600)
def load_deputies() -> pl.DataFrame:
    return get_all_deputies()

@st.cache_data(ttl=3600)
def load_emendas_kpis() -> dict:
    return get_emendas_kpis()

@st.cache_data(ttl=3600)
def load_emendas_por_uf() -> pl.DataFrame:
    return get_emendas_por_uf()

df = load_data()
party_df = load_party_composition()
deputies_df = load_deputies()
emendas_kpis = load_emendas_kpis()
emendas_uf_df = load_emendas_por_uf()

# ── National KPIs ──────────────────────────────────────────────────────────
total = len(df)
pct_feminino = round(100 * len(df.filter(pl.col("sexo") == "Feminino")) / total, 1) if total else 0
num_partidos_senado = df["partido_sigla"].n_unique()

from datetime import date
hoje = date.today()

idades = (
    df.filter(pl.col("data_nascimento").is_not_null())
    .with_columns(
        ((pl.lit(hoje) - pl.col("data_nascimento").cast(pl.Date)).dt.total_days() / 365.25)
        .alias("idade")
    )["idade"]
)
idade_media = round(idades.mean(), 1) if len(idades) > 0 else 0

dep_ativos = deputies_df.filter(pl.col("em_exercicio") == True)
n_deputados = len(dep_ativos)
num_partidos_camara = dep_ativos["sigla_partido"].n_unique()
total_emendas_brl = float(emendas_kpis.get("total_pago", 0) or 0)

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Senadores em exercício", total)
k2.metric("Deputados em exercício", f"{n_deputados:,}".replace(",", "."))
k3.metric("Senadoras", f"{pct_feminino}%", help="Percentual de senadoras do total em exercício")
k4.metric(
    "Partidos — Senado / Câmara",
    f"{num_partidos_senado} / {num_partidos_camara}",
    help="Partidos representados no Senado e na Câmara dos Deputados",
)
k5.metric("Idade média (Senado)", f"{idade_media} anos")
k6.metric(
    "Total emendas pagas (2014–hoje)",
    f"R$ {total_emendas_brl / 1e9:.1f}B",
    help="Soma de emendas parlamentares efetivamente pagas (fase Pagamento) desde 2014",
)

st.divider()

# ── Reelection alert ───────────────────────────────────────────────────────
reeleicao_df = df.filter(
    pl.col("mandato_fim").cast(pl.Utf8).str.slice(0, 4).is_in(["2026", "2027"])
)
n_reeleicao = len(reeleicao_df)
if n_reeleicao > 0:
    st.info(
        f"🗳️ **{n_reeleicao} senadores** têm mandato encerrando em 2027 e são "
        f"**possíveis candidatos à reeleição em 2026**. "
        f"Veja a coluna 'Poss. reeleição 2026' na tabela abaixo.",
        icon=None,
    )

# ── Charts — party composition (both chambers) ─────────────────────────────
st.subheader("Composição partidária do Congresso")

chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.caption("**Senado Federal** (81 senadores)")
    fig_party = px.pie(
        party_df.to_pandas(),
        names="partido_sigla",
        values="num_senadores",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set3,
    )
    fig_party.update_traces(textposition="inside", textinfo="percent+label")
    fig_party.update_layout(
        showlegend=False,
        margin=dict(t=10, b=10, l=10, r=10),
        height=320,
    )
    st.plotly_chart(fig_party, use_container_width=True)

with chart_col2:
    st.caption("**Câmara dos Deputados** (em exercício)")
    dep_party_df = (
        dep_ativos
        .group_by("sigla_partido")
        .agg(pl.len().alias("num_deputados"))
        .sort("num_deputados", descending=True)
    )
    fig_dep_party = px.pie(
        dep_party_df.to_pandas(),
        names="sigla_partido",
        values="num_deputados",
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig_dep_party.update_traces(textposition="inside", textinfo="percent+label")
    fig_dep_party.update_layout(
        showlegend=False,
        margin=dict(t=10, b=10, l=10, r=10),
        height=320,
    )
    st.plotly_chart(fig_dep_party, use_container_width=True)

st.divider()

# ── Emendas parlamentares por UF (full width) ──────────────────────────────
st.subheader("Estados que mais receberam emendas parlamentares")
st.caption("Soma de emendas efetivamente pagas por UF de destino do recurso (2014–presente).")
if not emendas_uf_df.is_empty():
    uf_top = emendas_uf_df.sort("total_pago", descending=True).head(15)
    fig_uf = px.bar(
        uf_top.to_pandas(),
        x="total_pago",
        y="uf_recurso",
        orientation="h",
        color="total_pago",
        color_continuous_scale="Greens",
        labels={"total_pago": "Total pago (R$)", "uf_recurso": "UF"},
        text="total_pago",
    )
    fig_uf.update_traces(
        texttemplate="R$ %{x:,.0f}",
        textposition="outside",
    )
    fig_uf.update_layout(
        coloraxis_showscale=False,
        margin=dict(t=10, b=10, l=10, r=120),
        height=420,
        xaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig_uf, use_container_width=True)
else:
    st.info("Dados de emendas por UF não disponíveis.")

st.divider()

# ── Senator table ──────────────────────────────────────────────────────────
st.subheader("Senadores em exercício")
st.caption(
    "Clique em uma linha para abrir o perfil completo do senador. "
    "Use os filtros para explorar por partido, estado ou sexo."
)

col1, col2, col3, col4 = st.columns(4)

partidos = sorted(df["partido_sigla"].drop_nulls().unique().to_list())
estados  = sorted(df["estado_sigla"].drop_nulls().unique().to_list())

sel_partidos    = col1.multiselect("Partido", partidos)
sel_estados     = col2.multiselect("Estado (UF)", estados)
sel_sexo        = col3.selectbox("Sexo", ["Todos", "Masculino", "Feminino"])
sel_reeleicao   = col4.checkbox("Apenas possíveis candidatos à reeleição 2026")

filtered = df
if sel_partidos:
    filtered = filtered.filter(pl.col("partido_sigla").is_in(sel_partidos))
if sel_estados:
    filtered = filtered.filter(pl.col("estado_sigla").is_in(sel_estados))
if sel_sexo != "Todos":
    filtered = filtered.filter(pl.col("sexo") == sel_sexo)
if sel_reeleicao:
    filtered = filtered.filter(
        pl.col("mandato_fim").cast(pl.Utf8).str.slice(0, 4).is_in(["2026", "2027"])
    )

m1, m2, m3, m4 = st.columns(4)
m1.metric("Senadores (filtro)", len(filtered))
m2.metric("Partidos", filtered["partido_sigla"].n_unique())
m3.metric("Estados", filtered["estado_sigla"].n_unique())
m4.metric("Senadoras", len(filtered.filter(pl.col("sexo") == "Feminino")))

display = filtered.with_columns(
    pl.when(pl.col("mandato_fim").cast(pl.Utf8).str.slice(0, 4).is_in(["2026", "2027"]))
    .then(pl.lit("Sim"))
    .otherwise(pl.lit("—"))
    .alias("reeleicao_2026")
).select([
    "nome_parlamentar",
    "partido_sigla",
    "estado_sigla",
    "sexo",
    "mandato_inicio",
    "mandato_fim",
    "descricao_participacao",
    "reeleicao_2026",
]).rename({
    "nome_parlamentar":      "Nome",
    "partido_sigla":         "Partido",
    "estado_sigla":          "UF",
    "sexo":                  "Sexo",
    "mandato_inicio":        "Início do mandato",
    "mandato_fim":           "Fim do mandato",
    "descricao_participacao":"Participação",
    "reeleicao_2026":        "🗳️ Poss. reeleição 2026",
})

selection = st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
)

# ── Navigate to profile on row click ───────────────────────────────────────
selected_rows = selection.selection.rows
if selected_rows:
    idx = selected_rows[0]
    senator_id = filtered["senador_id"][idx]
    st.session_state["selected_senator_id"] = senator_id
    st.switch_page("pages/1_Perfil_do_Senador.py")

st.caption(
    "Fontes: API de Dados Abertos do Senado Federal — legis.senado.leg.br/dadosabertos · "
    "API Câmara dos Deputados — dadosabertos.camara.leg.br"
)
