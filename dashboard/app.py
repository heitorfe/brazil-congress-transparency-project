import streamlit as st
import polars as pl
import plotly.express as px

from queries import get_all_senators, get_party_composition

st.set_page_config(
    page_title="Senado Federal — Transparência",
    page_icon="🏛️",
    layout="wide",
)

st.title("🏛️ Senado Federal do Brasil")
st.caption("Dados oficiais extraídos da API de Dados Abertos do Senado Federal.")

# ── Load data ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_data() -> pl.DataFrame:
    return get_all_senators()

@st.cache_data(ttl=3600)
def load_party_composition() -> pl.DataFrame:
    return get_party_composition()

df = load_data()
party_df = load_party_composition()

# ── National KPIs ──────────────────────────────────────────────────────────
total = len(df)
pct_feminino = round(100 * len(df.filter(pl.col("sexo") == "Feminino")) / total, 1) if total else 0
num_partidos = df["partido_sigla"].n_unique()

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

k1, k2, k3, k4 = st.columns(4)
k1.metric("Senadores em exercício", total)
k2.metric("Senadoras", f"{pct_feminino}%", help="Percentual de senadoras do total em exercício")
k3.metric("Partidos representados", num_partidos)
k4.metric("Idade média", f"{idade_media} anos")

st.divider()

# ── Reelection alert ───────────────────────────────────────────────────────
reeleicao_df = df.filter(
    pl.col("mandato_fim").cast(pl.Utf8).str.slice(0, 4).is_in(["2026", "2027"])
)
n_reeleicao = len(reeleicao_df)
if n_reeleicao > 0:
    st.info(
        f"🗳️ **{n_reeleicao} senadores** têm mandato encerrando em 2027 e são "
        f"**candidatos à reeleição nas eleições de 2026**. "
        f"Veja a coluna 'Reeleição 2026' na tabela abaixo.",
        icon=None,
    )

# ── Charts ─────────────────────────────────────────────────────────────────
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Composição por partido")
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
    st.subheader("Senadores por estado (UF)")
    estado_df = (
        df.group_by("estado_sigla")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    fig_estado = px.bar(
        estado_df.to_pandas(),
        x="count",
        y="estado_sigla",
        orientation="h",
        color="count",
        color_continuous_scale="Blues",
        labels={"count": "Senadores", "estado_sigla": "UF"},
    )
    fig_estado.update_layout(
        coloraxis_showscale=False,
        margin=dict(t=10, b=10, l=10, r=10),
        height=320,
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig_estado, use_container_width=True)

st.divider()

# ── Filters ────────────────────────────────────────────────────────────────
st.subheader("Filtros")
col1, col2, col3, col4 = st.columns(4)

partidos = sorted(df["partido_sigla"].drop_nulls().unique().to_list())
estados  = sorted(df["estado_sigla"].drop_nulls().unique().to_list())

sel_partidos    = col1.multiselect("Partido", partidos)
sel_estados     = col2.multiselect("Estado (UF)", estados)
sel_sexo        = col3.selectbox("Sexo", ["Todos", "Masculino", "Feminino"])
sel_reeleicao   = col4.checkbox("Apenas candidatos à reeleição 2026")

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

# ── Summary metrics ────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
m1.metric("Senadores (filtro)", len(filtered))
m2.metric("Partidos", filtered["partido_sigla"].n_unique())
m3.metric("Estados", filtered["estado_sigla"].n_unique())
m4.metric(
    "Senadoras",
    len(filtered.filter(pl.col("sexo") == "Feminino")),
)

st.divider()

# ── Senator table ──────────────────────────────────────────────────────────
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
    "reeleicao_2026":        "🗳️ Reeleição 2026",
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

st.caption("Fonte: API de Dados Abertos do Senado Federal — legis.senado.leg.br/dadosabertos")
