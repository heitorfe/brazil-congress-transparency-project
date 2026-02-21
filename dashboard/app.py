import streamlit as st
import polars as pl

from queries import get_all_senators

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

df = load_data()

# ── Filters ────────────────────────────────────────────────────────────────
st.subheader("Filtros")
col1, col2, col3 = st.columns(3)

partidos = sorted(df["partido_sigla"].drop_nulls().unique().to_list())
estados = sorted(df["estado_sigla"].drop_nulls().unique().to_list())

sel_partidos = col1.multiselect("Partido", partidos)
sel_estados  = col2.multiselect("Estado (UF)", estados)
sel_sexo     = col3.selectbox("Sexo", ["Todos", "Masculino", "Feminino"])

filtered = df
if sel_partidos:
    filtered = filtered.filter(pl.col("partido_sigla").is_in(sel_partidos))
if sel_estados:
    filtered = filtered.filter(pl.col("estado_sigla").is_in(sel_estados))
if sel_sexo != "Todos":
    filtered = filtered.filter(pl.col("sexo") == sel_sexo)

st.divider()

# ── Summary metrics ────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
m1.metric("Senadores", len(filtered))
m2.metric("Partidos", filtered["partido_sigla"].n_unique())
m3.metric("Estados", filtered["estado_sigla"].n_unique())
m4.metric(
    "Senadoras",
    len(filtered.filter(pl.col("sexo") == "Feminino")),
)

st.divider()

# ── Senator table ──────────────────────────────────────────────────────────
display = filtered.select([
    "nome_parlamentar",
    "partido_sigla",
    "estado_sigla",
    "sexo",
    "mandato_inicio",
    "mandato_fim",
]).rename({
    "nome_parlamentar": "Nome",
    "partido_sigla": "Partido",
    "estado_sigla": "UF",
    "sexo": "Sexo",
    "mandato_inicio": "Início do mandato",
    "mandato_fim": "Fim do mandato",
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
    


