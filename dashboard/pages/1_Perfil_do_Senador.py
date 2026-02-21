import streamlit as st

from queries import get_all_senators, get_senator_by_id

st.set_page_config(
    page_title="Perfil do Senador",
    page_icon="👤",
    layout="wide",
)

# ── Senator selection (from home page click or sidebar picker) ─────────────
@st.cache_data(ttl=3600)
def load_all():
    return get_all_senators()

all_senators = load_all()
names = all_senators.select(["senador_id", "nome_parlamentar"]).sort("nome_parlamentar")
name_to_id = dict(zip(names["nome_parlamentar"].to_list(), names["senador_id"].to_list()))

default_name = None
if "selected_senator_id" in st.session_state:
    sid = st.session_state["selected_senator_id"]
    matches = all_senators.filter(all_senators["senador_id"] == sid)["nome_parlamentar"]
    if len(matches) > 0:
        default_name = matches[0]

selected_name = st.sidebar.selectbox(
    "Selecione um senador",
    list(name_to_id.keys()),
    index=list(name_to_id.keys()).index(default_name) if default_name else 0,
)
senator_id = name_to_id[selected_name]
st.session_state["selected_senator_id"] = senator_id

# ── Load selected senator ──────────────────────────────────────────────────
row_df = get_senator_by_id(senator_id)

if row_df.is_empty():
    st.error("Senador não encontrado.")
    st.stop()

s = row_df.row(0, named=True)

# ── Layout ─────────────────────────────────────────────────────────────────
col_photo, col_info = st.columns([1, 3])

with col_photo:
    if s["foto_url"]:
        st.image(s["foto_url"], width=180)
    else:
        st.write("📷 Foto não disponível")

with col_info:
    st.title(s["nome_parlamentar"])
    st.caption(s["nome_completo"])

    c1, c2, c3 = st.columns(3)
    c1.metric("Partido", s["partido_sigla"] or "—")
    c2.metric("Estado (UF)", s["estado_sigla"] or "—")
    c3.metric("Sexo", s["sexo"] or "—")

    st.divider()

    c4, c5, c6 = st.columns(3)
    c4.metric("Início do mandato", str(s["mandato_inicio"]) if s["mandato_inicio"] else "—")
    c5.metric("Fim do mandato",    str(s["mandato_fim"])    if s["mandato_fim"]    else "—")
    c6.metric("Participação",      s["descricao_participacao"] or "—")

st.divider()

# ── Additional info ────────────────────────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Informações pessoais")
    st.write(f"**Nome completo:** {s['nome_completo'] or '—'}")
    st.write(f"**Data de nascimento:** {s['data_nascimento'] or '—'}")
    st.write(f"**Naturalidade:** {s['naturalidade'] or '—'} / {s['uf_naturalidade'] or '—'}")
    if s["email"]:
        st.write(f"**E-mail:** {s['email']}")
    if s["pagina_url"]:
        st.markdown(f"**Página oficial:** [{s['pagina_url']}]({s['pagina_url']})")

with col_b:
    st.subheader("Mandato atual")
    st.write(f"**Partido:** {s['partido_nome'] or s['partido_sigla'] or '—'}")
    st.write(f"**Legislatura de início:** {s['legislatura_inicio'] or '—'}")
    st.write(f"**Legislatura de fim:** {s['legislatura_fim'] or '—'}")
    em_exercicio = "✅ Em exercício" if s["em_exercicio"] else "⏹ Fora do exercício"
    st.write(f"**Status:** {em_exercicio}")

st.divider()
st.caption("Fonte: API de Dados Abertos do Senado Federal — legis.senado.leg.br/dadosabertos")
