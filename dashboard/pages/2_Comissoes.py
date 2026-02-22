import streamlit as st
import plotly.express as px
import polars as pl

from queries import get_comissoes, get_comissao_membros

st.set_page_config(
    page_title="Comissões do Senado",
    page_icon="🏛️",
    layout="wide",
)

st.title("🏛️ Comissões do Senado Federal")
st.caption(
    "Comissões permanentes, CPIs e comissões mistas do Senado Federal (SF) "
    "e do Congresso Nacional (CN)."
)

# ── Load data ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_comissoes():
    return get_comissoes()

df = load_comissoes()

# ── Filters ────────────────────────────────────────────────────────────────
f1, f2 = st.columns(2)

tipos_disponiveis = ["Todos"] + sorted(df["descricao_tipo"].drop_nulls().unique().to_list())
casas_disponiveis = ["Todas"] + sorted(df["sigla_casa"].drop_nulls().unique().to_list())

sel_tipo = f1.selectbox("Tipo de comissão", tipos_disponiveis)
sel_casa = f2.selectbox("Casa legislativa", casas_disponiveis)

filtered = df
if sel_tipo != "Todos":
    filtered = filtered.filter(pl.col("descricao_tipo") == sel_tipo)
if sel_casa != "Todas":
    filtered = filtered.filter(pl.col("sigla_casa") == sel_casa)

# ── KPIs ───────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("Comissões ativas (filtro)", len(filtered))
k2.metric("Total de membros atuais",  int(filtered["num_membros_atuais"].sum()))
k3.metric("Titulares",                int(filtered["num_titulares"].sum()))
k4.metric("Suplentes",                int(filtered["num_suplentes"].sum()))

st.divider()

# ── Type distribution chart ────────────────────────────────────────────────
tipo_counts = (
    filtered.group_by("descricao_tipo")
    .agg(pl.len().alias("count"))
    .sort("count", descending=True)
)
fig_tipo = px.bar(
    tipo_counts.to_pandas(),
    x="descricao_tipo",
    y="count",
    color="descricao_tipo",
    labels={"descricao_tipo": "Tipo", "count": "Quantidade"},
    title="Distribuição por tipo de comissão",
    color_discrete_sequence=px.colors.qualitative.Set2,
)
fig_tipo.update_layout(showlegend=False, height=280, margin=dict(t=40, b=10))
st.plotly_chart(fig_tipo, use_container_width=True)

st.divider()

# ── Committee table with drill-down ───────────────────────────────────────
st.subheader("Lista de comissões")
st.caption("Clique em uma linha para ver os membros da comissão.")

display = filtered.select([
    "codigo_comissao",
    "sigla_comissao",
    "nome_comissao",
    "sigla_casa",
    "descricao_tipo",
    "num_membros_atuais",
    "num_titulares",
    "num_suplentes",
    "data_inicio",
]).rename({
    "codigo_comissao":   "Código",
    "sigla_comissao":    "Sigla",
    "nome_comissao":     "Nome",
    "sigla_casa":        "Casa",
    "descricao_tipo":    "Tipo",
    "num_membros_atuais":"Membros atuais",
    "num_titulares":     "Titulares",
    "num_suplentes":     "Suplentes",
    "data_inicio":       "Criada em",
})

selection = st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
)

# ── Member drill-down ──────────────────────────────────────────────────────
selected_rows = selection.selection.rows
if selected_rows:
    idx = selected_rows[0]
    row = filtered.row(idx, named=True)
    codigo = row["codigo_comissao"]
    sigla  = row["sigla_comissao"]
    nome   = row["nome_comissao"]

    st.divider()
    st.subheader(f"Membros da comissão: {sigla} — {nome}")

    @st.cache_data(ttl=3600)
    def load_membros(c):
        return get_comissao_membros(c)

    membros_df = load_membros(codigo)

    if membros_df.is_empty():
        st.info("Nenhum membro registrado para esta comissão.")
    else:
        n_titulares  = len(membros_df.filter(pl.col("cargo").str.contains("Titular")))
        n_suplentes  = len(membros_df.filter(pl.col("cargo").str.contains("Suplente")))
        n_presidentes = len(membros_df.filter(pl.col("cargo").str.contains("Presidente")))

        mb1, mb2, mb3 = st.columns(3)
        mb1.metric("Titulares",   n_titulares)
        mb2.metric("Suplentes",   n_suplentes)
        mb3.metric("Presidentes", n_presidentes)

        member_display = membros_df.select([
            "nome_parlamentar", "cargo", "partido_sigla", "estado_sigla", "data_inicio", "senador_id",
        ]).rename({
            "nome_parlamentar": "Nome",
            "cargo":            "Cargo",
            "partido_sigla":    "Partido",
            "estado_sigla":     "UF",
            "data_inicio":      "Início",
            "senador_id":       "ID",
        })

        member_sel = st.dataframe(
            member_display,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
        )

        member_rows = member_sel.selection.rows
        if member_rows:
            midx = member_rows[0]
            sid  = membros_df["senador_id"][midx]
            if sid:
                st.session_state["selected_senator_id"] = sid
                st.switch_page("pages/1_Perfil_do_Senador.py")

st.divider()
st.caption("Fonte: API de Dados Abertos do Senado Federal — legis.senado.leg.br/dadosabertos")
