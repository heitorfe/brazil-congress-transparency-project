import streamlit as st
import plotly.express as px
import polars as pl

from queries import (
    get_all_senators,
    get_senator_by_id,
    get_senator_votes,
    get_senator_vote_summary,
    get_senator_comissoes,
    get_senator_ceaps,
    get_senator_liderancas,
    get_senator_housing,
    get_senator_emendas_kpis,
    get_senator_emendas_por_ano,
    get_senator_emendas_favorecidos,
    get_senator_emendas_municipios,
    get_senator_apoiamentos,
)

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

# ── Header: Photo + Identity ───────────────────────────────────────────────
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

    # Reelection alert
    mandato_fim_ano = str(s["mandato_fim"])[:4] if s["mandato_fim"] else ""
    if mandato_fim_ano in ("2026", "2027"):
        st.warning(
            "🗳️ **Candidato(a) à reeleição nas eleições de 2026.** "
            "O mandato atual encerra em 2027."
        )

# ── Accountability Scorecard ───────────────────────────────────────────────
st.divider()
st.subheader("Ficha de Accountability")
st.caption("Indicadores para apoiar a decisão de voto na reeleição")

vote_summary = get_senator_vote_summary(senator_id)
ceaps_df     = get_senator_ceaps(senator_id)
comissoes_df = get_senator_comissoes(senator_id)
housing_df   = get_senator_housing(senator_id)

# Participation rate
if not vote_summary.is_empty():
    v = vote_summary.row(0, named=True)
    taxa = v["taxa_presenca"] or 0.0
    total_votes = v["total_votacoes"] or 0
else:
    taxa = 0.0
    total_votes = 0

# CEAPS total
ceaps_total = ceaps_df["total_reembolsado"].sum() if not ceaps_df.is_empty() else 0.0

# Committee count (current)
n_comissoes = len(comissoes_df.filter(pl.col("is_current") == True)) if not comissoes_df.is_empty() else 0

# Housing allowance
if not housing_df.is_empty():
    h = housing_df.row(0, named=True)
    housing_label = "Sim" if h["auxilio_moradia"] else "Não"
    imovel_label  = "Sim" if h["imovel_funcional"] else "Não"
else:
    housing_label = "Não informado"
    imovel_label  = "Não informado"

sc1, sc2, sc3, sc4, sc5 = st.columns(5)
sc1.metric(
    "Taxa de presença",
    f"{taxa}%",
    help=f"Baseado em {total_votes} votações registradas no plenário desde 2019",
)
sc2.metric(
    "Total CEAPS (todos os anos)",
    f"R$ {ceaps_total:,.0f}".replace(",", "."),
    help="Reembolsos de despesas do exercício parlamentar (CEAPS)",
)
sc3.metric(
    "Comissões atuais",
    n_comissoes,
    help="Número de comissões com participação ativa",
)
sc4.metric(
    "Auxílio-moradia",
    housing_label,
    help="Recebe auxílio-moradia do Senado",
)
sc5.metric(
    "Imóvel funcional",
    imovel_label,
    help="Utiliza apartamento funcional do Senado",
)

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────
tab_perfil, tab_votos, tab_comissoes, tab_despesas, tab_lideranca, tab_emendas = st.tabs([
    "👤 Perfil",
    "🗳️ Votações",
    "🏛️ Comissões",
    "💰 Despesas (CEAPS)",
    "⭐ Liderança",
    "📋 Emendas",
])

# ── Tab 1: Perfil ──────────────────────────────────────────────────────────
with tab_perfil:
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

# ── Tab 2: Votações ────────────────────────────────────────────────────────
with tab_votos:
    votes_df = get_senator_votes(senator_id)

    if votes_df.is_empty():
        st.info("Nenhuma votação registrada para este senador.")
    else:
        # Vote distribution chart
        if not vote_summary.is_empty():
            v = vote_summary.row(0, named=True)
            dist_data = {
                "Tipo de voto": ["Sim", "Não", "Abstenção", "Ausente"],
                "Quantidade": [
                    v["total_sim"] or 0,
                    v["total_nao"] or 0,
                    v["total_abstencao"] or 0,
                    v["total_ausente"] or 0,
                ],
            }
            import pandas as pd
            fig_dist = px.bar(
                pd.DataFrame(dist_data),
                x="Tipo de voto",
                y="Quantidade",
                color="Tipo de voto",
                color_discrete_map={
                    "Sim": "#2ecc71",
                    "Não": "#e74c3c",
                    "Abstenção": "#f39c12",
                    "Ausente": "#95a5a6",
                },
                title=f"Distribuição de votos — {v['total_votacoes']} votações registradas",
            )
            fig_dist.update_layout(showlegend=False, height=300, margin=dict(t=40, b=10))
            st.plotly_chart(fig_dist, use_container_width=True)

        # Votes table
        st.subheader("Últimas votações")
        vote_display = votes_df.select([
            "data_sessao",
            "materia_identificacao",
            "materia_ementa",
            "sigla_voto",
            "resultado_votacao",
        ]).rename({
            "data_sessao":          "Data",
            "materia_identificacao": "Matéria",
            "materia_ementa":        "Ementa",
            "sigla_voto":            "Voto",
            "resultado_votacao":     "Resultado",
        })
        st.dataframe(vote_display, use_container_width=True, hide_index=True)

# ── Tab 3: Comissões ───────────────────────────────────────────────────────
with tab_comissoes:
    if comissoes_df.is_empty():
        st.info("Nenhuma comissão registrada para este senador.")
    else:
        st.subheader("Comissões atuais")
        current = comissoes_df.filter(pl.col("is_current") == True)
        if not current.is_empty():
            st.dataframe(
                current.select([
                    "sigla_comissao", "nome_comissao", "sigla_casa",
                    "descricao_participacao", "data_inicio",
                ]).rename({
                    "sigla_comissao":        "Sigla",
                    "nome_comissao":         "Comissão",
                    "sigla_casa":            "Casa",
                    "descricao_participacao":"Cargo",
                    "data_inicio":           "Início",
                }),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Sem participação ativa em comissões no momento.")

        past = comissoes_df.filter(pl.col("is_current") == False)
        if not past.is_empty():
            with st.expander(f"Histórico de comissões ({len(past)} registros)"):
                st.dataframe(
                    past.select([
                        "sigla_comissao", "nome_comissao", "descricao_participacao",
                        "data_inicio", "data_fim",
                    ]).rename({
                        "sigla_comissao":        "Sigla",
                        "nome_comissao":         "Comissão",
                        "descricao_participacao":"Cargo",
                        "data_inicio":           "Início",
                        "data_fim":              "Fim",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )

# ── Tab 4: Despesas ────────────────────────────────────────────────────────
with tab_despesas:
    if ceaps_df.is_empty():
        st.info("Nenhuma despesa CEAPS registrada para este senador.")
    else:
        # Spending by year
        by_year = (
            ceaps_df.group_by("ano")
            .agg(pl.col("total_reembolsado").sum().alias("total"))
            .sort("ano")
        )
        fig_year = px.bar(
            by_year.with_columns(pl.col("ano").cast(pl.Utf8)).to_pandas(),
            x="ano",
            y="total",
            title="Total reembolsado por ano",
            labels={"ano": "Ano", "total": ""},
            color_discrete_sequence=["#c0392b"],
            text_auto=False,
        )
        fig_year.update_traces(
            texttemplate="R$ %{y:,.0f}",
            textposition="outside",
        )
        fig_year.update_layout(
            height=300,
            margin=dict(t=50, b=10),
            yaxis=dict(
                tickprefix="R$ ",
                tickformat=",.0f",
            ),
        )
        st.plotly_chart(fig_year, use_container_width=True)

        # Spending by category — all years summed (no year selection needed)
        by_cat = (
            ceaps_df
            .group_by("tipo_despesa")
            .agg(pl.col("total_reembolsado").sum().alias("total"))
            .sort("total", descending=False)
        )
        fig_cat = px.bar(
            by_cat.to_pandas(),
            x="total",
            y="tipo_despesa",
            orientation="h",
            title="Despesas por categoria (todos os anos)",
            labels={"total": "", "tipo_despesa": ""},
            color_discrete_sequence=["#e67e22"],
        )
        fig_cat.update_traces(
            texttemplate="R$ %{x:,.0f}",
            textposition="outside",
        )
        fig_cat.update_layout(
            height=max(250, len(by_cat) * 35),
            margin=dict(t=50, b=10, r=160),
            xaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
        )
        st.plotly_chart(fig_cat, use_container_width=True)

        # Raw expense table
        with st.expander("Tabela detalhada"):
            detail = ceaps_df.rename({
                "ano": "Ano", "mes": "Mês",
                "tipo_despesa": "Categoria",
                "qtd_recibos": "Recibos",
                "total_reembolsado": "Total (R$)",
            })
            st.dataframe(detail, use_container_width=True, hide_index=True)

# ── Tab 5: Liderança ───────────────────────────────────────────────────────
with tab_lideranca:
    lider_df = get_senator_liderancas(senator_id)

    if lider_df.is_empty():
        st.info("Nenhuma liderança partidária ou de governo registrada para este senador.")
    else:
        st.dataframe(
            lider_df.rename({
                "descricao_tipo_unidade":  "Tipo de unidade",
                "sigla_tipo_lideranca":    "Sigla",
                "descricao_tipo_lideranca":"Cargo",
                "sigla_partido":           "Partido",
                "nome_partido":            "Nome do partido",
                "data_designacao":         "Designação",
                "casa":                    "Casa",
            }),
            use_container_width=True,
            hide_index=True,
        )

# ── Tab 6: Emendas ────────────────────────────────────────────────────────
with tab_emendas:
    emendas_kpis = get_senator_emendas_kpis(senator_id)

    if emendas_kpis["num_emendas"] == 0:
        st.info(
            "Nenhuma emenda parlamentar registrada para este senador. "
            "Isso pode ocorrer porque o nome do senador não pôde ser vinculado "
            "aos dados do Portal da Transparência, ou porque o senador não possui "
            "emendas individuais no período coberto (2014–presente)."
        )
    else:
        # KPI cards
        e1, e2, e3, e4 = st.columns(4)
        e1.metric(
            "Emendas",
            f"{emendas_kpis['num_emendas']:,}".replace(",", "."),
            help="Número de emendas distintas com pagamento registrado",
        )
        e2.metric(
            "Total pago",
            f"R$ {float(emendas_kpis['total_pago']) / 1e6:,.1f}M".replace(",", "X").replace(".", ",").replace("X", "."),
            help="Valor efetivamente transferido ao beneficiário (fase Pagamento)",
        )
        e3.metric(
            "Municípios beneficiados",
            f"{emendas_kpis['municipios']:,}".replace(",", "."),
            help="Municípios distintos que receberam recursos",
        )
        e4.metric(
            "Período",
            f"{emendas_kpis['ano_min']} – {emendas_kpis['ano_max']}"
            if emendas_kpis["ano_min"] else "—",
        )

        st.divider()

        col_esq, col_dir = st.columns(2)

        # Annual trend
        with col_esq:
            anual_df = get_senator_emendas_por_ano(senator_id)
            if not anual_df.is_empty():
                anual_pd = anual_df.to_pandas()
                anual_pd["ano_str"] = anual_pd["ano_emenda"].astype(str)
                fig_em_ano = px.bar(
                    anual_pd,
                    x="ano_str",
                    y="total_pago",
                    title="Total pago por ano de emenda",
                    labels={"ano_str": "Ano", "total_pago": "Total pago (R$)"},
                    color_discrete_sequence=["#2c7bb6"],
                    text="total_pago",
                )
                fig_em_ano.update_traces(
                    texttemplate="R$ %{y:,.0f}",
                    textposition="outside",
                )
                fig_em_ano.update_layout(
                    yaxis_tickformat=",.0f",
                    height=300,
                    margin=dict(t=40, b=10),
                )
                st.plotly_chart(fig_em_ano, use_container_width=True)

        # Top beneficiaries
        with col_dir:
            fav_df = get_senator_emendas_favorecidos(senator_id, n=12)
            if not fav_df.is_empty():
                fig_fav = px.bar(
                    fav_df.to_pandas().sort_values("total_pago"),
                    x="total_pago",
                    y="favorecido",
                    orientation="h",
                    title="Maiores beneficiários (favorecidos)",
                    labels={"total_pago": "Total pago (R$)", "favorecido": ""},
                    color_discrete_sequence=["#e07b00"],
                    text="total_pago",
                    custom_data=["municipio_favorecido", "uf_favorecido", "tipo_favorecido"],
                )
                fig_fav.update_traces(
                    texttemplate="R$ %{x:,.0f}",
                    textposition="outside",
                    hovertemplate=(
                        "<b>%{y}</b><br>"
                        "Município: %{customdata[0]} / %{customdata[1]}<br>"
                        "Tipo: %{customdata[2]}<br>"
                        "Total pago: R$ %{x:,.0f}<extra></extra>"
                    ),
                )
                fig_fav.update_layout(
                    xaxis_tickformat=",.0f",
                    yaxis=dict(categoryorder="total ascending"),
                    height=300,
                    margin=dict(t=40, b=10, r=120),
                )
                st.plotly_chart(fig_fav, use_container_width=True)

        # Municipality table
        mun_df = get_senator_emendas_municipios(senator_id)
        if not mun_df.is_empty():
            with st.expander(f"🗺️ Municípios beneficiados ({len(mun_df)} municípios)"):
                mun_top = mun_df.head(50).select([
                    pl.col("municipio_recurso").alias("Município"),
                    pl.col("uf_recurso").alias("UF"),
                    pl.col("num_emendas").alias("Emendas"),
                    pl.col("total_pago").map_elements(
                        lambda v: f"R$ {v:,.0f}".replace(",", "."),
                        return_dtype=pl.Utf8,
                    ).alias("Total pago"),
                ])
                st.dataframe(mun_top, use_container_width=True, hide_index=True)

        # Co-sponsorships
        apoio_df = get_senator_apoiamentos(senator_id)
        if not apoio_df.is_empty():
            with st.expander(f"🤝 Apoiamentos a emendas de outros parlamentares ({len(apoio_df)} registros)"):
                apoio_display = apoio_df.select([
                    pl.col("ano_emenda").alias("Ano"),
                    pl.col("nome_autor_emenda").alias("Autor da emenda"),
                    pl.col("tipo_emenda").alias("Tipo"),
                    pl.col("favorecido").alias("Favorecido"),
                    pl.col("uf_favorecido").alias("UF"),
                    pl.col("orgao").alias("Órgão"),
                    pl.col("valor_pago").map_elements(
                        lambda v: f"R$ {v:,.0f}".replace(",", ".") if v else "—",
                        return_dtype=pl.Utf8,
                    ).alias("Valor pago"),
                ]).head(200)
                st.dataframe(apoio_display, use_container_width=True, hide_index=True)

    st.caption(
        "Fonte: Portal da Transparência (CGU) — "
        "emendas-parlamentares-documentos + apoiamento-emendas-parlamentares"
    )

st.divider()
st.caption("Fonte: API de Dados Abertos do Senado Federal — legis.senado.leg.br/dadosabertos")
