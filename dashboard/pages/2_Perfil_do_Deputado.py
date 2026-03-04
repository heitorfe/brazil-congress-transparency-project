import streamlit as st
import plotly.express as px
import polars as pl
import pandas as pd

from queries import (
    get_all_deputies,
    get_deputy_by_id,
    get_deputy_vote_summary,
    get_deputy_votes,
    get_deputy_expenses,
    get_deputy_proposals,
    get_deputy_proposals_summary,
    get_deputy_emendas_kpis,
    get_deputy_emendas_por_ano,
    get_deputy_emendas_favorecidos,
    get_deputy_emendas_municipios,
    get_deputies_party_composition,
    get_deputy_emendas_kpis_by_name,
)

st.set_page_config(
    page_title="Perfil do Deputado",
    page_icon="🏛️",
    layout="wide",
)

# ── Deputy selection (sidebar) ─────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_all():
    return get_all_deputies()

all_deputies = load_all()
names = all_deputies.select(["deputado_id", "nome_parlamentar"]).sort("nome_parlamentar")
name_to_id = dict(zip(names["nome_parlamentar"].to_list(), names["deputado_id"].to_list()))

default_name = None
if "selected_deputy_id" in st.session_state:
    did = st.session_state["selected_deputy_id"]
    matches = all_deputies.filter(all_deputies["deputado_id"] == did)["nome_parlamentar"]
    if len(matches) > 0:
        default_name = matches[0]

selected_name = st.sidebar.selectbox(
    "Selecione um deputado",
    list(name_to_id.keys()),
    index=list(name_to_id.keys()).index(default_name) if default_name else 0,
)
deputy_id = name_to_id[selected_name]
st.session_state["selected_deputy_id"] = deputy_id

# ── Load selected deputy ────────────────────────────────────────────────────
row_df = get_deputy_by_id(str(deputy_id))
if row_df.is_empty():
    st.error("Deputado não encontrado.")
    st.stop()

d = row_df.row(0, named=True)

# ── Header: Photo + Identity ────────────────────────────────────────────────
col_photo, col_info = st.columns([1, 3])

with col_photo:
    if d["url_foto"]:
        st.image(d["url_foto"], width=180)
    else:
        st.write("📷 Foto não disponível")

with col_info:
    st.title(d["nome_parlamentar"])
    st.caption(d["nome_civil"])

    c1, c2, c3 = st.columns(3)
    c1.metric("Partido", d["sigla_partido"] or "—")
    c2.metric("Estado (UF)", d["sigla_uf"] or "—")
    c3.metric("Sexo", d["sexo"] or "—")

    st.divider()

    c4, c5, c6 = st.columns(3)
    c4.metric("Legislatura início", str(d["legislatura_min"]) if d["legislatura_min"] else "—")
    c5.metric("Legislatura fim", str(d["legislatura_max"]) if d["legislatura_max"] else "—")

    situacao_label = d["situacao"] or "—"
    c6.metric("Situação", situacao_label)

    em_exercicio = d["em_exercicio"]
    if em_exercicio:
        st.success("✅ Em exercício na legislatura atual (57ª)")
    else:
        st.warning("⏹ Não está em exercício na legislatura atual")

# ── Accountability Scorecard ────────────────────────────────────────────────
st.divider()
st.subheader("Ficha de Accountability")
st.caption("Indicadores de atividade parlamentar na Câmara dos Deputados (desde 2019)")

vote_summary = get_deputy_vote_summary(str(deputy_id))
expenses_df  = get_deputy_expenses(str(deputy_id))
emendas_kpis = get_deputy_emendas_kpis(str(deputy_id))

# Participation rate
if not vote_summary.is_empty():
    v = vote_summary.row(0, named=True)
    taxa = v["taxa_presenca"] or 0.0
    total_votes = v["total_votacoes"] or 0
else:
    taxa = 0.0
    total_votes = 0

# CEAP total
ceap_total = expenses_df["valor_liquido"].sum() if not expenses_df.is_empty() else 0.0

# Count proposals
proposals_df = get_deputy_proposals(str(deputy_id), n=10000)
n_proposals = len(proposals_df)

sc1, sc2, sc3, sc4 = st.columns(4)
sc1.metric(
    "Taxa de presença (votações)",
    f"{taxa}%",
    help=f"Baseado em {total_votes} votações registradas na Câmara desde 2019",
)
sc2.metric(
    "Total CEAP (todos os anos)",
    f"R$ {ceap_total:,.0f}".replace(",", "."),
    help="Reembolsos de despesas do exercício parlamentar (Cota para o Exercício da Atividade Parlamentar)",
)
sc3.metric(
    "Proposições apresentadas",
    f"{n_proposals:,}".replace(",", "."),
    help="Propostas legislativas de autoria do deputado (2019–2026)",
)
sc4.metric(
    "Emendas com pagamento",
    f"{emendas_kpis['num_emendas']:,}".replace(",", "."),
    help="Emendas individuais com recursos efetivamente pagos ao beneficiário",
)

st.divider()

# ── Tabs ────────────────────────────────────────────────────────────────────
tab_perfil, tab_votos, tab_despesas, tab_proposicoes, tab_emendas = st.tabs([
    "👤 Perfil",
    "🗳️ Votações",
    "💰 Despesas (CEAP)",
    "📄 Proposições",
    "📋 Emendas",
])

# ── Tab 1: Perfil ───────────────────────────────────────────────────────────
with tab_perfil:
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Informações pessoais")
        st.write(f"**Nome civil:** {d['nome_civil'] or '—'}")
        st.write(f"**Nome eleitoral:** {d['nome_eleitoral'] or '—'}")
        st.write(f"**Data de nascimento:** {d['data_nascimento'] or '—'}")
        st.write(f"**Naturalidade:** {d['municipio_nascimento'] or '—'} / {d['uf_nascimento'] or '—'}")
        st.write(f"**Escolaridade:** {d['escolaridade'] or '—'}")
        if d["email"]:
            st.write(f"**E-mail:** {d['email']}")
        if d["telefone_gabinete"]:
            st.write(f"**Telefone gabinete:** {d['telefone_gabinete']}")

    with col_b:
        st.subheader("Mandato")
        st.write(f"**Partido:** {d['sigla_partido'] or '—'}")
        st.write(f"**Estado:** {d['sigla_uf'] or '—'}")
        st.write(f"**Legislaturas:** {d['legislatura_min']} – {d['legislatura_max']}")
        multil = "Sim (56ª + 57ª)" if d.get("multi_legislatura") else "Não"
        st.write(f"**Reeleito (presente em duas legislaturas):** {multil}")
        st.write(f"**Condição eleitoral:** {d['condicao_eleitoral'] or '—'}")
        st.write(f"**Situação atual:** {d['situacao'] or '—'}")

    # Party context from national overview
    st.divider()
    st.subheader("Composição da Câmara por partido")

    @st.cache_data(ttl=3600)
    def load_party_comp():
        return get_deputies_party_composition()

    party_df = load_party_comp()
    if not party_df.is_empty():
        party_pd = party_df.to_pandas().head(20)
        # Highlight the current deputy's party using a color column
        party_pd["cor"] = party_pd["sigla_partido"].apply(
            lambda p: "#e74c3c" if p == d["sigla_partido"] else "#2c7bb6"
        )
        fig_party = px.bar(
            party_pd,
            x="sigla_partido",
            y="num_deputados",
            color="cor",
            color_discrete_map="identity",
            title=f"Deputados por partido (top 20) — {d['sigla_partido']} em destaque",
            labels={"sigla_partido": "Partido", "num_deputados": "Deputados"},
            height=320,
        )
        fig_party.update_layout(
            margin=dict(t=40, b=10),
            showlegend=False,
        )
        st.plotly_chart(fig_party, use_container_width=True)

# ── Tab 2: Votações ─────────────────────────────────────────────────────────
with tab_votos:
    votes_df = get_deputy_votes(str(deputy_id))

    if votes_df.is_empty():
        st.info("Nenhuma votação registrada para este deputado.")
    else:
        if not vote_summary.is_empty():
            v = vote_summary.row(0, named=True)
            taxa_val   = float(v["taxa_presenca"] or 0)
            total_vot  = int(v["total_votacoes"] or 0)
            ausentes   = total_vot - int((v["total_sim"] or 0) + (v["total_nao"] or 0) + (v["total_abstencao"] or 0))

            pc1, pc2, pc3 = st.columns(3)
            pc1.metric(
                "Taxa de presença",
                f"{taxa_val:.1f}%",
                help=f"Presença ativa (Sim / Não / Abstenção) em {total_vot} votações nominais desde 2019",
            )
            pc2.metric("Ausências estimadas", f"{max(ausentes, 0):,}".replace(",", "."))
            pc3.metric("Total de votações", f"{total_vot:,}".replace(",", "."))

            cor = "#2ecc71" if taxa_val >= 75 else ("#f39c12" if taxa_val >= 50 else "#e74c3c")
            st.markdown(
                f"""
                <div style="background:#eee;border-radius:4px;height:14px;width:100%">
                  <div style="background:{cor};border-radius:4px;height:14px;width:{taxa_val:.1f}%"></div>
                </div>
                <small style="color:#666">{taxa_val:.1f}% de presença ativa</small>
                """,
                unsafe_allow_html=True,
            )
            st.write("")

        # Votes table
        st.subheader("Últimas votações")
        vote_display = votes_df.select([
            "data_votacao",
            "proposicao_objeto",
            "descricao",
            "tipo_voto",
            "aprovacao",
            "partido_sigla_voto",
        ]).rename({
            "data_votacao":      "Data",
            "proposicao_objeto": "Proposição",
            "descricao":         "Descrição",
            "tipo_voto":         "Voto",
            "aprovacao":         "Aprovado?",
            "partido_sigla_voto":"Partido (na votação)",
        })
        st.dataframe(vote_display, use_container_width=True, hide_index=True)

    st.caption("Fonte: API de Dados Abertos da Câmara dos Deputados — dadosabertos.camara.leg.br")

# ── Tab 3: Despesas (CEAP) ──────────────────────────────────────────────────
with tab_despesas:
    if expenses_df.is_empty():
        st.info("Nenhuma despesa CEAP registrada para este deputado.")
    else:
        # Top-line totals only
        n_docs   = len(expenses_df)
        total_liq = expenses_df["valor_liquido"].sum()
        total_glosa = expenses_df["valor_glosa"].sum()
        anos_range = f"{expenses_df['ano'].min()} – {expenses_df['ano'].max()}"

        kd1, kd2, kd3, kd4 = st.columns(4)
        kd1.metric("Documentos", f"{n_docs:,}".replace(",", "."))
        kd2.metric(
            "Total reembolsado",
            f"R$ {total_liq:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            help="Soma de valor_liquido — após glosas",
        )
        kd3.metric(
            "Total glosado",
            f"R$ {total_glosa:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            help="Valores negados / devolvidos pelo sistema",
        )
        kd4.metric("Período", anos_range)

        # Expense summary charts (in-memory, no new query needed)
        exp_c1, exp_c2 = st.columns(2)

        with exp_c1:
            by_year_exp = (
                expenses_df.group_by("ano")
                .agg(pl.col("valor_liquido").sum().alias("total"))
                .sort("ano")
            )
            fig_exp_year = px.bar(
                by_year_exp.with_columns(pl.col("ano").cast(pl.Utf8)).to_pandas(),
                x="ano", y="total",
                title="Total CEAP por ano",
                labels={"ano": "Ano", "total": "Total (R$)"},
                color_discrete_sequence=["#2c7bb6"],
                text="total",
            )
            fig_exp_year.update_traces(texttemplate="R$ %{y:,.0f}", textposition="outside")
            fig_exp_year.update_layout(
                height=300, margin=dict(t=50, b=10),
                yaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
            )
            st.plotly_chart(fig_exp_year, use_container_width=True)

        with exp_c2:
            by_cat_exp = (
                expenses_df.group_by("tipo_despesa")
                .agg(pl.col("valor_liquido").sum().alias("total"))
                .sort("total")
                .tail(8)
            )
            fig_exp_cat = px.bar(
                by_cat_exp.to_pandas(),
                x="total", y="tipo_despesa", orientation="h",
                title="Top 8 categorias",
                labels={"total": "Total (R$)", "tipo_despesa": ""},
                color_discrete_sequence=["#e67e22"],
            )
            fig_exp_cat.update_layout(
                height=300, margin=dict(t=50, b=10, r=140),
                xaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
            )
            st.plotly_chart(fig_exp_cat, use_container_width=True)

        st.divider()

        # ── Filters ────────────────────────────────────────────────────────
        anos_disp = sorted(expenses_df["ano"].drop_nulls().unique().to_list(), reverse=True)
        tipos_disp = sorted(expenses_df["tipo_despesa"].drop_nulls().unique().to_list())

        fc1, fc2, fc3 = st.columns([1, 2, 2])
        with fc1:
            ano_sel = st.selectbox("Ano", ["Todos"] + [str(a) for a in anos_disp], key="exp_ano")
        with fc2:
            tipo_sel = st.selectbox("Categoria", ["Todas"] + tipos_disp, key="exp_tipo")
        with fc3:
            fornecedor_search = st.text_input("Buscar fornecedor / CNPJ", key="exp_forn",
                                              placeholder="ex: Latam, 04.902.979...")

        filtered_exp = expenses_df
        if ano_sel != "Todos":
            filtered_exp = filtered_exp.filter(pl.col("ano") == int(ano_sel))
        if tipo_sel != "Todas":
            filtered_exp = filtered_exp.filter(pl.col("tipo_despesa") == tipo_sel)
        if fornecedor_search:
            term = fornecedor_search.upper()
            filtered_exp = filtered_exp.filter(
                pl.col("nome_fornecedor").str.to_uppercase().str.contains(term)
                | pl.col("cnpj_cpf_fornecedor").str.to_uppercase().str.contains(term)
            )

        n_filtered = len(filtered_exp)
        total_filtered = filtered_exp["valor_liquido"].sum()
        total_fmt = f"{total_filtered:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        st.caption(f"**{n_filtered:,}** documentos · Total filtrado: **R$ {total_fmt}**")

        # ── Detail table ────────────────────────────────────────────────────
        display_exp = filtered_exp.select([
            pl.col("data_documento").alias("Data"),
            pl.col("tipo_despesa").alias("Categoria"),
            pl.col("tipo_documento").alias("Tipo doc."),
            pl.col("num_documento").cast(pl.Utf8).alias("NF / Recibo"),
            pl.col("nome_fornecedor").alias("Fornecedor"),
            pl.col("cnpj_cpf_fornecedor").alias("CNPJ / CPF"),
            pl.col("valor_documento").alias("Valor bruto (R$)"),
            pl.col("valor_glosa").alias("Glosa (R$)"),
            pl.col("valor_liquido").alias("Valor líquido (R$)"),
            pl.col("num_ressarcimento").cast(pl.Utf8).alias("Ressarcimento"),
            pl.col("url_documento").alias("Link NF"),
        ])

        st.dataframe(
            display_exp,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Link NF": st.column_config.LinkColumn("Link NF", display_text="🔗 Ver doc."),
                "Valor bruto (R$)": st.column_config.NumberColumn(format="R$ %.2f"),
                "Glosa (R$)":       st.column_config.NumberColumn(format="R$ %.2f"),
                "Valor líquido (R$)": st.column_config.NumberColumn(format="R$ %.2f"),
            },
        )

    st.caption("Fonte: API de Dados Abertos da Câmara dos Deputados — dadosabertos.camara.leg.br")

# ── Tab 4: Proposições ──────────────────────────────────────────────────────
with tab_proposicoes:
    if proposals_df.is_empty():
        st.info(
            "Nenhuma proposição registrada para este deputado no período 2019–2026. "
            "Deputados que ingressaram na 56ª legislatura (2019) em data posterior ou que "
            "não apresentaram propostas próprias não terão registros."
        )
    else:
        total_props = len(proposals_df)

        # KPI
        p1, p2, p3 = st.columns(3)
        p1.metric("Total de proposições", f"{total_props:,}".replace(",", "."))

        by_tipo = (
            proposals_df.group_by("sigla_tipo")
            .agg(pl.len().alias("n"))
            .sort("n", descending=True)
        )
        top_tipo = by_tipo.row(0, named=True) if not by_tipo.is_empty() else {}
        p2.metric("Tipo mais frequente", top_tipo.get("sigla_tipo", "—"),
                  help="Tipo de proposição mais apresentado pelo deputado")

        anos_ativos = proposals_df["ano"].n_unique()
        p3.metric("Anos com proposições", anos_ativos)

        # Proposals by type × year (stacked)
        prop_summary = get_deputy_proposals_summary(str(deputy_id))
        if not prop_summary.is_empty():
            # Keep only top-8 types, group rest as "Outros"
            top_tipos = (
                prop_summary.group_by("sigla_tipo")
                .agg(pl.col("num_proposicoes").sum())
                .sort("num_proposicoes", descending=True)
                .head(8)["sigla_tipo"]
                .to_list()
            )
            prop_summary = prop_summary.with_columns(
                pl.when(pl.col("sigla_tipo").is_in(top_tipos))
                .then(pl.col("sigla_tipo"))
                .otherwise(pl.lit("Outros"))
                .alias("tipo_agrupado")
            )
            prop_agg = (
                prop_summary.group_by(["ano", "tipo_agrupado"])
                .agg(pl.col("num_proposicoes").sum())
                .sort("ano")
            )

            fig_props = px.bar(
                prop_agg.with_columns(pl.col("ano").cast(pl.Utf8)).to_pandas(),
                x="ano",
                y="num_proposicoes",
                color="tipo_agrupado",
                title="Proposições por ano e tipo",
                labels={"ano": "Ano", "num_proposicoes": "Proposições", "tipo_agrupado": "Tipo"},
                barmode="stack",
                height=340,
            )
            fig_props.update_layout(margin=dict(t=40, b=10), legend_title="Tipo")
            st.plotly_chart(fig_props, use_container_width=True)

        # Searchable proposals table
        st.subheader("Lista de proposições")
        col_f1, col_f2 = st.columns([2, 1])
        with col_f1:
            search_term = st.text_input("Buscar na ementa", placeholder="ex: educação, saúde...")
        with col_f2:
            all_tipos = ["Todos"] + sorted(proposals_df["sigla_tipo"].drop_nulls().unique().to_list())
            tipo_filter = st.selectbox("Tipo", all_tipos, key="prop_tipo_filter")

        filtered = proposals_df
        if search_term:
            filtered = filtered.filter(
                pl.col("ementa").str.contains(search_term, literal=False)
            )
        if tipo_filter != "Todos":
            filtered = filtered.filter(pl.col("sigla_tipo") == tipo_filter)

        st.caption(f"Exibindo {len(filtered):,} de {total_props:,} proposições".replace(",", "."))

        display_props = filtered.select([
            pl.col("ano").alias("Ano"),
            pl.col("sigla_tipo").alias("Tipo"),
            pl.col("numero").alias("Número"),
            pl.col("data_apresentacao").alias("Apresentação"),
            pl.col("ementa").alias("Ementa"),
            pl.col("descricao_situacao").alias("Situação"),
            pl.col("url_inteiro_teor").alias("Link"),
        ]).head(300)
        st.dataframe(display_props, use_container_width=True, hide_index=True)
        if len(filtered) > 300:
            st.caption("Exibindo os 300 primeiros resultados. Use os filtros para refinar.")

    st.caption("Fonte: API de Dados Abertos da Câmara dos Deputados — dadosabertos.camara.leg.br")

# ── Tab 5: Emendas ──────────────────────────────────────────────────────────
with tab_emendas:
    # Try fallback by name if ID-based lookup returns nothing
    _emendas_kpis_used = emendas_kpis
    _fallback_used = False
    if emendas_kpis["num_emendas"] == 0:
        fallback = get_deputy_emendas_kpis_by_name(d["nome_parlamentar"])
        if fallback["num_emendas"] > 0:
            _emendas_kpis_used = fallback
            _fallback_used = True

    if _emendas_kpis_used["num_emendas"] == 0:
        st.info(
            "Nenhuma emenda parlamentar encontrada para este deputado. "
            "Possíveis razões: \n\n"
            "- O nome parlamentar não coincide exatamente com o Portal da Transparência (CGU)\n"
            "- O deputado não possui emendas individuais no período coberto (2014–presente)\n"
            "- As emendas existem mas estão apenas na fase de Empenho (sem Pagamento registrado)\n\n"
            "Deputados da 56ª legislatura (2019–2023) que não foram reeleitos para a 57ª podem "
            "não ter vínculo completo estabelecido."
        )
    else:
        if _fallback_used:
            st.caption(
                "ℹ️ Emendas encontradas por correspondência de nome parlamentar "
                "(vínculo via Portal da Transparência, não por ID da Câmara)."
            )
        # KPI cards
        e1, e2, e3, e4 = st.columns(4)
        e1.metric(
            "Emendas",
            f"{_emendas_kpis_used['num_emendas']:,}".replace(",", "."),
            help="Número de emendas distintas com pagamento registrado",
        )
        e2.metric(
            "Total pago",
            f"R$ {float(_emendas_kpis_used['total_pago']) / 1e6:,.1f}M"
            .replace(",", "X").replace(".", ",").replace("X", "."),
            help="Valor efetivamente transferido ao beneficiário (fase Pagamento)",
        )
        e3.metric(
            "Municípios beneficiados",
            f"{_emendas_kpis_used['municipios']:,}".replace(",", "."),
            help="Municípios distintos que receberam recursos",
        )
        e4.metric(
            "Período",
            f"{_emendas_kpis_used['ano_min']} – {_emendas_kpis_used['ano_max']}"
            if _emendas_kpis_used["ano_min"] else "—",
        )

        st.divider()

        col_esq, col_dir = st.columns(2)

        # Annual trend
        with col_esq:
            anual_df = get_deputy_emendas_por_ano(str(deputy_id))
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
            fav_df = get_deputy_emendas_favorecidos(str(deputy_id), n=12)
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
        mun_df = get_deputy_emendas_municipios(str(deputy_id))
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

    st.caption(
        "Fonte: Portal da Transparência (CGU) — "
        "emendas-parlamentares-documentos | Vinculação via nome parlamentar normalizado"
    )

st.divider()

# ── Guia Cívico ─────────────────────────────────────────────────────────────
st.header("📚 Guia Cívico — Como funciona a Câmara dos Deputados")
st.caption(
    "Informações sobre o papel do deputado federal, o funcionamento das despesas, votações, "
    "proposições e emendas parlamentares."
)

with st.expander("🏛️ O papel do Deputado Federal"):
    st.markdown("""
### O que é um Deputado Federal?

O **Deputado Federal** é o representante do povo brasileiro no Congresso Nacional. A Câmara dos
Deputados é composta por **513 deputados**, eleitos a cada **4 anos** pelo sistema proporcional de
lista aberta — ou seja, o número de vagas por estado é proporcional à sua população, com mínimo
de **8** e máximo de **70** cadeiras por estado.

### Funções e poderes

- **Legislar**: propor, discutir e votar leis ordinárias, complementares e emendas constitucionais
- **Fiscalizar**: exercer controle sobre o Poder Executivo, incluindo as contas do Presidente da República
- **Orçamento**: propor emendas ao Orçamento da União (LOA) e autorizar créditos adicionais
- **Autorizar processos**: a Câmara decide, por 2/3 dos votos, se autoriza a abertura de processo
  contra o Presidente e Vice-Presidente da República

### Poderes exclusivos da Câmara

| Matéria | Regra |
|---|---|
| Processo contra Presidente/Vice | 2/3 dos votos (342 deputados) |
| Auditar contas presidenciais | Quando não apresentadas no prazo constitucional |
| Eleger membros do Conselho da República | Conforme previsão legal |

### Câmara vs Senado

| | Câmara dos Deputados | Senado Federal |
|---|---|---|
| **Representa** | O povo | Os estados |
| **Tamanho** | 513 membros | 81 membros (3 por estado) |
| **Mandato** | 4 anos | 8 anos |
| **Eleição** | Proporcional, lista aberta | Majoritária (primeiro turno único) |
| **Inicia PECs** | Deputados podem propor | Senadores podem propor |
| **Revisão de leis** | Revisa projetos do Senado | Revisa projetos da Câmara |

A maioria dos projetos de lei é iniciada na Câmara e revisada pelo Senado. Quando uma casa
aprova emendas ao texto da outra, o projeto retorna para nova votação. Medidas Provisórias (MPVs)
são analisadas por uma comissão mista (deputados + senadores) antes da votação em plenário.
""")

with st.expander("💰 CEAP — Cota para o Exercício da Atividade Parlamentar"):
    st.markdown("""
### O que é a CEAP?

A **CEAP** (Cota para o Exercício da Atividade Parlamentar) é uma verba mensal de reembolso de
despesas dos deputados federais, destinada exclusivamente ao exercício do mandato. **Não é
salário** — é ressarcimento de gastos comprovados com nota fiscal. Os valores não utilizados em
um mês podem ser acumulados.

### Limite mensal (2026)

O valor varia de acordo com o estado de origem do deputado, para compensar as diferenças de
custo de deslocamento até Brasília:

| Faixa | Estados | Limite mensal |
|---|---|---|
| Mais alto | AM, RR, AP, PA (distância maior) | ~R$ 57.000 |
| Intermediário | Demais estados | R$ 41.000 – R$ 52.000 |
| Mais baixo | MG, SP, RJ, ES, GO, DF | ~R$ 41.000 |

O limite foi reajustado em **13,7%** em 2026 em relação ao ano anterior.

### Categorias permitidas

| Categoria | Exemplos |
|---|---|
| Passagens aéreas | Voos domésticos para o mandato |
| Telefonia | Celular, telefone do escritório |
| Serviços postais | Correspondências oficiais |
| Locação de escritório | Escritório de apoio parlamentar no estado |
| Alimentação | Refeições em serviço |
| Hospedagem | Hotéis em viagens a trabalho |
| Combustíveis e lubrificantes | Uso no exercício do mandato |
| Transporte | Aluguel de veículos, táxi, pedágio, estacionamento |
| Segurança | Serviços contratados para proteção pessoal |
| Consultorias | Serviços técnicos de apoio ao mandato |
| Participação em eventos | Cursos, congressos, palestras |
| Divulgação parlamentar | Comunicação com eleitores (vedada nos 120 dias pré-eleição) |

### O que NÃO é permitido

- Compras pessoais (roupas, móveis, eletrônicos de uso pessoal)
- Despesas de familiares sem relação com o mandato
- Serviços de empresas com irregularidades fiscais
- Qualquer despesa sem nota fiscal válida

### Glosa

**Glosa** é a parte do valor solicitado que é **negada** pelo sistema de controle da Câmara.
Pode ocorrer por nota fiscal inválida, fornecedor irregular ou categoria não permitida.
O valor glosado aparece na coluna "Glosa (R$)" da tabela de despesas. O deputado pode
recorrer da glosa, mas o valor glosado não é reembolsado até a resolução.

### Ressarcimento ao erário

Quando identificado uso indevido, o deputado deve ressarcir o valor à Câmara. O número de
ressarcimento (campo "Ressarcimento" na tabela) indica que houve devolução.

### Transparência e fiscalização

Todos os documentos de despesa são públicos e acessíveis na [API de Dados Abertos da Câmara](https://dadosabertos.camara.leg.br/).
A NF (Nota Fiscal) ou recibo pode ser verificado diretamente pelo link na tabela desta página.
O CNPJ do fornecedor pode ser consultado no [portal da Receita Federal](https://solucoes.receita.fazenda.gov.br/Servicos/cnpjreva/Cnpjreva_Solicitacao.asp).
""")

with st.expander("🗳️ Como funcionam as votações na Câmara"):
    st.markdown("""
### Tipos de votação

#### 1. Votação Simbólica

A forma mais comum. O presidente da sessão convida os que são **a favor** a permanecerem
sentados e os **contrários** a se manifestarem (em geral levantando-se ou levantando a mão).
Não gera registro individual por deputado — apenas o resultado (aprovado/rejeitado) é registrado.
Usada quando há consenso antecipado entre os líderes partidários.

#### 2. Votação Nominal

Votação **eletrônica e pública** — cada deputado vota individualmente pelo painel eletrônico
e o voto é registrado nominalmente (SIM / NÃO / ABSTENÇÃO / OBSTRUÇÃO).
Usada obrigatoriamente para:
- Proposta de Emenda à Constituição (PEC)
- Medidas Provisórias (MPV)
- Projetos que exijam quórum especial
- Quando qualquer deputado solicita "verificação de votação simbólica"

#### 3. Votação Secreta

Sistema eletrônico que preserva o anonimato do voto — o resultado final é público, mas não
os votos individuais. Usada principalmente para eleições internas (presidente, mesa diretora).

### Orientação de bancada

Antes de cada votação nominal, os **líderes de bancada** registram a **orientação** do partido
(Sim / Não / Liberado / Obstru.). Na votação simbólica, o voto do líder representa todos os
membros do partido presentes. Na nominal, é apenas uma indicação — o deputado pode votar
diferentemente (chamado de "voto divergente").

### Quóruns especiais

| Tipo de deliberação | Quórum mínimo |
|---|---|
| Votação ordinária | Maioria dos presentes (com pelo menos 257 deputados em plenário) |
| Lei complementar | Maioria absoluta (257 votos) |
| Emenda Constitucional (PEC) | 3/5 dos deputados = **308 votos**, em dois turnos |
| Cassação de mandato | 2/3 dos deputados = **342 votos** |

### Taxa de presença

A taxa de presença nesta página é calculada como a proporção de votações em que o deputado
registrou SIM, NÃO ou ABSTENÇÃO (presença ativa) sobre o total de votações realizadas desde
2019. **Ausência** significa que o deputado não registrou voto algum, o que pode ocorrer por
ausência justificada (licença, missão oficial, doença) ou injustificada.
""")

with st.expander("📄 Como funcionam as proposições legislativas"):
    st.markdown("""
### O que é uma proposição?

Toda matéria submetida à deliberação da Câmara dos Deputados é chamada de **proposição**.
Um deputado pode apresentar proposições individualmente ou em conjunto com outros parlamentares.
As proposições ficam registradas no sistema SILEG e são numeradas por tipo e ano.

### Principais tipos

| Sigla | Nome completo | Descrição |
|---|---|---|
| **PL** | Projeto de Lei | Proposta de lei ordinária — o tipo mais comum |
| **PLP** | Projeto de Lei Complementar | Proposta de lei complementar (quórum especial: maioria absoluta) |
| **PEC** | Proposta de Emenda Constitucional | Altera a Constituição Federal — quórum de 3/5 em dois turnos |
| **PDL** | Projeto de Decreto Legislativo | Disciplina relações entre Legislativo e Executivo/Judiciário |
| **PRC** | Projeto de Resolução da Câmara | Regula matéria de competência exclusiva da Câmara |
| **MPV** | Medida Provisória | Proposta pelo Presidente da República com força de lei imediata |
| **REQ** | Requerimento | Pedido formal (de informação, de criação de comissão, etc.) |
| **INC** | Indicação | Sugestão ao Executivo ou a outro poder |
| **EMC** | Emenda em Comissão | Alteração proposta por comissão a outro projeto |

### Ciclo de vida de um Projeto de Lei (PL)

```
Deputado apresenta o PL
         ↓
Distribuição às Comissões temáticas
(ex: Comissão de Saúde, de Educação, de Finanças e Tributação)
         ↓
Análise e votação em cada Comissão
(aprovado → segue; rejeitado → arquivado, salvo recurso ao Plenário)
         ↓
Votação em Plenário (se necessário ou se houver recurso)
         ↓
Aprovado pela Câmara → enviado ao Senado
         ↓
Senado vota (pode emendar, aprovar ou rejeitar)
    ├── Se o Senado emendar → volta à Câmara para votar as emendas
    └── Se aprovar sem emendas → vai à sanção presidencial
         ↓
Presidente sanciona (vira Lei) ou veta
    └── Veto pode ser derrubado pelo Congresso (maioria absoluta nas duas casas)
```

### O que acontece com a maioria dos projetos?

Segundo dados históricos da Câmara, a grande maioria das proposições é **arquivada** ao fim
da legislatura sem ter sido votada. Apenas uma fração chega ao plenário. Por isso, o número
de proposições apresentadas não deve ser confundido com efetividade legislativa — o que importa
é quantas foram **aprovadas** e **sancionadas**.

### Regimes de tramitação

| Regime | Significado |
|---|---|
| **Urgência** | Votação prioritária, com prazo encurtado |
| **Prioridade** | Antecede matérias em regime ordinário |
| **Ordinário** | Tramitação normal, sem prazo fixo |

### Como ler a tabela de proposições nesta página

- **Tipo**: Sigla da proposição (PL, PEC, REQ, etc.)
- **Número**: Identificador sequencial dentro do ano
- **Situação**: Fase atual no ciclo (Em tramitação / Aprovado / Arquivado / Vetado)
- **Link**: Texto completo da proposição no portal da Câmara
""")

with st.expander("📋 Emendas Parlamentares — como funcionam"):
    st.markdown("""
### O que são emendas parlamentares?

**Emendas parlamentares** são o mecanismo pelo qual deputados e senadores indicam como parte
dos recursos do Orçamento da União deve ser aplicada. Após aprovação, os recursos são
transferidos para estados, municípios, entidades públicas ou ONGs — os chamados **favorecidos**.

### Tipos de emendas

| Tipo | Quem propõe | Execução obrigatória? | Limite anual |
|---|---|---|---|
| **Individual** | Cada parlamentar individualmente | Sim (desde 2015) | 2% da RCL* |
| **De Bancada** | Bancada estadual (grupo do mesmo estado) | Sim (desde 2019) | 1% da RCL |
| **De Comissão** | Comissões temáticas do Congresso | Não | — |
| **Do Relator (RP9)** | Relator-geral do orçamento | Declarada inconstitucional pelo STF em 2022 | — |

*RCL = Receita Corrente Líquida da União. Em 2024, o limite das emendas individuais foi de
aproximadamente R$ 15,5 milhões por parlamentar por ano.

### Fases de execução

As emendas passam por três fases no SIAFI (Sistema de Administração Financeira):

| Fase | O que significa |
|---|---|
| **Empenho** | O governo reserva formalmente os recursos — compromisso contábil |
| **Liquidação** | Verificação de que a obra ou serviço foi entregue conforme contratado |
| **Pagamento** | Transferência efetiva do dinheiro ao favorecido (beneficiário) |

Os dados desta página usam a fase **Pagamento** para calcular os valores efetivamente
transferidos — que é a medida mais fidedigna do que realmente chegou ao beneficiário.

### O "Orçamento Secreto" (RP9 — 2020 a 2022)

Entre 2020 e 2022, as **Emendas do Relator (RP9)** permitiram que parlamentares indicassem
recursos sem que seu nome ficasse vinculado publicamente às emendas. Ficou conhecido como
**"orçamento secreto"**. O volume chegou a R$ 24,8 bilhões em 2020. O STF declarou o
mecanismo inconstitucional em dezembro de 2022, mas os recursos já transferidos não foram
devolvidos.

### Transparência e rastreabilidade

Todos os dados de emendas são publicados no [Portal da Transparência do Governo Federal (CGU)](https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares).
A vinculação entre o nome do parlamentar nos dados da CGU e o perfil do deputado nesta
página é feita por **normalização de nome** (remoção de acentos, maiúsculas), o que pode
gerar falhas para deputados com nomes muito comuns ou grafias divergentes entre sistemas.

### Por que alguns deputados não aparecem com emendas?

1. O nome no sistema da CGU não coincide exatamente com o nome parlamentar registrado na Câmara
2. O deputado não apresentou emendas individuais no período coberto (2014–presente)
3. As emendas existem mas ainda estão na fase de empenho ou liquidação (sem pagamento registrado)
""")

with st.expander("🔍 Fiscalização e controle — como o cidadão pode monitorar"):
    st.markdown("""
### Quem fiscaliza os deputados?

#### TCU — Tribunal de Contas da União

O **TCU** é o órgão de controle externo do Congresso Nacional. Audita a aplicação dos
recursos federais e pode:
- Aplicar multas e determinar ressarcimento de valores desviados
- Inabilitar gestores para cargos públicos
- Determinar a sustação de contratos irregulares
- Emitir pareceres sobre as contas do Presidente da República

#### CGU — Controladoria-Geral da União

A **CGU** é o órgão de controle interno do Poder Executivo. Publica os dados do Portal da
Transparência e investiga irregularidades na execução de emendas parlamentares e convênios.

#### Portal da Transparência

O [Portal da Transparência](https://portaldatransparencia.gov.br/) é a principal ferramenta
de acesso público aos dados do governo federal, incluindo:
- Despesas orçamentárias (por programa, órgão, favorecido)
- Emendas parlamentares com nome do parlamentar e favorecido
- Remuneração de servidores públicos federais
- Contratos e licitações

#### CPIs — Comissões Parlamentares de Inquérito

O próprio Congresso pode criar **CPIs** para investigar fatos determinados de interesse público.
Uma CPI tem poderes de investigação próprios de autoridades judiciais (pode convocar
depoimentos, requerer documentos, decretar quebra de sigilo com autorização judicial).

### Como você pode fiscalizar

| Ação | Como fazer |
|---|---|
| Verificar a NF de uma despesa CEAP | Clique no link "🔗 Ver doc." na tabela de despesas desta página |
| Consultar o CNPJ de um fornecedor | [Receita Federal — consulta CNPJ](https://solucoes.receita.fazenda.gov.br/Servicos/cnpjreva/) |
| Ver emendas de todos os parlamentares | [Portal da Transparência — emendas](https://portaldatransparencia.gov.br/emendas-parlamentares) |
| Acompanhar projetos de lei | [Portal da Câmara — proposições](https://www.camara.leg.br/busca-portal/proposicoes/pesquisa-simplificada) |
| Ver votações do plenário | [Portal da Câmara — votações](https://www.camara.leg.br/votacoes) |
| Acesso aos dados abertos | [API da Câmara](https://dadosabertos.camara.leg.br/) |
""")

st.divider()
st.caption("Fonte principal: API de Dados Abertos da Câmara dos Deputados — dadosabertos.camara.leg.br")
