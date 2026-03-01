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
            "🗳️ **Possível candidato(a) à reeleição em 2026.** "
            "O mandato atual encerra em 2027 — candidatura não confirmada."
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
        # Presence rate visual (replaces the meaningless Sim/Não distribution chart)
        if not vote_summary.is_empty():
            v = vote_summary.row(0, named=True)
            taxa_val = float(v["taxa_presenca"] or 0)
            total_vot = int(v["total_votacoes"] or 0)
            ausentes = int(v["total_ausente"] or 0)

            pc1, pc2, pc3 = st.columns(3)
            pc1.metric(
                "Taxa de presença",
                f"{taxa_val:.1f}%",
                help=f"Presença ativa (Sim / Não / Abstenção) em {total_vot} votações nominais desde 2019",
            )
            pc2.metric("Ausências registradas", f"{ausentes:,}".replace(",", "."))
            pc3.metric("Total de votações", f"{total_vot:,}".replace(",", "."))

            # Color-coded progress bar
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

        # Spending by category — all years summed
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
            height=max(250, len(by_cat) * 38),
            margin=dict(t=50, b=10, r=160),
            xaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
        )
        st.plotly_chart(fig_cat, use_container_width=True)

        # Monthly breakdown toggle
        ver_mensal = st.toggle("Ver evolução mensal", key="ceaps_mensal_toggle")
        if ver_mensal:
            anos_disp = sorted(ceaps_df["ano"].drop_nulls().unique().to_list(), reverse=True)
            ano_sel = st.selectbox("Ano", anos_disp, key="ceaps_ano_sel")
            mensal = (
                ceaps_df
                .filter(pl.col("ano") == ano_sel)
                .group_by("mes")
                .agg(pl.col("total_reembolsado").sum().alias("total"))
                .sort("mes")
            )
            MESES_PT = {1:"Jan",2:"Fev",3:"Mar",4:"Abr",5:"Mai",6:"Jun",
                        7:"Jul",8:"Ago",9:"Set",10:"Out",11:"Nov",12:"Dez"}
            mensal = mensal.with_columns(
                pl.col("mes").map_elements(lambda m: MESES_PT.get(m, str(m)),
                                          return_dtype=pl.Utf8).alias("mes_label")
            )
            fig_mensal = px.bar(
                mensal.to_pandas(),
                x="mes_label",
                y="total",
                title=f"Despesas mensais — {ano_sel}",
                labels={"mes_label": "Mês", "total": "Total (R$)"},
                color_discrete_sequence=["#c0392b"],
                text="total",
            )
            fig_mensal.update_traces(
                texttemplate="R$ %{y:,.0f}",
                textposition="outside",
            )
            fig_mensal.update_layout(
                height=300,
                margin=dict(t=50, b=10),
                yaxis=dict(tickprefix="R$ ", tickformat=",.0f"),
            )
            st.plotly_chart(fig_mensal, use_container_width=True)

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

# ── Guia Cívico ─────────────────────────────────────────────────────────────
st.header("📚 Guia Cívico — Como funciona o Senado Federal")
st.caption(
    "Informações sobre o papel do senador, o funcionamento das despesas (CEAPS), "
    "votações, emendas e os mecanismos de fiscalização do Congresso Nacional."
)

with st.expander("🏛️ O papel do Senador Federal"):
    st.markdown("""
### O que é um Senador Federal?

O **Senador Federal** representa o seu **estado** no Congresso Nacional — ao contrário dos
deputados, que representam o povo de forma proporcional à população. O Senado é composto
por **81 senadores**: exatamente **3 por estado** e pelo Distrito Federal, independentemente
do tamanho ou população da unidade federativa.

### Eleição e mandato

- **Mandato**: 8 anos — o mais longo cargo eletivo do Brasil
- **Sistema eleitoral**: Majoritário (quem tem mais votos ganha, sem segundo turno)
- **Renovação**: O Senado se renova em dois momentos alternados dentro de cada legislatura de 8 anos:
  - **1/3** dos senadores é renovado nas eleições de 2018, 2026, 2034...
  - **2/3** são renovados nas eleições de 2022, 2030, 2038...

Isso garante continuidade institucional — o Senado nunca é totalmente renovado de uma só vez.

### Funções e poderes

- **Aprovar indicações presidenciais**: embaixadores, ministros do STF, TCU, AGU, diretores de
  agências reguladoras, chefes do Banco Central etc.
- **Autorizar empréstimos externos** de estados e municípios
- **Julgar o Presidente, Vice-Presidente e Ministros** de Estado por crimes de responsabilidade
  (quando autorizado pela Câmara)
- **Revisar projetos de lei** aprovados pela Câmara (e vice-versa no bicameralismo)
- **Propor e votar PECs** (Propostas de Emenda à Constituição)
- **Suspender a vigência de lei** declarada inconstitucional pelo STF

### Câmara vs Senado

| | Câmara dos Deputados | Senado Federal |
|---|---|---|
| **Representa** | O povo | Os estados |
| **Tamanho** | 513 membros | 81 membros (3 por estado/DF) |
| **Mandato** | 4 anos | 8 anos |
| **Eleição** | Proporcional, lista aberta | Majoritária (turno único) |
| **Julgamento de autoridades** | Autoriza o processo | Julga e condena |
| **Aprovação de nomeações** | Não participa | Vota indicações presidenciais |
""")

with st.expander("💰 CEAPS — Cota para o Exercício da Atividade Parlamentar do Senado"):
    st.markdown("""
### O que é a CEAPS?

A **CEAPS** (Cota para o Exercício da Atividade Parlamentar do Senado) é o equivalente
senatorial do CEAP da Câmara — uma verba de reembolso de despesas exclusivamente vinculadas
ao exercício do mandato. **Não é salário**: exige comprovação com nota fiscal ou recibo.

### Composição do valor mensal

O valor total mensal varia por estado, pois inclui o custo real de passagens entre o estado
de origem e Brasília:

| Componente | Valor |
|---|---|
| Base de indenização fixa | R$ 15.000 / mês |
| Passagens aéreas (5 voos de ida e volta por mês) | Custo real — varia por estado |
| **Total médio mensal** | **~R$ 44.300** (varia de ~R$ 30.000 a ~R$ 57.000) |

O estado de origem mais distante (AM, AP, RR, PA) gera o maior custo de passagens e,
portanto, a CEAPS mais alta. Estados próximos a Brasília (GO, MG) têm valores menores.

### Categorias permitidas

As mesmas categorias válidas para os deputados (CEAP) se aplicam:

| Categoria | Exemplos |
|---|---|
| Passagens aéreas | Voos para o exercício do mandato |
| Hospedagem | Hotéis em viagens de trabalho |
| Alimentação | Refeições em exercício do mandato |
| Telefonia | Celular e telefone do escritório |
| Locação de escritório | Escritório de apoio parlamentar no estado |
| Transporte | Aluguel de veículos, táxi, pedágio, estacionamento |
| Combustíveis e lubrificantes | Para uso no mandato |
| Consultorias | Serviços técnicos de apoio à atividade parlamentar |
| Publicações | Assinatura de jornais e revistas relacionadas ao mandato |
| Segurança | Serviços contratados de segurança pessoal |

### Transparência

Todos os registros de CEAPS são publicados no [Portal de Dados Abertos do Senado](https://dadosabertos.senado.leg.br/)
e no [Portal da Transparência do Senado](https://www12.senado.leg.br/transparencia).
Os dados desta página refletem o que está disponível na API aberta do Senado Federal.
""")

with st.expander("🗳️ Como funcionam as votações no Senado"):
    st.markdown("""
### Tipos de votação no Senado

#### 1. Votação Simbólica

A forma mais comum. O presidente do Senado convida os que são **a favor** a permanecerem
sentados e os contrários a se manifestarem. Não gera registro individual — apenas o resultado
é registrado. Usada quando há consenso entre as lideranças.

#### 2. Votação Nominal (eletrônica)

Cada senador vota individualmente pelo painel eletrônico. O voto de cada um é **público**
(SIM / NÃO / ABSTENÇÃO). É obrigatória para:
- Proposta de Emenda à Constituição (PEC)
- Projetos que exijam quórum especial (leis complementares, cassações)
- Quando qualquer senador solicita a verificação de votação simbólica

#### 3. Votação Secreta

Usada principalmente para eleições internas (Presidente do Senado, Mesa Diretora) e para
votação do processo de impeachment do Presidente da República (fase de condenação).

### Orientação de liderança

Assim como na Câmara, os **líderes de bancada** registram a orientação do partido antes de
cada votação nominal. Os senadores são livres para votar de forma diferente (voto divergente),
mas há pressão política para seguir a orientação.

### Quóruns especiais

| Tipo de deliberação | Quórum mínimo |
|---|---|
| Votação ordinária | Maioria dos presentes (quórum mínimo de 41 senadores em plenário) |
| Lei complementar | Maioria absoluta (41 votos) |
| Emenda Constitucional (PEC) | 3/5 dos senadores = **49 votos**, em dois turnos |
| Condenação no impeachment | 2/3 dos senadores = **54 votos** |
| Suspensão de lei inconstitucional | Maioria absoluta (41 votos) |

### Taxa de presença nesta página

A taxa é calculada como a proporção de votações registradas na base de dados (desde 2019)
em que o senador registrou SIM, NÃO, ABSTENÇÃO ou voto equivalente. **Ausência** pode ser
por motivo justificado (licença médica, missão oficial, representação no exterior) ou
injustificado. Senadores com mandato iniciado após 2019 terão histórico menor.
""")

with st.expander("📋 Emendas Parlamentares — como funcionam"):
    st.markdown("""
### O que são emendas parlamentares?

**Emendas parlamentares** são o mecanismo pelo qual senadores e deputados indicam como parte
dos recursos do Orçamento da União (LOA) deve ser aplicada. Após aprovação da LOA, os recursos
são transferidos a estados, municípios, entidades públicas ou organizações — os **favorecidos**.

### Tipos de emendas

| Tipo | Quem propõe | Execução obrigatória? | Limite anual |
|---|---|---|---|
| **Individual** | Cada parlamentar individualmente | Sim (desde 2015) | 2% da RCL* |
| **De Bancada** | Bancada estadual (senadores + deputados do mesmo estado) | Sim (desde 2019) | 1% da RCL |
| **De Comissão** | Comissões temáticas do Congresso | Não | — |
| **Do Relator (RP9)** | Relator-geral do orçamento | Declarada inconstitucional (STF, 2022) | — |

*RCL = Receita Corrente Líquida da União. O limite das emendas individuais em 2024 foi de
aproximadamente R$ 15,5 milhões por parlamentar (senadores e deputados têm o mesmo limite).

### Fases de execução

| Fase | O que significa |
|---|---|
| **Empenho** | O governo reserva formalmente os recursos — compromisso contábil |
| **Liquidação** | Verificação de que a obra ou serviço foi entregue conforme contratado |
| **Pagamento** | Transferência efetiva do dinheiro ao favorecido |

Esta página usa a fase **Pagamento** como referência principal de valor transferido — o
indicador mais conservador e confiável do que efetivamente chegou ao beneficiário.

### O "Orçamento Secreto" (RP9 — 2020 a 2022)

As **Emendas do Relator** permitiram distribuição de bilhões sem identificação pública do
parlamentar beneficiado. O volume chegou a R$ 24,8 bilhões em 2020. O STF declarou o
mecanismo inconstitucional em dezembro de 2022 (ADPF 854).

### Vinculação dos dados nesta página

A vinculação entre os dados de emendas do Portal da Transparência (CGU) e os perfis de
senadores é feita por **normalização de nome** (acentos removidos, maiúsculas uniformizadas).
Isso pode causar falhas para senadores com nomes idênticos ou grafias divergentes entre
o sistema da CGU e o sistema do Senado Federal.

### Apoiamentos

**Apoiamento** é quando um segundo senador (ou deputado) co-assina um empenho de emenda de
outro parlamentar. O apoiador não é o autor original da emenda, mas indica formalmente
concordância com a destinação dos recursos.
""")

with st.expander("🏢 Comissões do Senado — como funcionam"):
    st.markdown("""
### O que são as comissões?

As **comissões** são órgãos colegiados do Senado compostos por um subconjunto de senadores,
criados para analisar matérias em profundidade antes da votação em plenário. Cada comissão
é especializada em uma área temática.

### Tipos de comissões no Senado

| Tipo | Descrição |
|---|---|
| **Permanente** | Existência contínua — analisam matérias de sua área temática (ex: CAE, CI, CCJ) |
| **Temporária** | Criadas para finalidade específica e se extinguem ao cumprir sua missão |
| **CPI** | Comissão Parlamentar de Inquérito — investigativa, com poderes quase judiciais |
| **Mistas** | Compostas por senadores e deputados (ex: Comissão Mista do Orçamento — CMO) |

### Comissões permanentes importantes

| Sigla | Nome | Área |
|---|---|---|
| CCJ | Constituição, Justiça e Cidadania | Constitucionalidade de proposições |
| CAE | Assuntos Econômicos | Economia, finanças, tributação |
| CAS | Assuntos Sociais | Saúde, previdência, assistência social |
| CI | Ciência, Tecnologia, Inovação e Comunicação | Tecnologia, telecomunicações |
| CRA | Agricultura e Reforma Agrária | Agronegócio, terras |
| CDR | Desenvolvimento Regional e Turismo | Infraestrutura, turismo |
| CREDN | Relações Exteriores e Defesa Nacional | Política externa, forças armadas |

### Cargo nas comissões

- **Titular**: Membro efetivo com direito a voto
- **Suplente**: Substitui o titular quando este está ausente; pode ser convocado
- **Presidente** / **Vice-presidente**: Conduz os trabalhos, decide a pauta

Um senador pode participar de diversas comissões simultaneamente, mas a presidência de uma
comissão é cargo de grande poder político — define quais propostas chegam a votar.
""")

with st.expander("🔍 Fiscalização e controle — como o cidadão pode monitorar"):
    st.markdown("""
### Quem fiscaliza o Senado?

#### TCU — Tribunal de Contas da União

O **TCU** é o órgão de controle externo do Congresso Nacional — é auxiliar do próprio
Legislativo no controle do Executivo. Pode auditar a aplicação de recursos de emendas
parlamentares e de CEAPS, aplicar multas e determinar ressarcimentos.

#### CGU — Controladoria-Geral da União

A **CGU** fiscaliza a execução dos programas federais, incluindo a execução de emendas
parlamentares, e publica os dados no Portal da Transparência.

#### Portal da Transparência do Senado

O [Portal de Transparência do Senado](https://www12.senado.leg.br/transparencia) publica:
- Despesas com CEAPS por senador
- Remunerações de servidores
- Contratos e licitações do Senado
- Prestações de contas das lideranças

### Como você pode fiscalizar

| Ação | Como fazer |
|---|---|
| Ver os gastos CEAPS de qualquer senador | [Transparência do Senado](https://www12.senado.leg.br/transparencia/sen) |
| Ver emendas de todos os parlamentares | [Portal da Transparência — emendas](https://portaldatransparencia.gov.br/emendas-parlamentares) |
| Acompanhar votações do plenário | [API do Senado — votações](https://legis.senado.leg.br/dadosabertos/plenario/lista/votacao) |
| Consultar o texto de qualquer proposição | [Sistema de Legislação do Senado](https://www25.senado.leg.br/web/atividade/materias) |
| Verificar a composição das comissões | [Comissões do Senado](https://www25.senado.leg.br/web/atividade/comissoes) |
| Dados abertos do Senado | [dadosabertos.senado.leg.br](https://dadosabertos.senado.leg.br/) |
""")

st.divider()
st.caption("Fonte: API de Dados Abertos do Senado Federal — legis.senado.leg.br/dadosabertos")
