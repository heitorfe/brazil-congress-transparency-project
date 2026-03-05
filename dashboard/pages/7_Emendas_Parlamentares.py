import json
from pathlib import Path

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from queries import (
    get_emendas_kpis,
    get_emendas_por_ano,
    get_emendas_por_uf,
    get_emendas_por_uf_autoria,
    get_top_autores_emendas,
    get_favorecidos_emenda,
    get_fornecedor_emenda_crossref,
)

st.set_page_config(
    page_title="Emendas Parlamentares",
    page_icon="📋",
    layout="wide",
)


def _fmt_brl(v, scale: str = "B") -> str:
    """Format a BRL value with optional B/M suffix. Accepts Decimal or float."""
    v = float(v)
    if scale == "B":
        return f"R$ {v / 1e9:,.2f}B".replace(",", "X").replace(".", ",").replace("X", ".")
    if scale == "M":
        return f"R$ {v / 1e6:,.1f}M".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {v:,.0f}".replace(",", ".")


@st.cache_data(ttl=3600)
def load_kpis():
    return get_emendas_kpis()


@st.cache_data(ttl=3600)
def load_por_ano():
    return get_emendas_por_ano()


@st.cache_data(ttl=3600)
def load_top(n: int):
    return get_top_autores_emendas(n)


@st.cache_data(ttl=3600)
def load_por_uf(ano_inicio: int | None, ano_fim: int | None):
    return get_emendas_por_uf(ano_inicio, ano_fim)


@st.cache_data(ttl=3600)
def load_por_uf_autoria(ano_inicio: int | None, ano_fim: int | None):
    return get_emendas_por_uf_autoria(ano_inicio, ano_fim)


@st.cache_data(ttl=3600)
def load_favorecidos(year, n):
    return get_favorecidos_emenda(year=year, n=n)


@st.cache_data(ttl=3600)
def load_crossref_emendas(year):
    return get_fornecedor_emenda_crossref(year=year)


@st.cache_data
def load_geojson() -> dict:
    """Load the pre-built Brazilian state GeoJSON from local assets (97 KB)."""
    path = Path(__file__).parent.parent / "assets" / "br_states.geojson"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── Page header ─────────────────────────────────────────────────────────────

st.title("📋 Emendas Parlamentares")
st.caption(
    "Execução orçamentária de emendas individuais e de bancada ao Orçamento da União — "
    "fonte: Portal da Transparência (CGU), 2014–presente."
)

kpis = load_kpis()

if kpis["total_emendas"] == 0:
    st.error(
        "Dados não disponíveis. Execute `python src/extraction/extract_emendas.py` "
        "seguido de `dbt run --select marts.dim_emenda+ marts.agg_emenda_por_autor+` primeiro."
    )
    st.stop()

# ── KPI Cards ───────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
c1.metric(
    "Emendas cadastradas",
    f"{kpis['total_emendas']:,}".replace(",", "."),
    help="Número de emendas distintas com pagamento registrado no período",
)
c2.metric(
    "Autores distintos",
    f"{kpis['total_autores']:,}".replace(",", "."),
    help="Parlamentares que apresentaram pelo menos uma emenda com pagamento",
)
c3.metric(
    "Total pago",
    _fmt_brl(kpis["total_pago"]),
    help="Valor efetivamente transferido ao beneficiário final (fase Pagamento)",
)
c4.metric(
    "Período",
    f"{kpis['ano_min']} – {kpis['ano_max']}",
    help="Anos cobertos pelos dados disponíveis",
)

with st.expander("ℹ️ O que são emendas parlamentares?"):
    st.markdown("""
**Emendas parlamentares** são instrumentos do processo orçamentário brasileiro que permitem a
deputados federais e senadores indicar como parte dos recursos públicos federais devem ser aplicados.
São a forma pela qual o Poder Legislativo influencia a execução do orçamento anual, direcionando
verbas para obras, serviços e programas em suas bases eleitorais.

---

**Como funcionam no processo orçamentário**

1. O Poder Executivo elabora o projeto de orçamento (PLOA).
2. O Congresso analisa e propõe emendas ao projeto.
3. As emendas aprovadas integram a Lei Orçamentária Anual (LOA).
4. A partir de 2015, emendas individuais se tornaram **impositivas** — o governo é
   obrigado a executá-las dentro de limites constitucionais.

---

**Tipos principais**

| Tipo | Quem propõe | Obrigatória? |
|---|---|---|
| **Emenda Individual** | Cada parlamentar isoladamente | Sim (desde 2015) |
| **Emenda de Bancada** | Bancada estadual (grupo de parlamentares do mesmo estado) | Sim (desde 2019) |
| **Emenda de Comissão** | Comissões temáticas do Congresso | Não |
| **Emenda do Relator (RP9)** | Relator-geral do orçamento | Não — extinta pelo STF em 2022 |

---

**O "Orçamento Secreto" (2020–2022)**

As **Emendas do Relator (RP9)** ficaram conhecidas como _orçamento secreto_ por não exigirem
identificação do parlamentar beneficiado, nem critérios públicos para distribuição. Isso criou
um mecanismo de patronagem política opaco e de grande escala — visível no pico de **R$ 24,8 bi**
em 2020 e **R$ 22,6 bi** em 2023. O STF declarou as emendas de relator inconstitucionais em
dezembro de 2022, mas parte dos recursos já havia sido executada.

---

**Por que os valores cresceram com o tempo?**

- A transição para emendas impositivas (2015, 2019) aumentou a taxa de execução dos valores previstos.
- A criação e expansão do RP9 (2020–2022) injetou volume adicional de recursos sem rastreabilidade.
- Mesmo após o fim do RP9, o teto constitucional das emendas individuais continuou sendo corrigido
  a cada ciclo orçamentário.
""")

st.divider()


# ══════════════════════════════════════════════════════════════════════════
# SEÇÃO 1 — EVOLUÇÃO ANUAL
# ══════════════════════════════════════════════════════════════════════════

st.header("📅 Evolução Anual — Total Pago")

anual_df = load_por_ano()

if not anual_df.is_empty():
    anual_pd = anual_df.to_pandas()
    anual_pd["ano_str"] = anual_pd["ano_emenda"].astype(str)
    anual_pd["total_pago_f"] = anual_pd["total_pago"].apply(float)

    col_chart, col_info = st.columns([3, 1])

    with col_chart:
        fig_anual = go.Figure()
        fig_anual.add_trace(go.Bar(
            x=anual_pd["ano_str"],
            y=anual_pd["total_pago_f"],
            name="Total pago",
            marker_color="#2c7bb6",
            text=anual_pd["total_pago_f"].apply(lambda v: f"R$ {v / 1e9:.1f}B"),
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Total pago: R$ %{y:,.0f}<extra></extra>",
        ))

        # Annotation for the orçamento secreto period
        fig_anual.add_vrect(
            x0="2019.5", x1="2022.5",
            fillcolor="#f39c12", opacity=0.08,
            line_width=0,
            annotation_text="Orçamento Secreto (RP9)",
            annotation_position="top left",
            annotation_font_size=11,
            annotation_font_color="#b7770d",
        )

        fig_anual.update_layout(
            xaxis_title="Ano da emenda",
            yaxis_title="Total pago (R$)",
            yaxis_tickformat=",.0f",
            hovermode="x unified",
            showlegend=False,
            height=380,
            margin=dict(t=30, b=10),
        )
        st.plotly_chart(fig_anual, use_container_width=True)
        st.caption(
            "Área destacada (2020–2022): período das Emendas do Relator (RP9), "
            "declaradas inconstitucionais pelo STF em dezembro de 2022."
        )

    with col_info:
        st.subheader("Resumo por ano")
        display_anual = anual_df.select([
            pl.col("ano_emenda").alias("Ano"),
            pl.col("num_emendas").alias("Emendas"),
            pl.col("num_autores").alias("Autores"),
            pl.col("total_pago").map_elements(
                lambda v: f"R$ {float(v) / 1e6:,.0f}M".replace(",", "."),
                return_dtype=pl.Utf8,
            ).alias("Pago"),
        ])
        st.dataframe(display_anual, use_container_width=True, hide_index=True, height=320)
else:
    st.info("Dados anuais não disponíveis.")

st.divider()


# ══════════════════════════════════════════════════════════════════════════
# SEÇÃO 2 — MAPA COROPLÉTICO POR UF
# ══════════════════════════════════════════════════════════════════════════

st.header("🗺️ Distribuição Geográfica por Estado (UF)")

# ── Mode toggle ─────────────────────────────────────────────────────────
_MODO_DESTINO  = "🏥 Destino — UF que recebe o recurso"
_MODO_AUTORIA  = "🏛️ Autoria — UF do parlamentar autor"

map_modo = st.radio(
    "Perspectiva do mapa",
    options=[_MODO_DESTINO, _MODO_AUTORIA],
    horizontal=True,
    key="map_modo",
    help=(
        "**Destino**: onde o dinheiro chegou (estado do beneficiário). "
        "**Autoria**: de onde vêm os parlamentares que enviaram o dinheiro."
    ),
)

if map_modo == _MODO_DESTINO:
    st.caption("Total efetivamente pago a beneficiários localizados em cada estado.")
else:
    st.caption(
        "Total pago por parlamentares representando cada estado — "
        "inclui apenas autores vinculados a senadores identificados na base."
    )

# ── Year range selector ──────────────────────────────────────────────────
_anos = sorted(anual_df["ano_emenda"].to_list()) if not anual_df.is_empty() else [2015, 2025]
_ano_min_data, _ano_max_data = int(_anos[0]), int(_anos[-1])

map_col, filter_col = st.columns([4, 1])
with filter_col:
    st.markdown("**Filtrar período**")
    map_ano_inicio, map_ano_fim = st.slider(
        "Ano da emenda",
        min_value=_ano_min_data,
        max_value=_ano_max_data,
        value=(_ano_min_data, _ano_max_data),
        key="map_ano_slider",
        label_visibility="collapsed",
    )
    st.caption(f"Exibindo {map_ano_inicio}–{map_ano_fim}")

_ano_i = map_ano_inicio if map_ano_inicio > _ano_min_data else None
_ano_f = map_ano_fim   if map_ano_fim   < _ano_max_data   else None

# ── Load the right dataset ───────────────────────────────────────────────
if map_modo == _MODO_DESTINO:
    uf_df    = load_por_uf(_ano_i, _ano_f)
    uf_col   = "uf_recurso"
    bar_y_label = "UF (destino)"
    map_hover = {
        "uf_recurso":      False,
        "total_pago_f":    False,
        "total_pago_fmt":  True,
        "num_emendas":     True,
        "num_favorecidos": True,
        "num_municipios":  True,
    }
    map_labels = {
        "total_pago_fmt":  "Total pago",
        "num_emendas":     "Emendas",
        "num_favorecidos": "Favorecidos",
        "num_municipios":  "Municípios",
    }
    bar_custom   = ["num_emendas", "num_municipios", "num_favorecidos"]
    bar_hover_tpl = (
        "<b>%{y}</b><br>"
        "Total pago: %{text}<br>"
        "Emendas: %{customdata[0]}<br>"
        "Municípios: %{customdata[1]}<br>"
        "Favorecidos: %{customdata[2]}<extra></extra>"
    )
else:
    uf_df    = load_por_uf_autoria(_ano_i, _ano_f)
    uf_col   = "uf"
    bar_y_label = "UF (autoria)"
    map_hover = {
        "uf":             False,
        "total_pago_f":   False,
        "total_pago_fmt": True,
        "num_emendas":    True,
        "num_autores":    True,
        "num_municipios": True,
    }
    map_labels = {
        "total_pago_fmt": "Total pago",
        "num_emendas":    "Emendas",
        "num_autores":    "Autores",
        "num_municipios": "Municípios (destino)",
    }
    bar_custom   = ["num_emendas", "num_autores", "num_municipios"]
    bar_hover_tpl = (
        "<b>%{y}</b><br>"
        "Total pago: %{text}<br>"
        "Emendas: %{customdata[0]}<br>"
        "Autores: %{customdata[1]}<br>"
        "Municípios (destino): %{customdata[2]}<extra></extra>"
    )

br_geojson = load_geojson()

with map_col:
    if not uf_df.is_empty():
        uf_pd = uf_df.to_pandas()
        uf_pd["total_pago_f"]   = uf_pd["total_pago"].apply(float)
        uf_pd["total_pago_fmt"] = uf_pd["total_pago_f"].apply(
            lambda v: f"R$ {v / 1e9:.2f}B"
        )

        fig_map = px.choropleth_map(
            uf_pd,
            geojson=br_geojson,
            locations=uf_col,
            featureidkey="id",
            color="total_pago_f",
            map_style="white-bg",
            zoom=3.0,
            center={"lat": -15.0, "lon": -55.0},
            color_continuous_scale="Blues",
            hover_name=uf_col,
            opacity=0.8,
            hover_data=map_hover,
            labels=map_labels,
        )
        fig_map.update_coloraxes(
            colorbar_title="R$",
            colorbar_tickformat=",.0f",
        )
        fig_map.update_layout(
            height=520,
            margin=dict(t=10, b=10, l=0, r=0),
        )
        st.plotly_chart(fig_map, use_container_width=True)

# UF bar chart below the map — same filtered data
if not uf_df.is_empty():
    fig_uf = px.bar(
        uf_pd.sort_values("total_pago_f"),
        x="total_pago_f",
        y=uf_col,
        orientation="h",
        color="total_pago_f",
        color_continuous_scale="Blues",
        text="total_pago_fmt",
        labels={"total_pago_f": "Total pago (R$)", uf_col: bar_y_label},
        height=max(500, len(uf_pd) * 22),
        custom_data=bar_custom,
    )
    fig_uf.update_traces(
        textposition="outside",
        hovertemplate=bar_hover_tpl,
    )
    fig_uf.update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        xaxis_tickformat=",.0f",
        yaxis=dict(categoryorder="total ascending"),
        margin=dict(t=10, b=10, r=120),
    )
    st.plotly_chart(fig_uf, use_container_width=True)

st.divider()


# ══════════════════════════════════════════════════════════════════════════
# SEÇÃO 3 — RANKING DE AUTORES
# ══════════════════════════════════════════════════════════════════════════

st.header("🏆 Maiores Autores de Emendas")
st.caption(
    "Ranking dos parlamentares com maior volume pago ao beneficiário final, todos os anos."
)

top_n = st.slider("Número de autores exibidos", 10, 50, 20, key="top_n_emendas")
top_df = load_top(top_n)

if not top_df.is_empty():
    top_pd = top_df.to_pandas()

    # Flag senators vs. non-senators with color
    top_pd["categoria"] = top_pd["is_senador_atual"].apply(
        lambda v: "Senador atual" if v else "Outro parlamentar"
    )

    fig_top = px.bar(
        top_pd,
        x="total_pago",
        y="nome_autor_emenda",
        orientation="h",
        color="categoria",
        color_discrete_map={
            "Senador atual":      "#1f6cb0",
            "Outro parlamentar":  "#aaa",
        },
        labels={
            "total_pago":        "Total Pago (R$)",
            "nome_autor_emenda": "Autor",
            "categoria":         "Categoria",
        },
        text="total_pago",
        height=max(420, top_n * 28),
        custom_data=["partido_sigla", "estado_sigla", "num_emendas", "municipios"],
    )
    fig_top.update_traces(
        texttemplate="R$ %{x:,.0f}",
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Partido: %{customdata[0]}<br>"
            "UF: %{customdata[1]}<br>"
            "Emendas: %{customdata[2]}<br>"
            "Municípios: %{customdata[3]}<br>"
            "Total pago: R$ %{x:,.0f}<extra></extra>"
        ),
    )
    fig_top.update_layout(
        yaxis=dict(categoryorder="total ascending"),
        xaxis_tickformat=",.0f",
        showlegend=False,
        margin=dict(l=0, r=120, t=20, b=10),
    )
    st.plotly_chart(fig_top, use_container_width=True)

    with st.expander("📋 Tabela completa do ranking"):
        display_top = top_df.select([
            pl.col("nome_autor_emenda").alias("Autor"),
            pl.col("partido_sigla").alias("Partido"),
            pl.col("estado_sigla").alias("UF"),
            pl.col("num_emendas").alias("Emendas"),
            pl.col("municipios").alias("Municípios"),
            pl.col("total_empenhado").map_elements(
                lambda v: f"R$ {v:,.0f}".replace(",", "."),
                return_dtype=pl.Utf8,
            ).alias("Empenhado"),
            pl.col("total_pago").map_elements(
                lambda v: f"R$ {v:,.0f}".replace(",", "."),
                return_dtype=pl.Utf8,
            ).alias("Pago"),
            pl.col("is_senador_atual").alias("Senador atual?"),
        ])
        st.dataframe(display_top, use_container_width=True, hide_index=True)
else:
    st.info("Dados de autores não disponíveis.")

st.divider()

# ══════════════════════════════════════════════════════════════════════════
# GLOSSÁRIO
# ══════════════════════════════════════════════════════════════════════════

with st.expander("📖 Glossário — termos técnicos desta página"):
    st.markdown("""
| Termo | Significado |
|---|---|
| **Empenho** | Reserva formal de recursos no orçamento — o governo se compromete a pagar. |
| **Liquidação** | Verificação de que a obra/serviço foi entregue conforme contratado. |
| **Pagamento** | Transferência efetiva de recursos ao beneficiário final. |
| **LOA** | Lei Orçamentária Anual — define o orçamento federal para o exercício. |
| **SIAFI** | Sistema de Administração Financeira do Governo Federal — registra todos os documentos de despesa. |
| **RP6** | Rubrica orçamentária das emendas individuais impositivas. |
| **RP7** | Rubrica das emendas de bancada. |
| **RP8** | Rubrica das emendas de comissão. |
| **RP9** | Rubrica das emendas do relator ("orçamento secreto") — extinta em 2022. |
| **Favorecido** | Entidade ou pessoa que recebeu o recurso (prefeitura, ONG, empresa, etc.). |
| **Apoiamento** | Co-assinatura de empenho: outro parlamentar que endossa a destinação de recursos. |
""")

st.divider()

# ── Favorecidos (TransfereGov) ───────────────────────────────────────────────

st.header("💰 Favorecidos de Emendas (TransfereGov)")
st.caption(
    "Beneficiários diretos de emendas parlamentares — dados do TransfereGov "
    "(Portal da Transparência). Cada linha representa uma empresa ou pessoa que "
    "recebeu recursos de emendas parlamentares."
)

col_fav_year, col_fav_n = st.columns([1, 1])
with col_fav_year:
    fav_year = st.selectbox(
        "Filtrar por ano",
        [None] + list(range(2014, 2026)),
        format_func=lambda x: "Todos os anos" if x is None else str(x),
        key="fav_year",
    )
with col_fav_n:
    fav_n = st.slider("Top N beneficiários", 10, 100, 30, key="fav_n")

fav_df = load_favorecidos(year=fav_year, n=fav_n)

if fav_df.is_empty():
    st.info("Dados de favorecidos não disponíveis. Execute o extractor TransfereGov primeiro.")
else:
    col_pj, col_pf = st.columns(2)
    pj_count = fav_df.filter(pl.col("tipo_pessoa").str.contains("(?i)jur")).height
    pf_count = fav_df.filter(pl.col("tipo_pessoa").str.contains("(?i)f")).height
    col_pj.metric("Pessoa Jurídica (Top N)", f"{pj_count:,}")
    col_pf.metric("Pessoa Física (Top N)", f"{pf_count:,}")

    fig_fav = px.bar(
        fav_df.sort("total_transferido").tail(20).to_pandas(),
        x="total_transferido",
        y="nome_favorecido",
        orientation="h",
        color="tipo_pessoa",
        labels={
            "total_transferido": "Total Transferido (R$)",
            "nome_favorecido": "",
            "tipo_pessoa": "Tipo",
        },
        text_auto=".2s",
    )
    fig_fav.update_layout(showlegend=True)
    st.plotly_chart(fig_fav, use_container_width=True)

    # UF breakdown
    if "uf_favorecido" in fav_df.columns:
        uf_fav = (
            fav_df.group_by("uf_favorecido")
            .agg(pl.sum("total_transferido"))
            .sort("total_transferido", descending=True)
            .head(15)
            .to_pandas()
        )
        fig_uf = px.bar(
            uf_fav,
            x="uf_favorecido",
            y="total_transferido",
            labels={"uf_favorecido": "UF", "total_transferido": "Total (R$)"},
            title="Total Transferido por UF Destino",
        )
        st.plotly_chart(fig_uf, use_container_width=True)

    with st.expander("📋 Tabela completa dos favorecidos"):
        st.dataframe(
            fav_df.rename({
                "codigo_favorecido": "CNPJ/CPF",
                "nome_favorecido": "Beneficiário",
                "tipo_pessoa": "Tipo Pessoa",
                "uf_favorecido": "UF",
                "num_emendas": "Nº Emendas",
                "total_transferido": "Total (R$)",
            }).to_pandas(),
            use_container_width=True,
            hide_index=True,
        )

    st.subheader("🔗 Beneficiários que também têm contratos federais")
    st.caption(
        "Empresas (CNPJ) identificadas como favorecidas de emendas que TAMBÉM "
        "possuem contratos com o governo federal — potencial de cruzamento de dados."
    )
    crossref_df = load_crossref_emendas(year=None)
    if not crossref_df.is_empty():
        st.metric("Empresas identificadas no cruzamento", len(crossref_df))
        st.dataframe(
            crossref_df.rename({
                "cnpj": "CNPJ",
                "nome_empresa": "Empresa",
                "total_contratos": "Total Contratos (R$)",
                "total_emendas": "Total Emendas (R$)",
                "num_contratos": "Nº Contratos",
                "num_emendas": "Nº Emendas",
            }).to_pandas(),
            use_container_width=True,
            hide_index=True,
        )

st.caption(
    "**Fonte:** Portal da Transparência — CGU "
    "(https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares) | "
    f"Período: {kpis['ano_min']}–{kpis['ano_max']}."
)
