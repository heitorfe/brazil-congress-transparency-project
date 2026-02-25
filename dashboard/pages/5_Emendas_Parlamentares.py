import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import polars as pl

from queries import (
    get_emendas_kpis,
    get_emendas_por_ano,
    get_top_autores_emendas,
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
# SEÇÃO 2 — RANKING DE AUTORES
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
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
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

st.caption(
    "**Fonte:** Portal da Transparência — CGU "
    "(https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares) | "
    f"Período: {kpis['ano_min']}–{kpis['ano_max']}."
)
