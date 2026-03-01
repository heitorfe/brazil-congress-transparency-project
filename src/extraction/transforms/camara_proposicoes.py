"""Flatten function for Chamber legislative proposals (proposicoes).

Proposals are fetched per-deputy (GET /proposicoes?idDeputadoAutor={id}),
so the deputado_id author is injected by the extractor, not present in
the record itself.

The ``statusProposicao`` sub-object holds the current tramitation state.
"""


def flatten_proposicao(rec: dict, deputado_id: str) -> dict:
    """Flatten one proposal record from GET /proposicoes?idDeputadoAutor={id}."""
    status = rec.get("statusProposicao") or {}
    return {
        "proposicao_id":       str(rec.get("id") or ""),
        "deputado_id_autor":   deputado_id,
        "sigla_tipo":          rec.get("siglaTipo"),
        "cod_tipo":            rec.get("codTipo"),
        "numero":              rec.get("numero"),
        "ano":                 rec.get("ano"),
        "ementa":              rec.get("ementa"),
        "ementa_detalhada":    rec.get("ementaDetalhada"),
        "keywords":            rec.get("keywords"),
        "data_apresentacao":   rec.get("dataApresentacao"),
        "sigla_orgao_status":  status.get("siglaOrgao"),
        "regime_status":       status.get("regime"),
        "descricao_situacao":  status.get("descricaoSituacao"),
        "cod_situacao":        status.get("codSituacao"),
        "apreciacao":          status.get("apreciacao"),
        "url_inteiro_teor":    rec.get("urlInteiroTeor"),
    }
