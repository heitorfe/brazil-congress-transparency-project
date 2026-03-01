"""Flatten functions for Chamber voting session and individual deputy vote data.

Two-level extraction mirrors the Senate pattern:
  flatten_votacao_camara()  — session-level metadata (one row per session)
  flatten_voto_camara()     — individual deputy vote (exploded from votos)

Key Chamber API quirk:
  The deputy sub-object inside a vote record uses the key ``deputado_``
  (with a trailing underscore) — not ``deputado``.
"""


def flatten_votacao_camara(v: dict) -> dict:
    """Flatten one voting session from GET /votacoes?dataInicio=...&dataFim=...."""
    return {
        "votacao_id":         v.get("id"),
        "data":               v.get("data"),
        "data_hora_registro": v.get("dataHoraRegistro"),
        "sigla_orgao":        v.get("siglaOrgao"),
        "uri_evento":         v.get("uriEvento"),
        "proposicao_objeto":  v.get("proposicaoObjeto"),
        "uri_proposicao":     v.get("uriProposicaoObjeto"),
        "descricao":          v.get("descricao"),
        "aprovacao":          v.get("aprovacao"),
    }


def flatten_voto_camara(votacao_id: str, rec: dict) -> dict:
    """Flatten one deputy vote from GET /votacoes/{id}/votos.

    Note: deputy info lives under the ``deputado_`` key (trailing underscore).
    """
    dep = rec.get("deputado_") or {}
    return {
        "votacao_id":    votacao_id,
        "deputado_id":   str(dep.get("id") or ""),
        "nome":          dep.get("nome"),
        "sigla_partido": dep.get("siglaPartido"),
        "sigla_uf":      dep.get("siglaUf"),
        "id_legislatura": dep.get("idLegislatura"),
        "tipo_voto":     rec.get("tipoVoto"),
        "data_registro": rec.get("dataRegistroVoto"),
    }
