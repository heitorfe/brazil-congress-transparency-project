"""Flatten functions for voting session and individual senator vote data."""


def flatten_votacao(v: dict) -> dict:
    """Extract session-level fields from one voting session object.

    The nested ``informeLegislativo`` sub-object is collapsed to a single
    ``informe_texto`` string. The ``votos`` array is intentionally discarded
    here — it is exploded separately via ``flatten_voto``.
    """
    inf = v.get("informeLegislativo") or {}
    return {
        "codigo_sessao_votacao":     v.get("codigoSessaoVotacao"),
        "codigo_votacao_sve":        v.get("codigoVotacaoSve"),
        "codigo_sessao":             v.get("codigoSessao"),
        "codigo_sessao_legislativa": v.get("codigoSessaoLegislativa"),
        "sigla_tipo_sessao":         v.get("siglaTipoSessao"),
        "numero_sessao":             v.get("numeroSessao"),
        "data_sessao":               v.get("dataSessao"),
        "id_processo":               v.get("idProcesso"),
        "codigo_materia":            v.get("codigoMateria"),
        "identificacao":             v.get("identificacao"),
        "sigla_materia":             v.get("sigla"),
        "numero_materia":            str(v.get("numero") or ""),
        "ano_materia":               v.get("ano"),
        "data_apresentacao":         v.get("dataApresentacao"),
        "ementa":                    v.get("ementa"),
        "sequencial_sessao":         v.get("sequencialSessao"),
        "votacao_secreta":           v.get("votacaoSecreta"),
        "descricao_votacao":         v.get("descricaoVotacao"),
        "resultado_votacao":         v.get("resultadoVotacao"),
        "total_votos_sim":           v.get("totalVotosSim"),
        "total_votos_nao":           v.get("totalVotosNao"),
        "total_votos_abstencao":     v.get("totalVotosAbstencao"),
        "informe_texto":             inf.get("texto"),
    }


def flatten_voto(codigo_sessao_votacao: int, voto: dict) -> dict:
    """Extract one senator's vote from the nested ``votos`` array."""
    return {
        "codigo_sessao_votacao": codigo_sessao_votacao,
        "codigo_parlamentar":    voto.get("codigoParlamentar"),
        "nome_parlamentar":      voto.get("nomeParlamentar"),
        "sexo_parlamentar":      voto.get("sexoParlamentar"),
        "sigla_partido":         voto.get("siglaPartidoParlamentar"),
        "sigla_uf":              voto.get("siglaUFParlamentar"),
        "sigla_voto":            voto.get("siglaVotoParlamentar"),
        "descricao_voto":        voto.get("descricaoVotoParlamentar"),
    }
