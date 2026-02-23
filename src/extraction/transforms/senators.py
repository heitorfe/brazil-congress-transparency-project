"""Flatten functions for senator biographical and mandate data."""


def flatten_senator(raw: dict) -> dict:
    """Flatten one senator record from GET /senador/{code}.json."""
    ident = raw.get("IdentificacaoParlamentar", {})
    dados = raw.get("DadosBasicosParlamentar", {})
    return {
        "senador_id":       str(ident.get("CodigoParlamentar", "")),
        "nome_parlamentar": ident.get("NomeParlamentar"),
        "nome_completo":    ident.get("NomeCompletoParlamentar"),
        "sexo":             ident.get("SexoParlamentar"),
        "foto_url":         ident.get("UrlFotoParlamentar"),
        "pagina_url":       ident.get("UrlPaginaParlamentar"),
        "email":            ident.get("EmailParlamentar"),
        "partido_sigla":    ident.get("SiglaPartidoParlamentar"),
        "estado_sigla":     ident.get("UfParlamentar"),
        "data_nascimento":  dados.get("DataNascimento"),
        "naturalidade":     dados.get("Naturalidade"),
        "uf_naturalidade":  dados.get("UfNaturalidade"),
    }


def flatten_mandate(senador_id: str, mandato: dict) -> dict:
    """Flatten one mandate record from GET /senador/{code}/mandatos.json.

    Each 8-year mandate spans two 4-year legislaturas:
      mandato_inicio = PrimeiraLegislatura.DataInicio
      mandato_fim    = SegundaLegislatura.DataFim
    """
    leg1 = mandato.get("PrimeiraLegislaturaDoMandato", {})
    leg2 = mandato.get("SegundaLegislaturaDoMandato", {})
    return {
        "senador_id":             senador_id,
        "mandato_id":             str(mandato.get("CodigoMandato", "")),
        "estado_sigla":           mandato.get("UfParlamentar"),
        "data_inicio":            leg1.get("DataInicio"),
        "data_fim":               leg2.get("DataFim"),
        "legislatura_inicio":     str(leg1.get("NumeroLegislatura", "")),
        "legislatura_fim":        str(leg2.get("NumeroLegislatura", "")),
        "descricao_participacao": mandato.get("DescricaoParticipacao"),
    }
