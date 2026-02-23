"""Flatten functions for committee master list and membership data."""


def _int(val: str | None) -> int | None:
    """Safely convert a string to int; return None on failure."""
    try:
        return int(val) if val else None
    except (ValueError, TypeError):
        return None


def flatten_colegiado(c: dict) -> dict:
    """Flatten one record from GET /comissao/lista/colegiados.

    Fields are flat PascalCase — no nested tipo sub-object.
    The ``Publica`` flag is ``'S'``/``'N'``; converted to bool.
    The three mistas-only member-count fields are always None for this source
    and may be augmented later by ``flatten_mista``.
    """
    return {
        "codigo_comissao":           str(c.get("Codigo") or ""),
        "sigla_comissao":            c.get("Sigla"),
        "nome_comissao":             c.get("Nome"),
        "finalidade":                c.get("Finalidade"),
        "sigla_casa":                c.get("SiglaCasa"),
        "codigo_tipo":               c.get("CodigoTipoColegiado"),
        "sigla_tipo":                c.get("SiglaTipoColegiado"),
        "descricao_tipo":            c.get("DescricaoTipoColegiado"),
        "data_inicio":               c.get("DataInicio"),
        "data_fim":                  c.get("DataFim"),
        "publica":                   c.get("Publica") == "S" if c.get("Publica") else None,
        "qtd_titulares":             None,
        "qtd_senadores_titulares":   None,
        "qtd_deputados_titulares":   None,
        "fonte":                     "colegiados",
    }


def flatten_mista(c: dict) -> dict:
    """Flatten one record from GET /comissao/lista/mistas.

    Uses ``CodigoColegiado`` / ``NomeColegiado`` / ``SiglaColegiado`` keys
    (not ``Codigo`` / ``Nome`` / ``Sigla`` as in colegiados).
    Member counts are strings in the API — cast to int.
    """
    qtd = c.get("QuantidadesMembros") or {}
    return {
        "codigo_comissao":           str(c.get("CodigoColegiado") or ""),
        "sigla_comissao":            c.get("SiglaColegiado"),
        "nome_comissao":             c.get("NomeColegiado"),
        "finalidade":                c.get("Finalidade"),
        "sigla_casa":                "CN",
        "codigo_tipo":               None,
        "sigla_tipo":                "MISTA",
        "descricao_tipo":            "Comissão Mista",
        "data_inicio":               None,
        "data_fim":                  None,
        "publica":                   None,
        "qtd_titulares":             _int(qtd.get("Titulares")),
        "qtd_senadores_titulares":   _int(qtd.get("SenadoresTitulares")),
        "qtd_deputados_titulares":   _int(qtd.get("DeputadosTitulares")),
        "fonte":                     "mistas",
    }


def flatten_membro(senador_id: str, comissao: dict) -> dict:
    """Flatten one committee membership record from /senador/{code}/comissoes."""
    ident = comissao.get("IdentificacaoComissao") or {}
    return {
        "senador_id":             senador_id,
        "codigo_comissao":        str(ident.get("CodigoComissao") or ""),
        "sigla_comissao":         ident.get("SiglaComissao"),
        "nome_comissao":          ident.get("NomeComissao"),
        "sigla_casa":             ident.get("SiglaCasaComissao"),
        "descricao_participacao": comissao.get("DescricaoParticipacao"),
        "data_inicio":            comissao.get("DataInicio"),
        "data_fim":               comissao.get("DataFim"),
    }
