"""Flatten function for Senate leadership position records.

Key quirks:
  - ``codigoParlamentar`` is an integer — stored as string for FK join to dim_senador.
  - ``codigoPartido`` is optional (None for government leaders who are identified
    by ``codigoPartidoFiliacao`` instead).
  - ``numeroOrdemViceLider`` is optional (None for primary leaders).
"""


def flatten_lideranca_record(rec: dict) -> dict:
    """Flatten one leadership record from GET /composicao/lideranca.json."""
    return {
        "codigo":                       rec.get("codigo"),
        "casa":                         rec.get("casa"),
        "sigla_tipo_unidade_lideranca": rec.get("siglaTipoUnidadeLideranca"),
        "descricao_tipo_unidade":       rec.get("descricaoTipoUnidadeLideranca"),
        "codigo_parlamentar":           str(rec.get("codigoParlamentar") or ""),
        "nome_parlamentar":             rec.get("nomeParlamentar"),
        "data_designacao":              rec.get("dataDesignacao"),
        "sigla_tipo_lideranca":         rec.get("siglaTipoLideranca"),
        "descricao_tipo_lideranca":     rec.get("descricaoTipoLideranca"),
        "numero_ordem_vice_lider":      rec.get("numeroOrdemViceLider"),
        # Party-specific leadership (optional — only for party/bloc leaders)
        "codigo_partido":               str(rec.get("codigoPartido") or ""),
        "sigla_partido":                rec.get("siglaPartido"),
        "nome_partido":                 rec.get("nomePartido"),
        # Senator's own party affiliation
        "codigo_partido_filiacao":      str(rec.get("codigoPartidoFiliacao") or ""),
        "sigla_partido_filiacao":       rec.get("siglaPartidoFiliacao"),
        "nome_partido_filiacao":        rec.get("nomePartidoFiliacao"),
    }
