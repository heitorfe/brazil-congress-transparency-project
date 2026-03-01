"""Flatten functions for Chamber deputy list and biographical data.

Two-stage extraction:
  1. flatten_deputado_list()   — from GET /deputados?idLegislatura={n}
                                  produces camara_deputados_lista.parquet
  2. flatten_deputado_detail() — from GET /deputados/{id}
                                  produces camara_deputados.parquet

The list parquet records which legislature(s) a deputy belonged to.
The detail parquet holds biographical data (nomeCivil, sexo, dataNascimento, etc.)
from the ultimoStatus sub-object.
"""


def flatten_deputado_list(rec: dict, legislatura_id: int) -> dict:
    """Flatten one record from GET /deputados?idLegislatura={n}.

    Each row represents one deputy × one legislature (a deputy may appear
    in both legislature 56 and 57 — they get two rows here).
    """
    return {
        "deputado_id":    str(rec.get("id") or ""),
        "nome":           rec.get("nome"),
        "sigla_partido":  rec.get("siglaPartido"),
        "sigla_uf":       rec.get("siglaUf"),
        "id_legislatura": int(rec.get("idLegislatura") or legislatura_id),
        "url_foto":       rec.get("urlFoto"),
        "email":          rec.get("email"),
    }


def flatten_deputado_detail(rec: dict) -> dict:
    """Flatten one record from GET /deputados/{id}.

    Uses the ``ultimoStatus`` sub-object for current-status fields (party,
    state, situation). Falls back to top-level fields for biography.
    """
    status = rec.get("ultimoStatus") or {}
    gabinete = status.get("gabinete") or {}
    return {
        "deputado_id":          str(rec.get("id") or ""),
        "nome_civil":           rec.get("nomeCivil"),
        "nome_parlamentar":     status.get("nome"),
        "nome_eleitoral":       status.get("nomeEleitoral"),
        "sigla_partido":        status.get("siglaPartido"),
        "sigla_uf":             status.get("siglaUf"),
        "id_legislatura":       int(status.get("idLegislatura") or 0),
        "url_foto":             status.get("urlFoto"),
        "email":                status.get("email"),
        "situacao":             status.get("situacao"),
        "condicao_eleitoral":   status.get("condicaoEleitoral"),
        "descricao_status":     status.get("descricaoStatus"),
        "data_status":          status.get("data"),
        "sexo":                 rec.get("sexo"),
        "data_nascimento":      rec.get("dataNascimento"),
        "uf_nascimento":        rec.get("ufNascimento"),
        "municipio_nascimento": rec.get("municipioNascimento"),
        "escolaridade":         rec.get("escolaridade"),
        "telefone_gabinete":    gabinete.get("telefone"),
    }
