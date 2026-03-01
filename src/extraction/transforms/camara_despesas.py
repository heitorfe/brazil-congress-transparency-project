"""Flatten function for Chamber deputy CEAP expense records.

Key differences from Senate CEAPS (stg_adm__ceaps):
  - Values are already floats (not Brazilian-locale strings) — no need for
    the REPLACE/CAST trick used for Senate ADM data.
  - codDocumento is the natural dedup key (not a separate "id" field).
  - deputado_id must be injected by the extractor (not present in the record).
"""


def flatten_despesa_deputado(deputado_id: str, rec: dict) -> dict:
    """Flatten one expense record from GET /deputados/{id}/despesas."""
    return {
        "cod_documento":       str(rec.get("codDocumento") or ""),
        "deputado_id":         deputado_id,
        "ano":                 rec.get("ano"),
        "mes":                 rec.get("mes"),
        "tipo_despesa":        rec.get("tipoDespesa"),
        "cod_tipo_documento":  rec.get("codTipoDocumento"),
        "tipo_documento":      rec.get("tipoDocumento"),
        "data_documento":      rec.get("dataDocumento"),
        "num_documento":       rec.get("numDocumento"),
        "valor_documento":     rec.get("valorDocumento"),
        "url_documento":       rec.get("urlDocumento"),
        "nome_fornecedor":     rec.get("nomeFornecedor"),
        "cnpj_cpf_fornecedor": rec.get("cnpjCpfFornecedor"),
        "valor_liquido":       rec.get("valorLiquido"),
        "valor_glosa":         rec.get("valorGlosa"),
        "num_ressarcimento":   rec.get("numRessarcimento"),
        "cod_lote":            str(rec.get("codLote") or ""),
        "parcela":             rec.get("parcela"),
    }
