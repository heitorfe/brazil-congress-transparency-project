"""Flatten function for CEAPS expense reimbursement records.

Key quirk: ``codSenador`` is an integer in the ADM API response.
Stored as string so it can join ``dim_senador.senador_id`` (VARCHAR).
"""


def flatten_ceaps_record(rec: dict) -> dict:
    """Flatten one CEAPS record from GET /api/v1/senadores/despesas_ceaps/{ano}."""
    return {
        "id":                rec.get("id"),
        "tipo_documento":    rec.get("tipoDocumento"),
        "ano":               rec.get("ano"),
        "mes":               rec.get("mes"),
        "cod_senador":       str(rec.get("codSenador") or ""),
        "nome_senador":      rec.get("nomeSenador"),
        "tipo_despesa":      rec.get("tipoDespesa"),
        "cnpj_cpf":          rec.get("cpfCnpj"),
        "fornecedor":        rec.get("fornecedor"),
        "documento":         rec.get("documento"),
        "data":              rec.get("data"),
        "detalhamento":      rec.get("detalhamento"),
        "valor_reembolsado": rec.get("valorReembolsado"),
    }
