"""Flatten function for legislative proposal (processo) records.

Key quirks:
  - The ``id`` field maps to ``id_processo`` in our schema.
  - ``sigla_materia``, ``numero_materia``, and ``ano_materia`` are parsed from
    the ``identificacao`` string (e.g. "PL 1234/2025").
  - ``tramitando`` is "Sim" / "Não" string, not boolean — converted in dbt staging.
  - ``dataUltimaAtualizacao`` can be absent in some records.
"""


def flatten_processo_record(rec: dict) -> dict:
    """Flatten one proposal record from GET /processo?sigla={sigla}&ano={ano}."""
    ident = rec.get("identificacao") or ""
    parts = ident.split(" ", 1) if " " in ident else [ident, ""]
    num_ano = parts[1].split("/") if "/" in parts[1] else [parts[1], None]
    return {
        "id_processo":             rec.get("id"),
        "codigo_materia":          rec.get("codigoMateria"),
        "identificacao":           ident or None,
        "sigla_materia":           parts[0] or None,
        "numero_materia":          num_ano[0] or None,
        "ano_materia":             int(num_ano[1]) if num_ano[1] else None,
        "ementa":                  rec.get("ementa"),
        "tipo_documento":          rec.get("tipoDocumento"),
        "data_apresentacao":       rec.get("dataApresentacao"),
        "autoria":                 rec.get("autoria"),
        "casa_identificadora":     rec.get("casaIdentificadora"),
        "tramitando":              rec.get("tramitando"),
        "data_ultima_atualizacao": rec.get("dataUltimaAtualizacao"),
        "url_documento":           rec.get("urlDocumento"),
    }
