"""Flatten function for housing allowance (Auxílio-Moradia) snapshot records.

Key quirk: No senator ID in the API response — only ``nomeParlamentar`` is
available. Matching to ``dim_senador`` is done by name in the dbt layer.
Boolean fields arrive as Python booleans from the JSON parser.
"""


def flatten_auxilio_moradia_record(rec: dict) -> dict:
    """Flatten one record from GET /api/v1/senadores/auxilio-moradia."""
    return {
        "nome_parlamentar": rec.get("nomeParlamentar"),
        "estado_eleito":    rec.get("estadoEleito"),
        "partido_eleito":   rec.get("partidoEleito"),
        "auxilio_moradia":  bool(rec.get("auxilioMoradia", False)),
        "imovel_funcional": bool(rec.get("imovelFuncional", False)),
    }
