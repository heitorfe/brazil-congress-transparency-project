"""Flatten functions for staff, pensioner, payroll, and overtime data.
"""


def flatten_servidor(rec: dict) -> dict:
    """Flatten one staff registry record from GET /api/v1/servidores/servidores."""
    return {
        "sequencial":           rec.get("sequencial"),
        "nome":                 rec.get("nome"),
        "vinculo":              rec.get("vinculo"),
        "situacao":             rec.get("situacao"),
        "cargo_nome":           (rec.get("cargo") or {}).get("nome"),
        "padrao":               rec.get("padrao"),
        "especialidade":        rec.get("especialidade"),
        "funcao_nome":          (rec.get("funcao") or {}).get("nome"),
        "lotacao_sigla":        (rec.get("lotacao") or {}).get("sigla"),
        "lotacao_nome":         (rec.get("lotacao") or {}).get("nome"),
        "categoria_codigo":     (rec.get("categoria") or {}).get("codigo"),
        "categoria_nome":       (rec.get("categoria") or {}).get("nome"),
        "cedido_tipo":          (rec.get("cedido") or {}).get("tipo_cessao"),
        "cedido_orgao_origem":  (rec.get("cedido") or {}).get("orgao_origem"),
        "cedido_orgao_destino": (rec.get("cedido") or {}).get("orgao_destino"),
        "ano_admissao":         rec.get("ano_admissao"),
    }


def flatten_pensionista(rec: dict) -> dict:
    """Flatten one pensioner registry record from GET /api/v1/servidores/pensionistas."""
    return {
        "sequencial":         rec.get("sequencial"),
        "nome":               rec.get("nome"),
        "vinculo":            rec.get("vinculo"),
        "fundamento":         rec.get("fundamento"),
        "cargo_nome":         (rec.get("cargo") or {}).get("nome"),
        "funcao_nome":        (rec.get("funcao") or {}).get("nome"),
        "categoria_codigo":   (rec.get("categoria") or {}).get("codigo"),
        "categoria_nome":     (rec.get("categoria") or {}).get("nome"),
        "nome_instituidor":   rec.get("nome_instituidor"),
        "ano_exercicio":      rec.get("ano_exercicio"),
        "data_obito":         rec.get("data_obito"),
        "data_inicio_pensao": rec.get("data_inicio_pensao"),
    }


def flatten_remuneracao(rec: dict, ano: int, mes: int) -> dict:
    """Flatten one staff payroll record from GET /api/v1/servidores/remuneracoes/{ano}/{mes}."""
    return {
        "sequencial":                   rec.get("sequencial"),
        "nome":                         rec.get("nome"),
        "ano":                          ano,
        "mes":                          mes,
        "tipo_folha":                   rec.get("tipo_folha"),
        "remuneracao_basica":           rec.get("remuneracao_basica"),
        "vantagens_pessoais":           rec.get("vantagens_pessoais"),
        "funcao_comissionada":          rec.get("funcao_comissionada"),
        "gratificacao_natalina":        rec.get("gratificacao_natalina"),
        "horas_extras":                 rec.get("horas_extras"),
        "outras_eventuais":             rec.get("outras_eventuais"),
        "diarias":                      rec.get("diarias"),
        "auxilios":                     rec.get("auxilios"),
        "faltas":                       rec.get("faltas"),
        "previdencia":                  rec.get("previdencia"),
        "abono_permanencia":            rec.get("abono_permanencia"),
        "reversao_teto_constitucional": rec.get("reversao_teto_constitucional"),
        "imposto_renda":                rec.get("imposto_renda"),
        "remuneracao_liquida":          rec.get("remuneracao_liquida"),
        "vantagens_indenizatorias":     rec.get("vantagens_indenizatorias"),
    }


def flatten_remuneracao_pensionista(rec: dict, ano: int, mes: int) -> dict:
    """Flatten one pensioner payroll record from GET /api/v1/servidores/pensionistas/remuneracoes/{ano}/{mes}."""
    return {
        "sequencial":                   rec.get("sequencial"),
        "nome":                         rec.get("nome"),
        "ano":                          ano,
        "mes":                          mes,
        "tipo_folha":                   rec.get("tipo_folha"),
        "remuneracao_basica":           rec.get("remuneracao_basica"),
        "vantagens_pessoais":           rec.get("vantagens_pessoais"),
        "funcao_comissionada":          rec.get("funcao_comissionada"),
        "gratificacao_natalina":        rec.get("gratificacao_natalina"),
        "reversao_teto_constitucional": rec.get("reversao_teto_constitucional"),
        "imposto_renda":                rec.get("imposto_renda"),
        "remuneracao_liquida":          rec.get("remuneracao_liquida"),
        "vantagens_indenizatorias":     rec.get("vantagens_indenizatorias"),
        "previdencia":                  rec.get("previdencia"),
    }


def flatten_hora_extra(rec: dict, ano: int, mes: int) -> dict:
    """Flatten one overtime record from GET /api/v1/servidores/horas-extras/{ano}/{mes}."""
    return {
        "sequencial":        rec.get("sequencial"),
        "nome":              rec.get("nome"),
        "valor_total":       rec.get("valorTotal"),
        "mes_ano_prestacao": rec.get("mes_ano_prestacao"),
        "mes_ano_pagamento": rec.get("mes_ano_pagamento"),
        "ano_pagamento":     ano,
        "mes_pagamento":     mes,
    }
