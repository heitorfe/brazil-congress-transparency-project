"""
Flatten functions for all Senate and Chamber data domains.

Re-exports every public flatten function so callers can import from the
top-level package without knowing which submodule a function lives in:

    from transforms import flatten_senator, flatten_mandate
    # equivalent to:
    from transforms.senators import flatten_senator, flatten_mandate

Each submodule corresponds to one data domain and contains only pure
dict-in / dict-out transformation functions — no I/O, no API calls.
"""

from .senators import flatten_senator, flatten_mandate
from .votacoes import flatten_votacao, flatten_voto
from .comissoes import flatten_colegiado, flatten_mista, flatten_membro
from .servidores import (
    flatten_servidor,
    flatten_pensionista,
    flatten_remuneracao,
    flatten_remuneracao_pensionista,
    flatten_hora_extra,
)
from .ceaps import flatten_ceaps_record
from .liderancas import flatten_lideranca_record
from .processos import flatten_processo_record
from .auxilio_moradia import flatten_auxilio_moradia_record

# Chamber of Deputies (Câmara dos Deputados)
from .camara_deputados import flatten_deputado_list, flatten_deputado_detail
from .camara_despesas import flatten_despesa_deputado
from .camara_proposicoes import flatten_proposicao
from .camara_votacoes import flatten_votacao_camara, flatten_voto_camara

__all__ = [
    "flatten_senator",
    "flatten_mandate",
    "flatten_votacao",
    "flatten_voto",
    "flatten_colegiado",
    "flatten_mista",
    "flatten_membro",
    "flatten_servidor",
    "flatten_pensionista",
    "flatten_remuneracao",
    "flatten_remuneracao_pensionista",
    "flatten_hora_extra",
    "flatten_ceaps_record",
    "flatten_lideranca_record",
    "flatten_processo_record",
    "flatten_auxilio_moradia_record",
    # Chamber
    "flatten_deputado_list",
    "flatten_deputado_detail",
    "flatten_despesa_deputado",
    "flatten_proposicao",
    "flatten_votacao_camara",
    "flatten_voto_camara",
]
