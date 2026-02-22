"""
Extract committee master list and senator committee memberships from the Senate LEGIS API.

Primary endpoints for the master committee list
(in order of preference — see docs/comissoes_endpoints_comparison.md):

  GET /comissao/lista/colegiados
  -- Returns ALL active committees across the National Congress (SF + CN + CD).
  -- Flat JSON structure; replaces the old per-tipo loop.
  -- Shape: ListaColegiados.Colegiados.Colegiado[]
  -- Each record: Codigo, Sigla, Nome, SiglaCasa, SiglaTipoColegiado,
     DescricaoTipoColegiado, CodigoTipoColegiado, DataInicio, Publica, Finalidade

  GET /comissao/lista/mistas
  -- Returns joint Congress (CN) committees with member-count breakdown.
  -- Augments colegiados records that already exist; adds records that don't.
  -- Shape: ComissoesMistasCongresso.Colegiados.Colegiado[]
  -- Each record: CodigoColegiado, NomeColegiado, SiglaColegiado,
     QuantidadesMembros.{Titulares, SenadoresTitulares, DeputadosTitulares}

Membership endpoint (per senator):

  GET /senador/{code}/comissoes.json
  -- Returns full committee membership history for one senator.
  -- Called for each of the 81 current senators.
  -- Shape: MembroComissaoParlamentar.Parlamentar.MembroComissoes.Comissao[]

Outputs:
  data/raw/comissoes.parquet        — unified committee master list
  data/raw/membros_comissao.parquet — membership history (senator × committee × period)

Legacy note:
  The old approach used /comissao/lista/{tipo} for tipo in (permanente, cpi, temporaria,
  orgaos). These four endpoints return a subset of what /comissao/lista/colegiados
  returns, with a more complex nested structure. They are no longer used here.
  See docs/comissoes_endpoints_comparison.md for the full comparison.
"""

import sys
from pathlib import Path

import polars as pl

from api_client import SenateApiClient

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

RAW_DIR = Path("data/raw")


# ---------------------------------------------------------------------------
# Flatten helpers
# ---------------------------------------------------------------------------


def _flatten_colegiado(c: dict) -> dict:
    """Flatten one record from GET /comissao/lista/colegiados.

    Fields are flat PascalCase — no nested tipo sub-object.
    'Publica' flag is 'S'/'N'; converted to bool.
    """
    return {
        "codigo_comissao":  str(c.get("Codigo") or ""),
        "sigla_comissao":   c.get("Sigla"),
        "nome_comissao":    c.get("Nome"),
        "finalidade":       c.get("Finalidade"),
        "sigla_casa":       c.get("SiglaCasa"),
        "codigo_tipo":      c.get("CodigoTipoColegiado"),
        "sigla_tipo":       c.get("SiglaTipoColegiado"),
        "descricao_tipo":   c.get("DescricaoTipoColegiado"),
        "data_inicio":      c.get("DataInicio"),
        "data_fim":         c.get("DataFim"),
        "publica":          c.get("Publica") == "S" if c.get("Publica") else None,
        # mistas-only fields — null for this source
        "qtd_titulares":             None,
        "qtd_senadores_titulares":   None,
        "qtd_deputados_titulares":   None,
        "fonte":            "colegiados",
    }


def _flatten_mista(c: dict) -> dict:
    """Flatten one record from GET /comissao/lista/mistas.

    Uses CodigoColegiado / NomeColegiado / SiglaColegiado keys (not Codigo/Nome/Sigla).
    Member counts are strings in the API — cast to int.
    """
    qtd = c.get("QuantidadesMembros") or {}

    def _int(val: str | None) -> int | None:
        try:
            return int(val) if val else None
        except (ValueError, TypeError):
            return None

    return {
        "codigo_comissao":  str(c.get("CodigoColegiado") or ""),
        "sigla_comissao":   c.get("SiglaColegiado"),
        "nome_comissao":    c.get("NomeColegiado"),
        "finalidade":       c.get("Finalidade"),
        "sigla_casa":       "CN",
        "codigo_tipo":      None,
        "sigla_tipo":       "MISTA",
        "descricao_tipo":   "Comissão Mista",
        "data_inicio":      None,
        "data_fim":         None,
        "publica":          None,
        "qtd_titulares":             _int(qtd.get("Titulares")),
        "qtd_senadores_titulares":   _int(qtd.get("SenadoresTitulares")),
        "qtd_deputados_titulares":   _int(qtd.get("DeputadosTitulares")),
        "fonte":            "mistas",
    }


def _flatten_membro(senador_id: str, comissao: dict) -> dict:
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unwrap_list(obj: list | dict | None) -> list:
    """Return a guaranteed list, wrapping dict singletons from XML→JSON conversion."""
    if obj is None:
        return []
    return [obj] if isinstance(obj, dict) else obj


def _get_senator_ids(client: SenateApiClient) -> list[str]:
    """Read senator IDs from the already-extracted Parquet file."""
    parquet = RAW_DIR / "senadores.parquet"
    if parquet.exists():
        df = pl.read_parquet(parquet)
        return df["senador_id"].drop_nulls().unique().to_list()
    data = client.get_legis("/senador/lista/atual")
    senators = _unwrap_list(
        data.get("ListaParlamentarEmExercicio", {})
            .get("Parlamentares", {})
            .get("Parlamentar")
    )
    return [
        str(s.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar", ""))
        for s in senators
        if s.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar")
    ]


# ---------------------------------------------------------------------------
# Main extraction
# ---------------------------------------------------------------------------


def extract_all() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with SenateApiClient() as client:

        # --- 1. Primary master list: /comissao/lista/colegiados (all active, flat) ---
        print("Fetching all active committees (/comissao/lista/colegiados)...", end=" ", flush=True)
        raw = client.get_legis("/comissao/lista/colegiados")
        colegiados_raw = _unwrap_list(
            raw.get("ListaColegiados", {})
               .get("Colegiados", {})
               .get("Colegiado")
        )
        comissoes: dict[str, dict] = {}
        for c in colegiados_raw:
            flat = _flatten_colegiado(c)
            if flat["codigo_comissao"]:
                comissoes[flat["codigo_comissao"]] = flat
        print(f"{len(colegiados_raw)} records → {len(comissoes)} unique committees")

        # --- 2. Augment with joint Congress committees (/comissao/lista/mistas) ---
        print("Fetching mixed Congress committees (/comissao/lista/mistas)...", end=" ", flush=True)
        raw = client.get_legis("/comissao/lista/mistas")
        mistas_raw = _unwrap_list(
            raw.get("ComissoesMistasCongresso", {})
               .get("Colegiados", {})
               .get("Colegiado")
        )
        new_from_mistas = 0
        for c in mistas_raw:
            flat = _flatten_mista(c)
            codigo = flat["codigo_comissao"]
            if not codigo:
                continue
            if codigo in comissoes:
                # Augment existing colegiados record with member-count data
                comissoes[codigo]["qtd_titulares"]           = flat["qtd_titulares"]
                comissoes[codigo]["qtd_senadores_titulares"] = flat["qtd_senadores_titulares"]
                comissoes[codigo]["qtd_deputados_titulares"] = flat["qtd_deputados_titulares"]
                # Mark merged fonte
                comissoes[codigo]["fonte"] = "colegiados+mistas"
            else:
                # Committee in mistas but not in colegiados — add it
                comissoes[codigo] = flat
                new_from_mistas += 1
        augmented = sum(1 for c in comissoes.values() if c["fonte"] == "colegiados+mistas")
        print(f"{len(mistas_raw)} mixed committees | {augmented} augmented | {new_from_mistas} new")

        # --- 3. Senator membership history ---
        senator_ids = _get_senator_ids(client)
        print(f"Fetching committee memberships for {len(senator_ids)} senators...")

        all_membros: list[dict] = []
        for i, senador_id in enumerate(senator_ids, 1):
            label = f"[{i:>3}/{len(senator_ids)}] senator {senador_id}"
            try:
                raw = client.get_legis(f"/senador/{senador_id}/comissoes")
                parlamentar = (
                    raw.get("MembroComissaoParlamentar", {})
                       .get("Parlamentar", {})
                )
                comissoes_membro = _unwrap_list(
                    parlamentar.get("MembroComissoes", {}).get("Comissao")
                )
                membros = [_flatten_membro(senador_id, c) for c in comissoes_membro]
                all_membros.extend(membros)
                print(f"  {label}  memberships={len(membros)}")
            except Exception as e:
                print(f"  {label}  ERROR: {e}")

    # --- Save committee master ---
    all_comissoes = list(comissoes.values())
    if all_comissoes:
        df_comissoes = pl.DataFrame(all_comissoes)
        out_comissoes = RAW_DIR / "comissoes.parquet"
        df_comissoes.write_parquet(out_comissoes)
        print(f"\nSaved {len(df_comissoes)} committees → {out_comissoes}")
        # Summary by fonte
        print(df_comissoes.group_by("fonte").agg(pl.len().alias("n")).sort("fonte"))
    else:
        print("\nWARNING: No committee master data saved.")

    # --- Save membership history ---
    if all_membros:
        df_membros = (
            pl.DataFrame(all_membros)
            .filter(
                pl.col("senador_id").is_not_null()
                & pl.col("codigo_comissao").is_not_null()
                & (pl.col("codigo_comissao") != "")
            )
        )
        out_membros = RAW_DIR / "membros_comissao.parquet"
        df_membros.write_parquet(out_membros)
        print(f"Saved {len(df_membros)} membership records → {out_membros}")
    else:
        print("WARNING: No membership data saved.")


if __name__ == "__main__":
    extract_all()
