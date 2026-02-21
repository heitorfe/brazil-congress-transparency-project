"""
Extract committee master list and senator committee memberships from the Senate LEGIS API.

Endpoints used:
  GET /comissao/lista.json
  -- Returns the full list of Senate committees (sigla, name, type, dates).

  GET /senador/{code}/comissoes.json
  -- Returns the full committee membership history for one senator.
  -- Called for each of the 81 current senators to build a complete history.

Outputs:
  data/raw/comissoes.parquet       — committee master list (1 row per committee)
  data/raw/membros_comissao.parquet — membership history (1 row per senator × committee × period)

Key quirks:
  - The membership endpoint uses PascalCase keys nested under
    MembroComissaoParlamentar.Parlamentar.MembroComissoes.Comissao[].
  - IdentificacaoComissao sub-object contains committee identifiers.
  - Singleton guard: if Comissao is a dict instead of list, wrap it.
"""

import sys
from pathlib import Path

import polars as pl

from api_client import SenateApiClient

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

RAW_DIR = Path("data/raw")


def _flatten_comissao(c: dict) -> dict:
    """Flatten one record from the /comissao/lista response."""
    return {
        "codigo_comissao": str(c.get("CodigoComissao") or c.get("codigoComissao") or ""),
        "sigla_comissao":  c.get("SiglaComissao") or c.get("siglaComissao"),
        "nome_comissao":   c.get("NomeComissao") or c.get("nomeComissao"),
        "sigla_casa":      c.get("SiglaCasaComissao") or c.get("siglaCasa"),
        "tipo":            c.get("TipoComissao") or c.get("tipo"),
        "data_inicio":     c.get("DataCriacao") or c.get("dataInicio"),
        "data_fim":        c.get("DataExtincao") or c.get("dataFim"),
    }


def _flatten_membro(senador_id: str, comissao: dict) -> dict:
    """Flatten one committee membership record from /senador/{code}/comissoes."""
    ident = comissao.get("IdentificacaoComissao") or {}
    return {
        "senador_id":           senador_id,
        "codigo_comissao":      str(ident.get("CodigoComissao") or ""),
        "sigla_comissao":       ident.get("SiglaComissao"),
        "nome_comissao":        ident.get("NomeComissao"),
        "sigla_casa":           ident.get("SiglaCasaComissao"),
        "descricao_participacao": comissao.get("DescricaoParticipacao"),
        "data_inicio":          comissao.get("DataInicio"),
        "data_fim":             comissao.get("DataFim"),
    }


def _get_senator_ids(client: SenateApiClient) -> list[str]:
    """Read senator IDs from the already-extracted Parquet file."""
    parquet = RAW_DIR / "senadores.parquet"
    if parquet.exists():
        df = pl.read_parquet(parquet)
        return df["senador_id"].drop_nulls().unique().to_list()
    # Fallback: call the API directly
    data = client.get_legis("/senador/lista/atual")
    senators = (
        data.get("ListaParlamentarEmExercicio", {})
            .get("Parlamentares", {})
            .get("Parlamentar", [])
    )
    if isinstance(senators, dict):
        senators = [senators]
    return [
        str(s.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar", ""))
        for s in senators
        if s.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar")
    ]


def extract_all() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with SenateApiClient() as client:
        # --- 1. Committee master list ---
        print("Fetching committee master list from /comissao/lista...", end=" ", flush=True)
        try:
            raw_lista = client.get_legis("/comissao/lista")
            # Response is typically: {"ListaComissoes": {"Comissao": [...]}}
            comissoes_raw = (
                raw_lista.get("ListaComissoes", {}).get("Comissao")
                or raw_lista.get("Comissao")
                or (raw_lista if isinstance(raw_lista, list) else [])
            )
            if isinstance(comissoes_raw, dict):
                comissoes_raw = [comissoes_raw]
            comissoes = [_flatten_comissao(c) for c in comissoes_raw if c]
            print(f"{len(comissoes)} committees")
        except Exception as e:
            print(f"ERROR: {e}")
            comissoes = []

        # --- 2. Senator membership history ---
        senator_ids = _get_senator_ids(client)
        print(f"Fetching committee memberships for {len(senator_ids)} senators...")

        all_membros: list[dict] = []
        for i, senador_id in enumerate(senator_ids, 1):
            label = f"[{i:>3}/{len(senator_ids)}] senator {senador_id}"
            try:
                raw = client.get_legis(f"/senador/{senador_id}/comissoes")
                # Navigate: MembroComissaoParlamentar.Parlamentar.MembroComissoes.Comissao
                parlamentar = (
                    raw.get("MembroComissaoParlamentar", {})
                       .get("Parlamentar", {})
                )
                comissoes_membro = (
                    parlamentar.get("MembroComissoes", {}).get("Comissao") or []
                )
                if isinstance(comissoes_membro, dict):
                    comissoes_membro = [comissoes_membro]
                membros = [_flatten_membro(senador_id, c) for c in comissoes_membro]
                all_membros.extend(membros)
                print(f"  {label}  memberships={len(membros)}")
            except Exception as e:
                print(f"  {label}  ERROR: {e}")

    # --- Save committee master ---
    if comissoes:
        df_comissoes = (
            pl.DataFrame(comissoes)
            .unique(subset=["codigo_comissao"])
            .filter(pl.col("codigo_comissao") != "")
        )
        out_comissoes = RAW_DIR / "comissoes.parquet"
        df_comissoes.write_parquet(out_comissoes)
        print(f"\nSaved {len(df_comissoes)} committees → {out_comissoes}")
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
