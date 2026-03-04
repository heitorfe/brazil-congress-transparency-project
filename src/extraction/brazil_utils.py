"""
Shared utilities for Brazilian public data extraction.

Covers:
- BRL monetary string parsing  ("1.234,56" → 1234.56)
- Name normalization            (NFKD accent removal, uppercase, whitespace collapse)
- CPF / CNPJ formatting        (digits → formatted string)
- Stable hash IDs               (SHA256[:16] for expense records without native IDs)
- Contract value capping        (R$10B cap for data entry error protection)

Ported and adapted from br-acc etl/src/bracc_etl/transforms/ (v0.3.1, 2026-03-04).
"""

import hashlib
import re
import unicodedata


# ── BRL monetary values ───────────────────────────────────────────────────────


def parse_brl_value(value: str | float | None) -> float:
    """Parse a Brazilian monetary string to float.

    Handles:
      '1.234,56'   → 1234.56
      'R$ 36.380,05' → 36380.05
      1234.56      → 1234.56  (already a number, pass-through)

    Returns 0.0 on empty/invalid input.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = re.sub(r"[R$\s]", "", str(value).strip())
    if not cleaned:
        return 0.0
    # Brazilian format: '.' = thousands separator, ',' = decimal separator
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


CAP_CONTRACT_VALUE = 10_000_000_000.0  # R$10B


def cap_value(value: float | None) -> float | None:
    """Return None for values above R$10B.

    Portal da Transparência and PNCP occasionally contain data-entry errors
    (e.g. R$1 trillion contracts). Capping at R$10B protects aggregations.
    The raw_value field should be preserved for auditability in the dbt model.
    """
    if value is None:
        return None
    return None if value > CAP_CONTRACT_VALUE else value


# ── Name normalization ────────────────────────────────────────────────────────


def normalize_name(name: str | None) -> str:
    """Normalize a Brazilian person/organization name for fuzzy joining.

    Pipeline: strip → uppercase → NFKD decomposition → remove combining chars
    → collapse internal whitespace.

    Examples:
        'José da Silva'  → 'JOSE DA SILVA'
        'Ângela Pessoa'  → 'ANGELA PESSOA'
        '  MARCOS  '    → 'MARCOS'

    IMPORTANT: Uses NFKD (compatibility decomposition) rather than NFC/NFD
    so that ligatures and compatibility characters are also decomposed.
    This matches the br-acc behavior in transforms/name_normalization.py.
    """
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name.strip().upper())
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", ascii_name)


# ── Document formatting (CPF / CNPJ) ─────────────────────────────────────────


def strip_document(doc: str | None) -> str:
    """Return only digits from a CPF or CNPJ string.

    '123.456.789-01' → '12345678901'
    '12.345.678/0001-99' → '12345678000199'
    """
    if not doc:
        return ""
    return re.sub(r"\D", "", str(doc))


def format_cpf(raw: str) -> str:
    """Format 11-digit CPF string as 'NNN.NNN.NNN-NN'.

    Returns the raw digit string unchanged if it doesn't have exactly 11 digits.
    """
    digits = strip_document(raw)
    if len(digits) != 11:
        return digits
    return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"


def format_cnpj(raw: str) -> str:
    """Format 14-digit CNPJ string as 'NN.NNN.NNN/NNNN-NN'.

    Returns the raw digit string unchanged if it doesn't have exactly 14 digits.
    """
    digits = strip_document(raw)
    if len(digits) != 14:
        return digits
    return f"{digits[:2]}.{digits[2:5]}.{digits[5:8]}/{digits[8:12]}-{digits[12:]}"


def classify_document(doc: str) -> str:
    """Return 'CPF', 'CNPJ', or 'unknown' based on digit count."""
    digits = strip_document(doc)
    if len(digits) == 11:
        return "CPF"
    if len(digits) == 14:
        return "CNPJ"
    return "unknown"


# ── Stable hash IDs ───────────────────────────────────────────────────────────


def stable_hash_id(raw: str, prefix: str = "") -> str:
    """Generate a 16-character stable ID from a composite key string.

    Uses SHA256 truncated to 16 hex characters. Collision probability is
    negligible for datasets under 50M records (birthday bound ~1/2^32).

    Usage:
        expense_id = stable_hash_id(
            f"{senator_name}|{date}|{supplier_doc}|{value}",
            prefix="ceaps_senado"
        )

    Pattern from br-acc SenadoPipeline (expense ID generation).
    """
    full = f"{prefix}_{raw}" if prefix else raw
    return hashlib.sha256(full.encode()).hexdigest()[:16]
