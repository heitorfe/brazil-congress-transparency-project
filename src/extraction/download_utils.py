"""
Shared HTTP download utilities for bulk file extractors.

Provides:
- download_file()     : Streaming download with HTTP Range resume support
- safe_extract_zip()  : ZIP extraction with path traversal and zip-bomb guards
- validate_csv()      : Quick encoding/column-count validation

Pattern (ported from br-acc etl/scripts/_download_utils.py):
  Write to .partial file during download, rename to final name on success.
  This ensures a partially-downloaded file is never mistaken for a complete one.
"""

import zipfile
from pathlib import Path

import requests


def download_file(
    url: str,
    dest: Path,
    *,
    timeout: int = 600,
    chunk_size: int = 1024 * 1024,
) -> bool:
    """Download a file to dest with HTTP Range resume support.

    If dest.parent/<dest.name>.partial exists, resumes from its current size.
    Renames .partial to dest on success.

    Returns True if file was downloaded (or already complete), False on error.
    """
    if dest.exists():
        return True

    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_suffix(dest.suffix + ".partial")
    start_byte = partial.stat().st_size if partial.exists() else 0

    headers: dict[str, str] = {}
    if start_byte > 0:
        headers["Range"] = f"bytes={start_byte}-"
        print(f"  Resuming from byte {start_byte:,} ...", end=" ", flush=True)

    try:
        resp = requests.get(url, headers=headers, stream=True, timeout=timeout)
    except requests.RequestException as e:
        print(f"ERROR (network): {e}")
        return False

    if resp.status_code == 416:
        # Range not satisfiable → file is already complete
        partial.rename(dest)
        return True

    if resp.status_code not in (200, 206):
        print(f"ERROR (HTTP {resp.status_code})")
        return False

    if resp.status_code == 200 and start_byte > 0:
        # Server ignored Range header → restart
        start_byte = 0
        partial.unlink(missing_ok=True)

    mode = "ab" if start_byte > 0 else "wb"
    total_bytes = start_byte
    try:
        with open(partial, mode) as fh:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    fh.write(chunk)
                    total_bytes += len(chunk)
    except OSError as e:
        print(f"ERROR (write): {e}")
        return False

    partial.rename(dest)
    return True


def safe_extract_zip(
    zip_path: Path,
    output_dir: Path,
    *,
    max_total_bytes: int = 2 * 1024**3,
) -> list[Path]:
    """Extract ZIP with path traversal guard and zip-bomb protection.

    Returns the list of extracted file paths.
    Deletes the ZIP if it is corrupted (so it will be re-downloaded next run).

    max_total_bytes defaults to 2 GB — suitable for CEAP yearly ZIPs.
    The larger 50 GB limit in br-acc is for CNPJ data; we use 2 GB here.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []

    try:
        with zipfile.ZipFile(zip_path) as zf:
            total_size = sum(info.file_size for info in zf.infolist())
            if total_size > max_total_bytes:
                raise ValueError(
                    f"ZIP uncompressed size {total_size:,} bytes exceeds "
                    f"limit of {max_total_bytes:,} bytes (possible zip bomb)."
                )

            for member in zf.infolist():
                # Guard against path traversal: e.g. "../../etc/passwd"
                safe_path = output_dir / Path(member.filename).name
                if not safe_path.resolve().is_relative_to(output_dir.resolve()):
                    raise ValueError(f"Path traversal attempt in ZIP: {member.filename}")

                zf.extract(member, output_dir)
                extracted.append(output_dir / member.filename)

    except zipfile.BadZipFile as e:
        print(f"  WARNING: Corrupted ZIP, deleting for re-download: {e}")
        zip_path.unlink(missing_ok=True)
        return []

    return extracted


def validate_csv(path: Path, *, encoding: str = "latin-1", sep: str = ";") -> bool:
    """Read first 10 rows to verify encoding and column count.

    Returns True if the file is readable with the given encoding and has at least
    2 columns. Returns False on any error (encoding error, empty file, etc.).
    """
    try:
        import pandas as pd

        df = pd.read_csv(
            path,
            encoding=encoding,
            sep=sep,
            nrows=10,
            on_bad_lines="skip",
        )
        return len(df.columns) >= 2 and len(df) > 0
    except Exception as e:
        print(f"  WARNING: CSV validation failed for {path.name}: {e}")
        return False
