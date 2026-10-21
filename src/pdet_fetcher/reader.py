import collections
import csv
import logging
import re
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

try:
    import polars as pl
    from quantilica.analytics.writer import to_parquet
except ImportError as exc:
    raise ImportError(
        "pdet_fetcher.reader requer o extra 'analysis': "
        "uv add 'pdet-fetcher[analysis]' "
        "--index https://index.quantilica.com/simple/"
    ) from exc
from quantilica.core.exceptions import ParseError, StorageError
from quantilica.core.files import decompress_archive

from .constants import (
    BOOLEAN_CODES,
    BOOLEAN_COLUMNS,
    CAGED_AJUSTES_HEADER_NAMES,
    CAGED_HEADER_NAMES,
    INTEGER_COLUMNS,
    NA_VALUES,
    NOVO_CAGED_HEADER_NAMES,
    NUMERIC_COLUMNS,
    RAGGED_CSV_FILES,
    RAIS_ESTABELECIMENTOS_1985_2001_HEADER_NAMES,
    RAIS_ESTABELECIMENTOS_HEADER_NAMES,
    RAIS_VINCULOS_HEADER_NAMES,
)

# Encoding and header map of each CAGED dataset.
_CAGED_FORMATS: dict[str, tuple[str, dict[str, str | tuple[str, ...]]]] = {
    "caged": ("latin-1", CAGED_HEADER_NAMES),
    "caged-ajustes": ("latin-1", CAGED_AJUSTES_HEADER_NAMES),
    "caged-2020-exc": ("utf-8", NOVO_CAGED_HEADER_NAMES),
    "caged-2020-for": ("utf-8", NOVO_CAGED_HEADER_NAMES),
    "caged-2020-mov": ("utf-8", NOVO_CAGED_HEADER_NAMES),
}

logger = logging.getLogger(__name__)


def parse_filename(f: Path) -> dict[str, str | int | None]:
    """Parses a filename to extract metadata.

    Args:
        f (Path): The file path to parse.

    Returns:
        dict[str, str | int | None]: A dictionary containing the parsed metadata.
    """
    m = re.search(r"^([a-z0-9-]+)_([a-z0-9-]+)@(\d{8})\.(7z|zip)$", f.name)
    dataset, partition, modification, extension = m.groups()
    date_uf = partition.split("-")
    if len(date_uf) == 2:
        date, uf = date_uf
    else:
        date = date_uf[0]
        uf = None
    return {
        "filepath": f,
        "filename": f.name,
        "name": f.stem,
        "extension": extension,
        "modification": modification,
        "dataset": dataset,
        "date": int(date),
        "uf": uf,
    }


def convert_columns_dtypes(
    df: pl.DataFrame, decimal_separator: str = ","
) -> pl.DataFrame:
    """Converts the datatypes of columns in a DataFrame based on predefined constants.

    Args:
        df (pl.DataFrame): The input DataFrame.
        decimal_separator (str): "," for the legacy PDET format (decimal comma,
            "." as thousands separator) or "." for the 2026 format (decimal
            point, no thousands separator). Defaults to ",".

    Returns:
        pl.DataFrame: A DataFrame with the converted datatypes.

    Raises:
        ValueError: If ``decimal_separator`` is neither "," nor ".".
    """
    if decimal_separator not in (",", "."):
        raise ValueError(f"Unsupported decimal separator: {decimal_separator!r}")
    for column in df.columns:
        if column in INTEGER_COLUMNS:
            if decimal_separator == ",":
                expr = pl.col(column).str.replace(r" +", "").str.replace(r"\.", "")
            else:
                expr = pl.col(column).str.strip_chars()
            df = df.with_columns(expr.cast(pl.Int64))
        elif column in NUMERIC_COLUMNS:
            if decimal_separator == ",":
                expr = (
                    pl.col(column)
                    .str.replace(r" +", "")
                    .str.replace(r"\.", "")
                    .str.replace(",", ".")
                )
            else:
                expr = pl.col(column).str.strip_chars()
            df = df.with_columns(expr.cast(pl.Float64))
        elif column in BOOLEAN_COLUMNS:
            df = df.with_columns(_boolean_expr(df, column))
        else:  # Categorical
            df = df.with_columns(pl.col(column).str.strip_chars().cast(pl.Categorical))
    return df


def _boolean_expr(df: pl.DataFrame, column: str) -> pl.Expr:
    """Converts a PDET indicator column ("0"/"1") to Boolean.

    "9" is the PDET code for "não identificado" and becomes null; it used to
    become True, like any other non-zero code. Any other value raises.

    Raises:
        ParseError: If the column has a value other than "0", "1" or "9".
    """
    values = pl.col(column).cast(pl.Utf8).str.strip_chars()
    unexpected = (
        df.select(values.alias("v"))
        .filter(pl.col("v").is_not_null() & ~pl.col("v").is_in(BOOLEAN_CODES))
        .get_column("v")
        .unique()
    )
    if unexpected.len():
        raise ParseError(
            f"Unexpected value(s) in indicator column {column!r}: "
            f"{sorted(unexpected.to_list())[:10]}"
        )
    return (
        pl.when(values == "1")
        .then(True)
        .when(values == "0")
        .then(False)
        .otherwise(None)
        .cast(pl.Boolean)
        .alias(column)
    )


def _fix_ragged_csv(filepath: Path, encoding: str) -> Path:
    import csv

    logger.info("Fixing ragged CSV file: %s", filepath)

    dest_filepath = filepath.with_suffix(".fixed.csv")
    with open(filepath, encoding=encoding) as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        header_len = len(header)
        with open(dest_filepath, "w", encoding=encoding, newline="\n") as ff:
            writer = csv.writer(ff, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            for row in reader:
                writer.writerow(row[:header_len])

    return dest_filepath


def _sniff_separator(filepath: Path, encoding: str) -> str:
    with open(filepath, encoding=encoding, errors="replace") as f:
        first_line = f.readline()
    counts = {sep: first_line.count(sep) for sep in (";", "\t", ",")}
    return max(counts, key=counts.__getitem__)


def _resolve_columns(schema_dict: dict[int, tuple], date_key: int) -> tuple:
    columns_names = list(schema_dict.values())[-1]
    for key in schema_dict:
        if date_key < key:
            break
        columns_names = schema_dict[key]
    return columns_names


_CODE_SUFFIX = re.compile(r"\s*-\s*c[oó]digo$")


def normalize_header(header: str) -> str:
    """Normalizes a PDET column header for lookup in the header maps.

    Strips the " - Código" suffix of the 2026 format, collapses whitespace and
    casefolds, so "CNAE 2.0 Subclasse - Codigo" and "CNAE 2.0 Subclasse" match.

    Args:
        header (str): The column header as written in the file.

    Returns:
        str: The normalized header.
    """
    text = " ".join(header.replace("\ufeff", "").split()).casefold()
    return _CODE_SUFFIX.sub("", text)


def resolve_columns(
    header: Sequence[str],
    header_names: dict[str, str | tuple[str, ...]],
    label: str,
) -> list[str]:
    """Maps a file header to canonical column names with a header map.

    Args:
        header (Sequence[str]): The column headers, in file order.
        header_names (dict): Normalized header (see ``normalize_header``) to
            canonical name; a tuple value names repeated headers in the order
            they appear.
        label (str): Dataset name used in error messages.

    Returns:
        list[str]: The canonical column names, in file order.

    Raises:
        ParseError: If a header is unknown, or if two columns would get the
            same canonical name.
    """
    names: list[str] = []
    unknown: list[str] = []
    occurrences: collections.Counter[str] = collections.Counter()
    for column in header:
        key = normalize_header(column)
        target = header_names.get(key)
        if isinstance(target, tuple):
            index = occurrences[key]
            occurrences[key] += 1
            target = target[index] if index < len(target) else None
        if target is None:
            unknown.append(column)
            continue
        names.append(target)
    if unknown:
        raise ParseError(f"Unknown {label} column(s): {unknown}")
    duplicated = [name for name, n in collections.Counter(names).items() if n > 1]
    if duplicated:
        raise ParseError(f"{label} columns map to the same name: {duplicated}")
    return names


def resolve_rais_vinculos_columns(header: Sequence[str]) -> list[str]:
    """Maps the header of a RAIS Vínculos file to canonical column names.

    Args:
        header (Sequence[str]): The column headers, in file order.

    Returns:
        list[str]: The canonical column names, in file order.

    Raises:
        ParseError: If a header is unknown, or if two columns would get the
            same canonical name.
    """
    return resolve_columns(header, RAIS_VINCULOS_HEADER_NAMES, "RAIS vinculos")


def resolve_rais_estabelecimentos_columns(header: Sequence[str]) -> list[str]:
    """Maps the header of a RAIS Estabelecimentos file to canonical names.

    The 1985-2001 files have abbreviated, all-uppercase headers whose "UF" is
    the state abbreviation (``sigla_uf``); from 2002 on, "UF" is the state
    code (``id_uf``). The header style picks the map.

    Args:
        header (Sequence[str]): The column headers, in file order.

    Returns:
        list[str]: The canonical column names, in file order.

    Raises:
        ParseError: If a header is unknown, or if two columns would get the
            same canonical name.
    """
    if all(column == column.upper() for column in header):
        header_names = RAIS_ESTABELECIMENTOS_1985_2001_HEADER_NAMES
    else:
        header_names = RAIS_ESTABELECIMENTOS_HEADER_NAMES
    return resolve_columns(header, header_names, "RAIS estabelecimentos")


def resolve_caged_columns(header: Sequence[str], dataset: str) -> list[str]:
    """Maps the header of a CAGED or Novo CAGED file to canonical names.

    Args:
        header (Sequence[str]): The column headers, in file order.
        dataset (str): ``caged``, ``caged-ajustes``, ``caged-2020-mov``,
            ``caged-2020-for`` or ``caged-2020-exc``.

    Returns:
        list[str]: The canonical column names, in file order.

    Raises:
        ValueError: If an unknown CAGED dataset is provided.
        ParseError: If a header is unknown, or if two columns would get the
            same canonical name.
    """
    if dataset not in _CAGED_FORMATS:
        raise ValueError(f"Unknown CAGED dataset: {dataset!r}")
    _, header_names = _CAGED_FORMATS[dataset]
    return resolve_columns(header, header_names, dataset)


def _read_header(filepath: Path, separator: str, encoding: str) -> list[str]:
    with open(filepath, encoding=encoding, newline="") as f:
        return next(csv.reader(f, delimiter=separator))


def read_rais(filepath: Path, year: int, dataset: str, **read_csv_args) -> pl.DataFrame:
    """Reads a RAIS dataset from a CSV file into a Polars DataFrame.

    Columns are named from the file header (see ``RAIS_VINCULOS_HEADER_NAMES``
    and ``RAIS_ESTABELECIMENTOS_HEADER_NAMES``), so any layout from 1985 on is
    read by column name. Files separated by "," (the 2026 PDET format) use a
    decimal point; files separated by ";" use a decimal comma.

    Args:
        filepath (Path): The path to the CSV file.
        year (int): The year of the dataset (not used to name the columns;
            kept for compatibility).
        dataset (str): The specific RAIS dataset to read ('vinculos' or
            'estabelecimentos').
        **read_csv_args: Additional keyword arguments for polars.read_csv.

    Returns:
        pl.DataFrame: The loaded and typed DataFrame.

    Raises:
        ValueError: If an unknown RAIS dataset is provided.
        ParseError: If a header cannot be mapped.
    """
    if dataset not in ("vinculos", "estabelecimentos"):
        raise ValueError(f"Unknown RAIS dataset: {dataset!r}")
    logger.info("Reading %s from %s", dataset, filepath)
    separator = _sniff_separator(filepath, encoding="latin1")
    header = _read_header(filepath, separator, encoding="latin1")
    if dataset == "vinculos":
        columns_names = resolve_rais_vinculos_columns(header)
    else:
        columns_names = resolve_rais_estabelecimentos_columns(header)
    df = pl.read_csv(
        filepath,
        has_header=True,
        new_columns=columns_names,
        separator=separator,
        encoding="latin1",
        null_values=NA_VALUES,
        infer_schema_length=0,
        **read_csv_args,
    )
    df = convert_columns_dtypes(df, decimal_separator="." if separator == "," else ",")
    return df


def read_caged(
    filepath: Path, date: int, dataset: str, **read_csv_args
) -> pl.DataFrame:
    """Reads a CAGED dataset from a CSV file into a Polars DataFrame.

    Columns are named from the file header (``CAGED_HEADER_NAMES``,
    ``CAGED_AJUSTES_HEADER_NAMES`` and ``NOVO_CAGED_HEADER_NAMES``), not by
    position.

    Args:
        filepath (Path): The path to the CSV file.
        date (int): The date (usually year-month format) of the dataset (not
            used to name the columns; kept for compatibility).
        dataset (str): The specific CAGED dataset to read.
        **read_csv_args: Additional keyword arguments for polars.read_csv.

    Returns:
        pl.DataFrame: The loaded and typed DataFrame.

    Raises:
        ValueError: If an unknown CAGED dataset is provided.
        ParseError: If a header cannot be mapped.
    """
    if dataset not in _CAGED_FORMATS:
        raise ValueError(f"Unknown CAGED dataset: {dataset!r}")
    encoding, _ = _CAGED_FORMATS[dataset]

    logger.info("Reading %s from %s", dataset, filepath)
    if filepath.name in RAGGED_CSV_FILES:
        filepath = _fix_ragged_csv(filepath, encoding)
    separator = _sniff_separator(filepath, encoding=encoding)
    header = _read_header(filepath, separator, encoding=encoding)
    columns_names = resolve_caged_columns(header, dataset)
    df = pl.read_csv(
        filepath,
        has_header=True,
        new_columns=columns_names,
        separator=separator,
        encoding=encoding,
        null_values=NA_VALUES,
        infer_schema_length=0,
        **read_csv_args,
    )
    df = convert_columns_dtypes(df, decimal_separator="." if separator == "," else ",")
    return df


def write_parquet(
    df: pl.DataFrame,
    filepath: Path,
    source_path: Path | None = None,
    compression: str = "zstd",
) -> Path:
    """Writes a DataFrame to a Parquet file.

    Args:
        df (pl.DataFrame): The DataFrame to write.
        filepath (Path): The destination file path.
        source_path (Path | None): Raw archive whose ``.manifest.json`` sidecar
            provides the ``quantilica.*`` provenance. Defaults to None.
        compression (str): Parquet compression. Defaults to "zstd".

    Returns:
        Path: The file path where the Parquet file was written.
    """
    logger.info("Writing data to %s", filepath)
    return to_parquet(df, filepath, source_path=source_path, compression=compression)


def decompress(file_metadata: dict[str, Any]) -> dict[str, Path]:
    """Decompresses an archive file using quantilica.core.files.decompress_archive.

    Args:
        file_metadata (dict[str, Any]): A dictionary containing metadata
            for the file, including 'filepath'.

    Returns:
        dict[str, Path]: The file metadata updated with paths to the
            temporary directory and decompressed file.

    Raises:
        RuntimeError: If decompression fails or produces no output files.
    """
    compressed_filepath = Path(file_metadata["filepath"])
    logger.info("Decompressing %s", compressed_filepath)
    tmp_dir = Path(tempfile.mkdtemp(prefix="pdet"))
    try:
        decompressed_filepath = decompress_archive(
            compressed_filepath, output_dir=tmp_dir
        )
    except StorageError as exc:
        raise RuntimeError(
            f"7z failed decompressing {compressed_filepath}: {exc}"
        ) from exc
    return file_metadata | {
        "tmp_dir": tmp_dir,
        "decompressed_filepath": decompressed_filepath,
    }
