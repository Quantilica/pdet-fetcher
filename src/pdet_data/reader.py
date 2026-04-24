import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import polars as pl

from .constants import (
    BOOLEAN_COLUMNS,
    CAGED_2020_EXC_COLUMNS,
    CAGED_2020_FOR_COLUMNS,
    CAGED_2020_MOV_COLUMNS,
    CAGED_AJUSTES_COLUMNS,
    CAGED_COLUMNS,
    INTEGER_COLUMNS,
    NA_VALUES,
    NUMERIC_COLUMNS,
    RAGGED_CSV_FILES,
    RAIS_ESTABELECIMENTOS_COLUMNS,
    RAIS_VINCULOS_COLUMNS,
)


def parse_filename(f: Path) -> dict[str, str | int | None]:
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


def convert_columns_dtypes(df: pl.DataFrame) -> pl.DataFrame:
    for column in df.columns:
        if column in INTEGER_COLUMNS:
            df = df.with_columns(
                pl.col(column)
                .str.replace(r" +", "")
                .str.replace(r"\.", "")
                .cast(pl.Int64)
            )
        elif column in NUMERIC_COLUMNS:
            df = df.with_columns(
                pl.col(column)
                .str.replace(r" +", "")
                .str.replace(r"\.", "")
                .str.replace(",", ".")
                .cast(pl.Float64)
            )
        elif column in BOOLEAN_COLUMNS:
            df = df.with_columns(pl.col(column).cast(pl.Int8).cast(pl.Boolean))
        else:  # Categorical
            df = df.with_columns(pl.col(column).str.strip_chars().cast(pl.Categorical))
    return df


def _fix_ragged_csv(filepath: Path, encoding: str) -> Path:
    import csv

    print("Fixing ragged CSV file:", filepath)

    dest_filepath = filepath.with_suffix(".fixed.csv")
    with open(filepath, "r", encoding=encoding) as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        header_len = len(header)
        with open(dest_filepath, "w", encoding=encoding, newline="\n") as ff:
            writer = csv.writer(ff, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow(header)
            for row in reader:
                writer.writerow(row[:header_len])

    return dest_filepath


def read_rais(filepath: Path, year: int, dataset: str, **read_csv_args) -> pl.DataFrame:
    if dataset == "vinculos":
        columns_names = list(RAIS_VINCULOS_COLUMNS.values())[-1]
        for y in RAIS_VINCULOS_COLUMNS:
            if year < y:
                break
            columns_names = RAIS_VINCULOS_COLUMNS[y]
    elif dataset == "estabelecimentos":
        columns_names = list(RAIS_ESTABELECIMENTOS_COLUMNS.values())[-1]
        for y in RAIS_ESTABELECIMENTOS_COLUMNS:
            if year < y:
                break
            columns_names = RAIS_ESTABELECIMENTOS_COLUMNS[y]
    else:
        raise ValueError(f"Unknown RAIS dataset: {dataset!r}")
    print("Reading", dataset, filepath)
    df = pl.read_csv(
        filepath,
        has_header=True,
        new_columns=columns_names,
        separator=";",
        encoding="latin1",
        null_values=NA_VALUES,
        infer_schema_length=0,
        **read_csv_args,
    )
    df = convert_columns_dtypes(df)
    return df


def read_caged(
    filepath: Path, date: int, dataset: str, **read_csv_args
) -> pl.DataFrame:
    if dataset == "caged":
        encoding = "latin-1"
        columns_names = list(CAGED_COLUMNS.values())[-1]
        for d in CAGED_COLUMNS:
            if date < d:
                break
            columns_names = CAGED_COLUMNS[d]
    elif dataset == "caged-ajustes":
        encoding = "latin-1"
        columns_names = list(CAGED_AJUSTES_COLUMNS.values())[-1]
        for d in CAGED_AJUSTES_COLUMNS:
            if date < d:
                break
            columns_names = CAGED_AJUSTES_COLUMNS[d]
    elif dataset == "caged-2020-exc":
        encoding = "utf-8"
        columns_names = list(CAGED_2020_EXC_COLUMNS.values())[-1]
        for d in CAGED_2020_EXC_COLUMNS:
            if date < d:
                break
            columns_names = CAGED_2020_EXC_COLUMNS[d]
    elif dataset == "caged-2020-for":
        encoding = "utf-8"
        columns_names = list(CAGED_2020_FOR_COLUMNS.values())[-1]
        for d in CAGED_2020_FOR_COLUMNS:
            if date < d:
                break
            columns_names = CAGED_2020_FOR_COLUMNS[d]
    elif dataset == "caged-2020-mov":
        encoding = "utf-8"
        columns_names = list(CAGED_2020_MOV_COLUMNS.values())[-1]
        for d in CAGED_2020_MOV_COLUMNS:
            if date < d:
                break
            columns_names = CAGED_2020_MOV_COLUMNS[d]
    else:
        raise ValueError(f"Unknown CAGED dataset: {dataset!r}")

    print("Reading", dataset, filepath)
    if filepath.name in RAGGED_CSV_FILES:
        filepath = _fix_ragged_csv(filepath, encoding)
    df = pl.read_csv(
        filepath,
        has_header=True,
        new_columns=columns_names,
        separator=";",
        encoding=encoding,
        null_values=NA_VALUES,
        infer_schema_length=0,
        **read_csv_args,
    )
    df = convert_columns_dtypes(df)
    return df


def write_parquet(df: pl.DataFrame, filepath: Path) -> Path:
    print("Writing data to", filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(filepath)
    return filepath


def decompress(file_metadata: dict[str, Any]) -> dict[str, Path]:
    compressed_filepath = file_metadata["filepath"]
    print(f"Decompressing {compressed_filepath}")
    tmp_dir = Path(tempfile.mkdtemp(prefix="pdet"))
    command = [
        "7z",
        "e",
        str(compressed_filepath),
        f"-o{tmp_dir}",
    ]
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"7z failed decompressing {compressed_filepath}: "
            f"{result.stderr.decode(errors='replace')}"
        )
    extracted = list(tmp_dir.iterdir())
    if not extracted:
        raise RuntimeError(
            f"7z produced no files from {compressed_filepath}"
        )
    decompressed_filepath = extracted[0]
    return file_metadata | {
        "tmp_dir": tmp_dir,
        "decompressed_filepath": decompressed_filepath,
    }
