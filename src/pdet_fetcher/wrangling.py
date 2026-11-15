import csv
import logging
from pathlib import Path

try:
    import polars as pl
except ImportError as exc:
    raise ImportError(
        "pdet_fetcher.wrangling requer o extra 'analysis': "
        "uv add 'pdet-fetcher[analysis]' "
        "--index https://index.quantilica.com/simple/"
    ) from exc

from . import reader

logger = logging.getLogger(__name__)


def convert_rais(data_dir: Path, dest_dir: Path) -> None:
    """Converts RAIS raw files to Parquet format.

    Args:
        data_dir (Path): The directory containing raw RAIS files.
        dest_dir (Path): The destination directory for Parquet files.
    """
    for dataset in ("vinculos", "estabelecimentos"):
        dataset_name = f"rais-{dataset}"
        src_dir = data_dir / dataset_name
        out_dir = dest_dir / dataset_name

        logger.info("Converting RAIS %s data...", dataset)
        for year_dir in src_dir.iterdir():
            if not year_dir.is_dir():
                continue
            year = int(year_dir.name)

            all_files: dict[str | None, dict] = {}
            for file in year_dir.iterdir():
                file_metadata = reader.parse_filename(file)
                uf = file_metadata["uf"]
                if (
                    uf not in all_files
                    or file_metadata["modification"] > all_files[uf]["modification"]
                ):
                    all_files[uf] = file_metadata

            if not all_files:
                continue

            files = list(all_files.values())
            latest_modification = max(f["modification"] for f in files)

            dest_filepath = (
                out_dir / f"{dataset_name}_{year}@{latest_modification}.parquet"
            )
            if dest_filepath.exists():
                logger.info("Skipping %s (already converted)", dest_filepath.name)
                continue

            logger.info("Converting %d files to %s", len(files), dest_filepath.name)
            dest_filepath.parent.mkdir(parents=True, exist_ok=True)

            with pl.StringCache():
                frames = []
                for file_metadata in files:
                    with reader.extracted_data(file_metadata) as extracted:
                        # read_rais é eager: o DataFrame sobrevive ao bloco.
                        df = reader.read_rais(
                            extracted["decompressed_filepath"],
                            year=year,
                            dataset=dataset,
                        )
                    frames.append(df)
                data = pl.concat(frames, how="vertical")

            # Com várias UFs não há bruto único: só herda proveniência de 1 arquivo.
            source = files[0]["filepath"] if len(files) == 1 else None
            reader.write_parquet(data, dest_filepath, source_path=source)


def convert_caged(data_dir: Path, dest_dir: Path) -> None:
    """Converts CAGED raw files to Parquet format.

    Args:
        data_dir (Path): The directory containing raw CAGED files.
        dest_dir (Path): The destination directory for Parquet files.
    """
    logger.info("Converting CAGED data...")

    latest_files: dict[tuple, dict] = {}
    for file in data_dir.glob("**/caged*.*"):
        if file.suffix not in (".zip", ".7z"):
            continue
        file_metadata = reader.parse_filename(file)
        key = (
            file_metadata["dataset"],
            file_metadata["date"],
            file_metadata["uf"],
        )
        if (
            key not in latest_files
            or file_metadata["modification"] > latest_files[key]["modification"]
        ):
            latest_files[key] = file_metadata

    logger.info("Converting %d CAGED files", len(latest_files))
    for file_metadata in latest_files.values():
        date = file_metadata["date"]
        name = file_metadata["name"]
        dataset = file_metadata["dataset"]

        dest_filepath = dest_dir / dataset / f"{name}.parquet"
        if dest_filepath.exists():
            logger.info("Skipping %s (already converted)", dest_filepath.name)
            continue

        dest_filepath.parent.mkdir(parents=True, exist_ok=True)

        with reader.extracted_data(file_metadata) as extracted:
            decompressed_filepath = extracted["decompressed_filepath"]
            try:
                df = reader.read_caged(
                    decompressed_filepath,
                    date=date,
                    dataset=dataset,
                )
                reader.write_parquet(
                    df, dest_filepath, source_path=file_metadata["filepath"]
                )
            except pl.exceptions.ComputeError as e:
                logger.error("Error converting %s: %s", decompressed_filepath, e)
            except pl.exceptions.ShapeError as e:
                logger.error("Error converting %s: %s", decompressed_filepath, e)


def extract_columns_for_dataset(
    data_dir: Path,
    glob_pattern: str,
    output_file: Path,
    encoding: str = "latin-1",
    has_uf: bool = False,
) -> None:
    """Extracts column names from a dataset and writes them to a CSV file.

    Args:
        data_dir (Path): The directory containing the dataset files.
        glob_pattern (str): The glob pattern to match dataset files.
        output_file (Path): The path to the output CSV file.
        encoding (str, optional): The encoding of the input files.
            Defaults to "latin-1".
        has_uf (bool, optional): Whether the dataset includes UF
            information. Defaults to False.
    """
    fieldnames = ["column", "order", "name", "date"]
    if has_uf:
        fieldnames.append("uf")

    with open(output_file, "w", encoding="utf-8", newline="\n") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for file in data_dir.rglob(glob_pattern):
            logger.info("Extracting columns from %s", file)
            file_metadata = reader.parse_filename(file)
            with reader.extracted_data(file_metadata) as extracted:
                df: pl.DataFrame = pl.read_csv(
                    extracted["decompressed_filepath"],
                    n_rows=1,
                    encoding=encoding,
                    separator=";",
                    has_header=True,
                    infer_schema_length=0,
                )
            columns = df.columns

            logger.info("Found %d columns", len(columns))
            for order, column in enumerate(columns):
                row = {
                    "column": column,
                    "order": order,
                    "name": file_metadata["name"],
                    "date": file_metadata["date"],
                }
                if has_uf:
                    row["uf"] = file_metadata["uf"]
                writer.writerow(row)
