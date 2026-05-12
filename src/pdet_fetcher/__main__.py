import argparse
import logging
from pathlib import Path

from . import (
    __version__,
    connect,
    fetch_caged,
    fetch_caged_2020,
    fetch_caged_2020_docs,
    fetch_caged_docs,
    fetch_rais,
    fetch_rais_docs,
    list_caged,
    list_caged_2020,
    list_rais,
)
from .wrangling import convert_caged, convert_rais, extract_columns_for_dataset

logging.basicConfig(
    level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s"
)


def list_files_cmd(args):
    ftp = connect()
    try:
        for f in list_caged(ftp):
            dest_filepath = (
                args.output / f["dataset"] / str(f["year"]) / f["name"]
            )
            if dest_filepath.exists():
                continue
            print(f["full_path"], "-->", dest_filepath)
        for f in list_rais(ftp):
            dest_filepath = (
                args.output / f["dataset"] / str(f["year"]) / f["name"]
            )
            if dest_filepath.exists():
                continue
            print(f["full_path"], "-->", dest_filepath)
        for f in list_caged_2020(ftp):
            dest_filepath = (
                args.output / f["dataset"] / str(f["year"]) / f["name"]
            )
            if dest_filepath.exists():
                continue
            print(f["full_path"], "-->", dest_filepath)
    finally:
        ftp.close()


def fetch_cmd(args):
    ftp = connect()
    try:
        fetch_rais(ftp=ftp, dest_dir=args.output)
        fetch_rais_docs(ftp=ftp, dest_dir=args.output)
        fetch_caged(ftp=ftp, dest_dir=args.output)
        fetch_caged_docs(ftp=ftp, dest_dir=args.output)
        fetch_caged_2020(ftp=ftp, dest_dir=args.output)
        fetch_caged_2020_docs(ftp=ftp, dest_dir=args.output)
    finally:
        ftp.close()


def convert_cmd(args):
    convert_rais(args.input, args.output)
    convert_caged(args.input, args.output)


DATASETS = {
    "rais-estabelecimentos": {
        "glob_pattern": "rais-*.*",
        "has_uf": True,
        "encoding": "latin-1",
    },
    "rais-vinculos": {
        "glob_pattern": "rais-*.*",
        "has_uf": True,
        "encoding": "latin-1",
    },
    "caged": {
        "glob_pattern": "caged_*.*",
        "has_uf": False,
        "encoding": "latin-1",
    },
    "caged-ajustes": {
        "glob_pattern": "caged-ajustes_*.*",
        "has_uf": False,
        "encoding": "latin-1",
    },
    "caged-2020": {
        "glob_pattern": "caged-2020-*.*",
        "has_uf": False,
        "encoding": "utf-8",
    },
}


def columns_cmd(args):
    if args.dataset not in DATASETS:
        raise ValueError(f"Unknown dataset: {args.dataset}")

    config = DATASETS[args.dataset]
    output_file = args.output / f"{args.dataset}-columns.csv"
    extract_columns_for_dataset(
        args.input,
        config["glob_pattern"],
        output_file,
        encoding=config["encoding"],
        has_uf=config["has_uf"],
    )


def main():
    parser = argparse.ArgumentParser(
        description="Fetch and list Brazilian labor market microdata from PDET"
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    fetch_parser = subparsers.add_parser(
        "fetch", help="Fetch data from FTP server"
    )
    fetch_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("/data/pdet"),
        help="Destination directory (default: /data/pdet)",
    )
    fetch_parser.set_defaults(func=fetch_cmd)

    list_parser = subparsers.add_parser(
        "list", help="List available files on FTP server"
    )
    list_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("/data/pdet"),
        help="Destination directory reference (default: /data/pdet)",
    )
    list_parser.set_defaults(func=list_files_cmd)

    convert_parser = subparsers.add_parser(
        "convert", help="Convert raw data files to Parquet"
    )
    convert_parser.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Source directory with raw (compressed) data files",
    )
    convert_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("/data/pdet"),
        help="Destination directory for converted Parquet files (default: /data/pdet)",
    )
    convert_parser.set_defaults(func=convert_cmd)

    columns_parser = subparsers.add_parser(
        "columns", help="Extract column names from raw data files"
    )
    columns_parser.add_argument(
        "-i",
        "--input",
        type=Path,
        required=True,
        help="Source directory with raw (compressed) data files",
    )
    columns_parser.add_argument(
        "dataset",
        help=f"Dataset name ({', '.join(DATASETS.keys())})",
    )
    columns_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path.cwd(),
        help="Directory to write columns CSV (default: current directory)",
    )
    columns_parser.set_defaults(func=columns_cmd)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return

    args.func(args)


if __name__ == "__main__":
    main()
