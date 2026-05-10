import datetime as dt
import ftplib
import re
from pathlib import Path
from typing import Any, Callable, Generator, Sequence

from quantilica_core.ftp import FtpClient
import quantilica_core.metadata as core_meta

from . import logger
from .meta import datasets, docs
from .storage import DataRepository


FTP_HOST = "ftp.mtps.gov.br"

_list_files_cache: dict[str, list[dict]] = {}

# Global client for PDET
client = FtpClient(FTP_HOST)


def connect() -> ftplib.FTP:
    """Connect to the PDET FTP server for compatibility."""
    ftp = ftplib.FTP(FTP_HOST, encoding="latin-1")
    ftp.login()
    return ftp


def list_files(ftp: ftplib.FTP, directory: str) -> list[dict]:
    """List all files in the current directory using custom parser for MTPS server."""
    if directory in _list_files_cache:
        return _list_files_cache[directory]

    logger.info("Listing %s", directory)

    ftp_lines = []
    ftp.cwd(directory)
    ftp.retrlines("LIST", ftp_lines.append)

    # parse files' date, size and name
    def parse_line(line):
        m = re.match(
            r"^(\d{2}-\d{2}-\d{2}) +(\d{2}:\d{2})(AM|PM) +(<DIR>|\d+) +(.*)$",
            line,
        )
        if m:
            date, time, am_pm, size, name = m.groups()
            # parse datetime
            datetime = dt.datetime.strptime(
                f"{date} {time}{am_pm}",
                "%m-%d-%y %I:%M%p",
            )
            # parse size
            if size == "<DIR>":
                size = None
            else:
                size = int(size)
            # parse name
            name = name.strip()
            try:
                extension = name.rsplit(".", maxsplit=1)[1]
            except IndexError:
                extension = None
            file = {
                "datetime": datetime,
                "size": size,
                "name": name,
                "extension": extension,
                "full_path": f"{directory}/{name}".replace("//", "/"),
            }
            return file
        else:
            return None

    files = []
    for f in ftp_lines:
        file = parse_line(f)
        if file:
            files.append(file)
    _list_files_cache[directory] = files
    return files


def _get_date_dirs(
    fi: list[dict],
    dir_pattern: str | Sequence[str],
    dir_pattern_groups: Sequence[str] | Sequence[Sequence[str]],
) -> list[dict]:
    """Filters list of directories in FTP server that groups files by date."""
    if isinstance(dir_pattern, str):
        patterns = [dir_pattern]
        groups_list = [dir_pattern_groups]
    else:
        patterns = dir_pattern
        groups_list = dir_pattern_groups

    date_dirs = []
    for f in fi:
        if f["size"] is not None:
            continue
        for pattern, groups in zip(patterns, groups_list):
            m = re.match(pattern, f["name"])
            if m:
                group_meta = {"dir": f["name"]}
                for i, group in enumerate(groups):
                    text = m.groups()[i]
                    group_meta.update({group: text})
                date_dirs.append(group_meta)
                break
    return date_dirs


def _get_group_meta(m: re.Match, variation: dict) -> dict:
    """Return a dictionary with info in a file name given by variation's
    fn_pattern.
    """
    group_meta = {}
    for group in variation["fn_pattern_groups"]:
        if not group:
            continue
        index = variation["fn_pattern_groups"].index(group)
        text = m.groups()[index].replace("_", "")
        group_meta.update({group: text})
    return group_meta


def _list_variation_files(
    ftp: ftplib.FTP, variation: dict
) -> Generator[dict, None, None]:
    ftp_path = variation["path"]
    if variation["dir_pattern"]:
        date_dirs = _get_date_dirs(
            fi=list_files(ftp, directory=ftp_path),
            dir_pattern=variation["dir_pattern"],
            dir_pattern_groups=variation["dir_pattern_groups"],
        )
        for date_dir_meta in date_dirs:
            date_dir = date_dir_meta["dir"]
            files = list_files(ftp, directory=f"{ftp_path}/{date_dir}")
            yield from (f | date_dir_meta for f in files)
    else:
        files = list_files(ftp, directory=ftp_path)
        yield from (f | {"year": None} for f in files)


def _get_variation_files_metadata(
    ftp: ftplib.FTP, variation: dict
) -> Generator[dict, None, None]:
    for file in _list_variation_files(ftp=ftp, variation=variation):
        m = re.match(
            variation["fn_pattern"],
            file["name"].lower(),
        )
        if m:
            group_meta = _get_group_meta(m, variation=variation)
            yield file | group_meta


def _list_dataset_files(ftp: ftplib.FTP, dataset: str) -> Generator[dict, None, None]:
    for variation in datasets[dataset]["variations"]:
        for f in _get_variation_files_metadata(ftp=ftp, variation=variation):
            yield f | {"dataset": dataset}


def _fetch_loop(
    ftp: ftplib.FTP,
    list_fn: Callable[[ftplib.FTP], Generator[dict, None, None]],
    get_filepath_fn: Callable[[dict, Path], Path],
    dest_dir: Path,
) -> list[dict[str, Any]]:
    metadata_list = []
    # We use the global client which wraps FtpClient from quantilica-core
    for file in list_fn(ftp):
        ftp_filepath = file["full_path"]
        dest_filepath = get_filepath_fn(file, dest_dir)
        
        # We use FtpClient.download_with_manifest to handle freshness, atomic write and manifest
        try:
            downloaded_path = client.download_with_manifest(
                ftp_filepath,
                dest_filepath,
                source_id="pdet",
                dataset_id=file.get("dataset", "unknown"),
                producer="pdet-data",
            )
            
            metadata = file | {"filepath": downloaded_path}
            metadata_list.append(metadata)
        except Exception as e:
            logger.error(f"Failed to download {ftp_filepath}: {e}")
            
    return metadata_list


# -----------------------------------------------------------------------------
# ---------------------------------- CAGED ------------------------------------
# -----------------------------------------------------------------------------
def list_caged(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for dataset in ("caged", "caged-ajustes"):
        yield from _list_dataset_files(ftp=ftp, dataset=dataset)


def list_caged_docs(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for file in list_files(ftp, directory=docs["caged"]["dir_path"]):
        if not re.match(docs["caged"]["fn_pattern"], file["name"]):
            continue
        yield file | {"dataset": "caged"}
    for file in list_files(ftp, directory=docs["caged-ajustes"]["dir_path"]):
        if not re.match(docs["caged-ajustes"]["fn_pattern"], file["name"]):
            continue
        yield file | {"dataset": "caged-ajustes"}


def fetch_caged(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    from .storage import get_caged_filepath
    return _fetch_loop(ftp, list_caged, get_caged_filepath, dest_dir)


def fetch_caged_docs(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    from .storage import get_docs_filepath
    return _fetch_loop(ftp, list_caged_docs, get_docs_filepath, dest_dir)


def list_caged_2020(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for dataset in ("caged-2020-exc", "caged-2020-for", "caged-2020-mov"):
        yield from _list_dataset_files(ftp=ftp, dataset=dataset)


def list_caged_2020_docs(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for file in list_files(ftp, directory=docs["caged-2020"]["dir_path"]):
        if not re.match(docs["caged-2020"]["fn_pattern"], file["name"]):
            continue
        yield file | {"dataset": "caged-2020"}


def fetch_caged_2020(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    from .storage import get_caged_2020_filepath
    return _fetch_loop(ftp, list_caged_2020, get_caged_2020_filepath, dest_dir)


def fetch_caged_2020_docs(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    from .storage import get_docs_filepath
    return _fetch_loop(ftp, list_caged_2020_docs, get_docs_filepath, dest_dir)


# -----------------------------------------------------------------------------
# ----------------------------------- RAIS ------------------------------------
# -----------------------------------------------------------------------------
def list_rais(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for dataset in ("rais-estabelecimentos", "rais-vinculos"):
        yield from _list_dataset_files(ftp=ftp, dataset=dataset)


def list_rais_docs(ftp: ftplib.FTP) -> Generator[dict, None, None]:
    for file in list_files(ftp, directory=docs["rais-vinculos"]["dir_path"]):
        yield file | {"dataset": "rais-vinculos"}
    for file in list_files(ftp, directory=docs["rais-estabelecimentos"]["dir_path"]):
        yield file | {"dataset": "rais-estabelecimentos"}


def fetch_rais(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    from .storage import get_rais_filepath
    return _fetch_loop(ftp, list_rais, get_rais_filepath, dest_dir)


def fetch_rais_docs(ftp: ftplib.FTP, dest_dir: Path) -> list[dict[str, Any]]:
    from .storage import get_docs_filepath
    return _fetch_loop(ftp, list_rais_docs, get_docs_filepath, dest_dir)


def generate_catalog(downloaded_files: list[dict]) -> core_meta.MetadataCatalog:
    """Generate a validated MetadataCatalog from a list of downloaded PDET files."""
    source_id = "pdet"
    source = core_meta.Source(
        id=source_id,
        name="PDET - Programa de Disseminação de Estatísticas do Trabalho",
        homepage_url="http://pdet.mte.gov.br",
    )

    datasets_map = {}
    resources = []
    
    for file in downloaded_files:
        dataset_id = file.get("dataset", "unknown")
        if dataset_id not in datasets_map:
            datasets_map[dataset_id] = core_meta.Dataset(
                id=dataset_id,
                source_id=source_id,
                name=dataset_id.upper().replace("-", " "),
            )
            
        filename = file["filepath"].name
        resource_id = filename.replace(".", "_")
        
        resources.append(
            core_meta.Resource(
                id=resource_id,
                dataset_id=dataset_id,
                name=filename,
                url=file["full_path"], # FTP path
                format=file.get("extension", ""),
                path=str(file["filepath"].absolute()),
                metadata={
                    "remote_datetime": file["datetime"].isoformat(),
                    "size": file["size"],
                }
            )
        )
        
    catalog = core_meta.MetadataCatalog(
        sources=[source],
        datasets=list(datasets_map.values()),
        resources=resources,
    )
    catalog.validate_references()
    return catalog
