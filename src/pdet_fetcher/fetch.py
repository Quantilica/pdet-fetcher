import contextlib
import datetime as dt
import ftplib
import re
import time
from collections.abc import Generator, Sequence

from quantilica.core.ftp import FTP_TRANSIENT_ERRORS, ftp_connect
from quantilica.core.retry import exponential_delay

from . import logger
from .meta import datasets, docs

FTP_HOST = "ftp.mtps.gov.br"
_PDET_FTP_TIMEOUT = 60.0

_list_files_cache: dict[str, list[dict]] = {}

# AttributeError handles the case where ftp.sock is None after a silent connection drop.
_FTP_ERRORS = FTP_TRANSIENT_ERRORS + (AttributeError,)


def connect(attempts: int = 3) -> ftplib.FTP:
    """Connects to the PDET FTP server.

    Args:
        attempts (int, optional): Maximum number of connection attempts. Defaults to 3.

    Returns:
        ftplib.FTP: An active FTP connection to the server.
    """
    return ftp_connect(
        FTP_HOST,
        encoding="latin-1",
        timeout=_PDET_FTP_TIMEOUT,
        attempts=attempts,
        base_delay=2.0,
        max_delay=30.0,
        jitter=1.0,
    )


def list_files(directory: str) -> list[dict]:
    """List all files in the current directory using custom parser for MTPS server.

    Args:
        directory (str): The FTP directory path to list.

    Returns:
        list[dict]: A list of dictionaries containing file metadata.
    """
    if directory in _list_files_cache:
        return _list_files_cache[directory]

    logger.info("Listing %s", directory)

    ftp_lines: list[str] = []
    last_exc: Exception | None = None
    for attempt in range(3):
        if attempt > 0:
            delay = exponential_delay(
                attempt, base_delay=2.0, max_delay=30.0, jitter=1.0
            )
            logger.warning(
                "FTP error on attempt %d listing %s: %s. Reconnecting in %.1fs...",
                attempt,
                directory,
                last_exc,
                delay,
            )
            time.sleep(delay)
        try:
            ftp = connect()
            try:
                ftp_lines = []
                ftp.cwd(directory)
                ftp.retrlines("LIST", ftp_lines.append)
            finally:
                with contextlib.suppress(Exception):
                    ftp.quit()
            break
        except _FTP_ERRORS as exc:
            last_exc = exc
            if attempt >= 2:
                raise

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
                "id": name,
                "url": f"{directory}/{name}".replace("//", "/"),
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
        dir_meta = _match_date_dir(f, patterns, groups_list)
        if dir_meta is not None:
            date_dirs.append(dir_meta)
    return date_dirs


def _match_date_dir(
    f: dict,
    patterns: Sequence[str],
    groups_list: Sequence[Sequence[str]],
) -> dict | None:
    """Matches a FTP entry against the date dir patterns.

    Args:
        f (dict): FTP file metadata produced by ``list_files``.
        patterns (Sequence[str]): Directory name regex patterns.
        groups_list (Sequence[Sequence[str]]): Group names for each pattern.

    Returns:
        dict | None: Directory metadata (``dir`` plus captured groups) or
        ``None`` when the entry is a file or matches none of the patterns.
    """
    if f["size"] is not None:
        return None
    for pattern, groups in zip(patterns, groups_list, strict=False):
        m = re.match(pattern, f["name"])
        if m:
            dir_meta = {"dir": f["name"]}
            for i, group in enumerate(groups):
                dir_meta.update({group: m.groups()[i]})
            return dir_meta
    return None


_MAX_DATE_DIR_DEPTH = 2


def _list_date_dir_files(
    ftp_path: str,
    dir_path: str,
    dir_meta: dict,
    variation: dict,
    depth: int,
) -> Generator[dict, None, None]:
    """Lists (possibly nested) files inside a date directory.

    Some PDET datasets (e.g. NOVO CAGED) nest files two levels deep
    (``<year>/<yearmonth>/file.7z``), while others store files directly in
    the year directory. Child directories matching a date pattern are
    descended, up to ``_MAX_DATE_DIR_DEPTH`` levels, and their captured
    groups (e.g. ``year``/``month``) are merged into the file metadata.

    Args:
        ftp_path (str): Dataset base path on the FTP server.
        dir_path (str): Absolute FTP path of the current date directory.
        dir_meta (dict): Metadata matched for the current date directory.
        variation (dict): Dataset variation being listed.
        depth (int): Current nesting depth (1 for the top-level date dirs).

    Yields:
        dict: File metadata enriched with the date directory groups.
    """
    if isinstance(variation["dir_pattern"], str):
        patterns = [variation["dir_pattern"]]
        groups_list = [variation["dir_pattern_groups"]]
    else:
        patterns = variation["dir_pattern"]
        groups_list = variation["dir_pattern_groups"]

    files = list_files(dir_path)
    for f in files:
        if depth < _MAX_DATE_DIR_DEPTH:
            child_meta = _match_date_dir(f, patterns, groups_list)
            if child_meta is not None:
                child_path = f"{dir_path}/{child_meta['dir']}"
                yield from _list_date_dir_files(
                    ftp_path=ftp_path,
                    dir_path=child_path,
                    dir_meta=dir_meta | child_meta,
                    variation=variation,
                    depth=depth + 1,
                )
                continue
        yield f | dir_meta


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


def _list_variation_files(variation: dict) -> Generator[dict, None, None]:
    ftp_path = variation["path"]
    if variation["dir_pattern"]:
        date_dirs = _get_date_dirs(
            fi=list_files(ftp_path),
            dir_pattern=variation["dir_pattern"],
            dir_pattern_groups=variation["dir_pattern_groups"],
        )
        for date_dir_meta in date_dirs:
            yield from _list_date_dir_files(
                ftp_path=ftp_path,
                dir_path=f"{ftp_path}/{date_dir_meta['dir']}",
                dir_meta=date_dir_meta,
                variation=variation,
                depth=1,
            )
    else:
        files = list_files(ftp_path)
        yield from (f | {"year": None, "group": variation["group"]} for f in files)


def _get_variation_files_metadata(variation: dict) -> Generator[dict, None, None]:
    for file in _list_variation_files(variation=variation):
        m = re.match(
            variation["fn_pattern"],
            file["name"].lower(),
        )
        if m:
            group_meta = _get_group_meta(m, variation=variation)
            yield file | group_meta


def _list_dataset_files(dataset: str) -> Generator[dict, None, None]:
    for variation in datasets[dataset]["variations"]:
        variation = variation | {"group": dataset}
        for f in _get_variation_files_metadata(variation=variation):
            yield f | {"dataset": dataset, "group": dataset}


def _doc_entry(file: dict, dataset: str) -> dict:
    """Mark a listed file as a documentation entry of ``dataset``.

    Documentation files (layouts, leia-me) have no year/month in their names
    and live under ``_documentacao/`` locally. ``is_doc`` lets the path
    builder route them to the docs storage instead of the partitioned data
    layout, which requires ``year``.

    Args:
        file (dict): File metadata produced by :func:`list_files`.
        dataset (str): Dataset the documentation belongs to.

    Returns:
        dict: The file metadata enriched with dataset/group/is_doc.
    """
    return file | {
        "dataset": dataset,
        "group": dataset,
        "year": None,
        "ext": file["extension"],
        "is_doc": True,
    }


# -----------------------------------------------------------------------------
# ---------------------------------- CAGED ------------------------------------
# -----------------------------------------------------------------------------
def list_caged() -> Generator[dict, None, None]:
    """Lists files for the classic CAGED datasets.

    Yields:
        dict: File metadata for CAGED files.
    """
    for dataset in ("caged", "caged-ajustes"):
        yield from _list_dataset_files(dataset)


def list_caged_docs() -> Generator[dict, None, None]:
    """Lists documentation files for the classic CAGED datasets.

    Yields:
        dict: File metadata for CAGED documentation files.
    """
    for file in list_files(docs["caged"]["dir_path"]):
        if not re.match(docs["caged"]["fn_pattern"], file["name"]):
            continue
        yield _doc_entry(file, "caged")
    for file in list_files(docs["caged-ajustes"]["dir_path"]):
        if not re.match(docs["caged-ajustes"]["fn_pattern"], file["name"]):
            continue
        yield _doc_entry(file, "caged-ajustes")


def list_caged_2020() -> Generator[dict, None, None]:
    """Lists files for the Novo CAGED (2020+) datasets.

    Yields:
        dict: File metadata for Novo CAGED files.
    """
    for dataset in ("caged-2020-exc", "caged-2020-for", "caged-2020-mov"):
        yield from _list_dataset_files(dataset)


def list_caged_2020_docs() -> Generator[dict, None, None]:
    """Lists documentation files for the Novo CAGED (2020+) datasets.

    Yields:
        dict: File metadata for Novo CAGED documentation files.
    """
    for file in list_files(docs["caged-2020"]["dir_path"]):
        if not re.match(docs["caged-2020"]["fn_pattern"], file["name"]):
            continue
        yield _doc_entry(file, "caged-2020")


# -----------------------------------------------------------------------------
# ----------------------------------- RAIS ------------------------------------
# -----------------------------------------------------------------------------
def list_rais() -> Generator[dict, None, None]:
    """Lists files for the RAIS datasets.

    Yields:
        dict: File metadata for RAIS files.
    """
    for dataset in ("rais-estabelecimentos", "rais-vinculos"):
        yield from _list_dataset_files(dataset)


def list_rais_docs() -> Generator[dict, None, None]:
    """Lists documentation files for the RAIS datasets.

    Yields:
        dict: File metadata for RAIS documentation files.
    """
    for file in list_files(docs["rais-vinculos"]["dir_path"]):
        if file["size"] is None or file["extension"] is None:
            continue
        yield _doc_entry(file, "rais-vinculos")
    for file in list_files(docs["rais-estabelecimentos"]["dir_path"]):
        if file["size"] is None or file["extension"] is None:
            continue
        yield _doc_entry(file, "rais-estabelecimentos")
