# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Typer plugin for quantilica-cli integration."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Annotated, Any

import typer
from quantilica.cli.sdk import CheckPlanItem, FetcherApp
from quantilica.cli.ui import get_console, setup_rich_logging
from quantilica.core.ftp import FtpClient
from quantilica.core.manifests import manifest_sidecar_path

from pdet_fetcher import (
    list_caged,
    list_caged_2020,
    list_caged_2020_docs,
    list_caged_docs,
    list_rais,
    list_rais_docs,
)
from pdet_fetcher.storage import (
    get_caged_2020_docs_filepath,
    get_caged_2020_filepath,
    get_caged_docs_filepath,
    get_caged_filepath,
    get_rais_docs_filepath,
    get_rais_filepath,
)

from . import logger
from .fetch import FTP_HOST

_DEFAULT_OUTPUT = Path("/data/pdet")
console = get_console()

GROUPS = {
    "caged": {"name": "CAGED Histórico (até 2019)"},
    "caged-2020": {"name": "Novo CAGED (a partir de 2020)"},
    "rais": {"name": "RAIS (Vínculos e Estabelecimentos)"},
}


def pdet_list_datasets(group: str) -> list[dict[str, Any]]:
    """Lists datasets for a given group.

    Args:
        group (str): The name of the dataset group.

    Returns:
        list[dict[str, Any]]: A list of dictionaries with dataset information.
    """
    if group == "caged":
        return list(list_caged()) + list(list_caged_docs())
    elif group == "caged-2020":
        return list(list_caged_2020()) + list(list_caged_2020_docs())
    elif group == "rais":
        return list(list_rais()) + list(list_rais_docs())
    return []


def pdet_path_builder(
    output_dir: Path, entry: dict[str, Any], last_modified: dt.date | None
) -> Path:
    """Builds the file path for a dataset entry.

    Args:
        output_dir (Path): The base output directory.
        entry (dict[str, Any]): The dataset entry metadata.
        last_modified (dt.date | None): The last modified date of the entry.

    Returns:
        Path: The constructed file path.
    """
    # A base is the target dir
    dataset = entry.get("dataset")

    # Documentação é marcada na listagem (``is_doc``); o teste pela URL
    # fica como compatibilidade com entries antigas.
    is_doc = bool(entry.get("is_doc")) or "/_documentacao/" in entry.get("url", "")

    if dataset == "caged" or dataset == "caged-ajustes":
        if is_doc:
            return get_caged_docs_filepath(entry, output_dir)
        return get_caged_filepath(entry, output_dir)
    elif dataset is not None and dataset.startswith("caged-2020"):
        if is_doc:
            return get_caged_2020_docs_filepath(entry, output_dir)
        return get_caged_2020_filepath(entry, output_dir)
    elif dataset in ("rais-estabelecimentos", "rais-vinculos"):
        if is_doc:
            return get_rais_docs_filepath(entry, output_dir)
        return get_rais_filepath(entry, output_dir)

    # fallback
    return output_dir / (entry.get("id") or "unknown.bin")


fetcher = FetcherApp(
    name="pdet-fetcher",
    help="Microdados do PDET (CAGED, RAIS).",
    groups_dict=GROUPS,
    aliases_dict={},
    list_datasets=pdet_list_datasets,
    path_builder=pdet_path_builder,
    default_output=_DEFAULT_OUTPUT,
    client=FtpClient(FTP_HOST),
)

app = fetcher.app


def _entry_to_plan_dict(entry: dict[str, Any]) -> dict[str, Any]:
    """Return a JSON-serializable copy of a dataset entry.

    PDET entries carry ``datetime`` stamps from the FTP LIST parsing, which
    ``CheckPlan.to_json`` cannot serialize. They are converted to tagged
    dicts that :func:`_entry_from_plan_dict` restores.

    Args:
        entry (dict): The dataset entry produced by ``pdet_list_datasets``.

    Returns:
        dict[str, Any]: A JSON-serializable copy of the entry.
    """
    return _to_jsonable(entry)


def _to_jsonable(obj: Any) -> Any:
    """Recursively convert rich PDET values to JSON-safe structures.

    Args:
        obj (Any): Value to convert.

    Returns:
        Any: JSON-serializable representation of the value.
    """
    if isinstance(obj, dt.datetime):
        return {"__datetime__": True, "value": obj.isoformat()}
    if isinstance(obj, dt.date):
        return {"__date__": True, "value": obj.isoformat()}
    if isinstance(obj, dict):
        return {key: _to_jsonable(value) for key, value in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(value) for value in obj]
    return obj


def _entry_from_plan_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Restore a dataset entry deserialized from a check plan.

    Inverse of :func:`_entry_to_plan_dict`: tagged dicts become ``datetime``
    objects again so the entry works with the path builder and the download
    path.

    Args:
        data (dict[str, Any]): The entry payload stored in the plan.

    Returns:
        dict[str, Any]: The rehydrated dataset entry.
    """
    return _from_jsonable(data)


def _from_jsonable(obj: Any) -> Any:
    """Recursive counterpart of :func:`_to_jsonable`.

    Args:
        obj (Any): Value to restore.

    Returns:
        Any: Value with ``datetime`` objects rebuilt.
    """
    if isinstance(obj, dict):
        if obj.get("__datetime__"):
            return dt.datetime.fromisoformat(obj["value"])
        if obj.get("__date__"):
            return dt.date.fromisoformat(obj["value"])
        return {key: _from_jsonable(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_from_jsonable(value) for value in obj]
    return obj


def _pdet_check_entry(entry: dict[str, Any], output_dir: Path) -> CheckPlanItem:
    """Build a freshness verdict for one PDET entry without downloading.

    The generic SDK probe uses HTTP ``HEAD``, which ``FtpClient`` does not
    implement, so freshness here uses the metadata already obtained from the
    FTP LIST (size and modification timestamp) plus an honest local
    heuristic: an entry whose stamped local file (or its ephemeral
    ``.manifest.json`` sidecar) already exists is ``skip-up-to-date``,
    otherwise ``download``. There is no per-file round-trip (no HEAD, no
    MDTM). The stored entry is serialized via :func:`_entry_to_plan_dict` so
    ``check --json`` round-trips through ``sync --from-plan``.

    Args:
        entry (dict): Dataset entry from ``pdet_list_datasets``.
        output_dir (Path): Destination directory used for path computation.

    Returns:
        CheckPlanItem: The freshness verdict for the entry.
    """
    dataset = str(
        entry.get("group") or entry.get("dataset") or entry.get("id") or "datasets"
    )
    eid = str(entry.get("id", "unknown"))
    partition = entry.get("partition")
    try:
        local = pdet_path_builder(output_dir, entry, None)
    except Exception as exc:
        reason = f"path-error: {type(exc).__name__}: {exc}"
        logger.warning("Entrada %s sem caminho local (%s)", eid, reason)
        return CheckPlanItem(
            dataset=dataset,
            id=eid,
            url=str(entry.get("url", "")),
            partition=partition if isinstance(partition, str) else None,
            local_path="",
            remote_etag=None,
            remote_last_modified=None,
            remote_size=None,
            local_exists=False,
            action="download",
            reason=reason,
            entry=_entry_to_plan_dict(entry),
        )
    exists = local.exists() or manifest_sidecar_path(local).exists()
    modified = entry.get("datetime")
    size = entry.get("size")
    return CheckPlanItem(
        dataset=dataset,
        id=eid,
        url=str(entry.get("url", "")),
        partition=partition if isinstance(partition, str) else None,
        local_path=str(local),
        remote_etag=None,
        remote_last_modified=(
            modified.isoformat() if isinstance(modified, dt.datetime) else None
        ),
        remote_size=size if isinstance(size, int) else None,
        local_exists=exists,
        action="skip-up-to-date" if exists else "download",
        reason="local-exists" if exists else "not-present",
        entry=_entry_to_plan_dict(entry),
    )


fetcher.check_entry = _pdet_check_entry  # type: ignore[method-assign]
fetcher.entry_from_plan = _entry_from_plan_dict  # type: ignore[method-assign]

# Datasets reconhecidos pelo extrator de colunas.
_DATASETS = {
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


def convert(
    input_dir: Path,
    lake_dir: Path,
    *,
    dataset: list[str] | None = None,
    force: bool = False,
) -> Any:
    """Convert the raw microdata under ``input_dir`` into the lake at ``lake_dir``.

    The lake module needs the ``analysis`` extra, so it is imported on demand;
    the ``ImportError`` is handled by the ``convert`` and ``pipeline`` commands.
    """
    from pdet_fetcher import lake

    return lake.converter.convert(input_dir, lake_dir, datasets=dataset, force=force)


fetcher.command_convert(convert)
fetcher.command_pipeline(convert)


@app.command("columns")
def cmd_columns(
    dataset: Annotated[
        str,
        typer.Argument(help=f"Dataset ({', '.join(_DATASETS.keys())})"),
    ],
    input: Annotated[
        Path,
        typer.Option("-i", "--input", help="Diretório de origem com arquivos brutos"),
    ],
    output: Annotated[
        Path,
        typer.Option("-o", "--output", help="Diretório para CSV de colunas"),
    ] = Path("."),
    verbose: Annotated[bool, typer.Option("--verbose", help="Logs detalhados")] = False,
) -> None:
    """Extrair nomes de colunas dos arquivos brutos.

    Args:
        dataset (str): The name of the dataset.
        input (Path): Diretório de origem com arquivos brutos.
        output (Path, optional): Diretório para CSV de colunas. Defaults to
            current directory.
        verbose (bool, optional): Exibir logs detalhados. Defaults to False.

    Raises:
        typer.Exit: If the dataset is unknown or required dependencies are missing.
    """
    setup_rich_logging(verbose, console=console)
    if dataset not in _DATASETS:
        console.print(f"[red]Dataset desconhecido:[/red] {dataset}")
        raise typer.Exit(1)

    try:
        from pdet_fetcher import extract_columns_for_dataset

        if extract_columns_for_dataset is None:
            raise ImportError
    except ImportError:
        console.print(
            "[red]Erro:[/red] columns requer extras de análise: "
            "quantilica install pdet --extra analysis"
        )
        raise typer.Exit(1) from None

    cfg = _DATASETS[dataset]
    output_file = output / f"{dataset}-columns.csv"
    extract_columns_for_dataset(
        input,
        cfg["glob_pattern"],
        output_file,
        encoding=cfg["encoding"],
        has_uf=cfg["has_uf"],
    )
    console.print(f"[green]✓[/green] Colunas salvas em [bold]{output_file}[/bold]")
