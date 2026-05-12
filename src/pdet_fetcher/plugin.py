# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.

"""Typer plugin for quantilica-cli integration."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.table import Table

from pdet_fetcher import (
    connect,
    convert_caged,
    convert_rais,
    extract_columns_for_dataset,
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

app = typer.Typer(help="Microdados do PDET (CAGED, RAIS).")

_DEFAULT_OUTPUT = Path("/data/pdet")
console = Console()

_DATASETS = {
    "rais-estabelecimentos": {"glob_pattern": "rais-*.*", "has_uf": True, "encoding": "latin-1"},
    "rais-vinculos": {"glob_pattern": "rais-*.*", "has_uf": True, "encoding": "latin-1"},
    "caged": {"glob_pattern": "caged_*.*", "has_uf": False, "encoding": "latin-1"},
    "caged-ajustes": {"glob_pattern": "caged-ajustes_*.*", "has_uf": False, "encoding": "latin-1"},
    "caged-2020": {"glob_pattern": "caged-2020-*.*", "has_uf": False, "encoding": "utf-8"},
}


@app.command("fetch")
def cmd_fetch(
    output: Annotated[
        Path, typer.Option("-o", "--output", help="Diretório de destino")
    ] = _DEFAULT_OUTPUT,
    verbose: Annotated[
        bool, typer.Option("--verbose", help="Exibir logs detalhados em vez de barra de progresso")
    ] = False,
) -> None:
    """Baixar dados do PDET via FTP."""
    if not verbose:
        logging.getLogger("pdet_fetcher").setLevel(logging.WARNING)
        logging.getLogger("quantilica_core").setLevel(logging.WARNING)
    show_progress = not verbose
    with console.status("[cyan]Conectando ao FTP do PDET...[/cyan]"):
        ftp = connect()
    try:
        fetch_rais(ftp=ftp, dest_dir=output, show_progress=show_progress)
        fetch_rais_docs(ftp=ftp, dest_dir=output, show_progress=show_progress)
        fetch_caged(ftp=ftp, dest_dir=output, show_progress=show_progress)
        fetch_caged_docs(ftp=ftp, dest_dir=output, show_progress=show_progress)
        fetch_caged_2020(ftp=ftp, dest_dir=output, show_progress=show_progress)
        fetch_caged_2020_docs(ftp=ftp, dest_dir=output, show_progress=show_progress)
    finally:
        ftp.close()
    console.print("[green]✓[/green] Download concluído.")


@app.command("list")
def cmd_list(
    output: Annotated[
        Path, typer.Option("-o", "--output", help="Diretório de referência")
    ] = _DEFAULT_OUTPUT,
) -> None:
    """Listar arquivos disponíveis no FTP."""
    with console.status("[cyan]Conectando ao FTP do PDET...[/cyan]"):
        ftp = connect()
    try:
        t = Table(show_header=True, header_style="bold")
        t.add_column("Dataset", style="cyan")
        t.add_column("Ano", justify="right")
        t.add_column("Arquivo")
        t.add_column("Destino")

        for listing in (list_caged, list_caged_2020, list_rais):
            for f in listing(ftp):
                dest = output / f["dataset"] / str(f["year"]) / f["name"]
                if not dest.exists():
                    t.add_row(f["dataset"], str(f["year"]), f["name"], str(dest))
    finally:
        ftp.close()
    console.print(t)


@app.command("convert")
def cmd_convert(
    input: Annotated[
        Path, typer.Option("-i", "--input", help="Diretório de origem com arquivos brutos")
    ],
    output: Annotated[
        Path, typer.Option("-o", "--output", help="Diretório de destino para Parquet")
    ] = _DEFAULT_OUTPUT,
) -> None:
    """Converter arquivos brutos para Parquet."""
    convert_rais(input, output)
    convert_caged(input, output)
    console.print("[green]✓[/green] Conversão concluída.")


@app.command("columns")
def cmd_columns(
    dataset: Annotated[str, typer.Argument(help=f"Dataset ({', '.join(_DATASETS.keys())})")],
    input: Annotated[
        Path, typer.Option("-i", "--input", help="Diretório de origem com arquivos brutos")
    ],
    output: Annotated[
        Path, typer.Option("-o", "--output", help="Diretório para CSV de colunas")
    ] = Path("."),
) -> None:
    """Extrair nomes de colunas dos arquivos brutos."""
    if dataset not in _DATASETS:
        console.print(f"[red]Dataset desconhecido:[/red] {dataset}", stderr=True)
        raise typer.Exit(1)
    cfg = _DATASETS[dataset]
    output_file = output / f"{dataset}-columns.csv"
    extract_columns_for_dataset(input, cfg["glob_pattern"], output_file, encoding=cfg["encoding"], has_uf=cfg["has_uf"])
    console.print(f"[green]✓[/green] Colunas salvas em [bold]{output_file}[/bold]")
