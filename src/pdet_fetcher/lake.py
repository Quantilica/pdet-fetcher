"""Convert the raw PDET microdata (RAIS, CAGED) into the Hive-partitioned lake.

Each raw archive (``.7z`` or ``.zip``) lands in
``<lake>/pdet/<dataset_id>/<key>=<value>/`` as ``<file_key>.parquet``. The
partition comes from the file's own name or folder, never from its rows:

* ``rais-vinculos``: ``ano=`` and ``regiao=``
  (``rais-vinculos_2025-sul@20260513.7z`` -> ``ano=2025/regiao=sul``; a file
  without region goes to ``regiao=__HIVE_DEFAULT_PARTITION__``);
* ``rais-estabelecimentos``: ``ano=``;
* ``caged``, ``caged-ajustes`` and ``caged-2020-*``: ``ano=`` and ``mes=``
  (``caged_201901@20260603.7z`` -> ``ano=2019/mes=01``; files published per
  year, such as ``caged-ajustes_2002``, go to ``mes=__HIVE_DEFAULT_PARTITION__``).

Columns are kept as text with normalized names.
"""

from __future__ import annotations

import re
import unicodedata
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

try:
    import duckdb
    from quantilica.analytics.lake import LakeError, LakeWriter
    from quantilica.core.lake import DatasetDescriptor, file_key
except ImportError as exc:
    raise ImportError(
        "pdet_fetcher.lake requer o extra 'analysis': "
        "uv add 'pdet-fetcher[analysis]' "
        "--index https://index.quantilica.com/simple/"
    ) from exc

from pdet_fetcher import __version__
from pdet_fetcher.constants import (
    RAIS_ESTABELECIMENTOS_HEADER_NAMES,
    RAIS_VINCULOS_HEADER_NAMES,
)

SOURCE_ID = "pdet"

# Raw files are archives; the ``.manifest.json`` sidecars next to them are not.
ARCHIVE_SUFFIXES = frozenset({".7z", ".zip"})

# The 2023-2025 RAIS republications (2026 format) are comma-separated.
FALLBACK_SEPARATOR = ","

_ANO_REGIAO = re.compile(r"_(?P<ano>\d{4})(?:-(?P<regiao>[0-9A-Za-z._-]+))?$")
_ANO_MES = re.compile(r"(?P<ano>(?:19|20)\d{2})(?P<mes>0[1-9]|1[0-2])(?!\d)")
_DATE_STAMP = re.compile(r"@(?P<stamp>[0-9T]+)$")
_YEAR = re.compile(r"^(?:19|20)\d{2}$")
_YEAR_MONTH = re.compile(r"^(?P<ano>(?:19|20)\d{2})(?P<mes>0[1-9]|1[0-2])$")


@dataclass(frozen=True)
class DatasetSpec:
    """How one raw dataset is read and registered in the lake."""

    dataset_id: str
    partition_keys: tuple[str, ...]
    required: tuple[str, ...]
    encoding: str = "latin-1"
    separator: str = ";"
    header: bool = True
    rename: dict[str, str] = field(default_factory=dict)

    @property
    def alias(self) -> str:
        """Short view name: the dataset id as a SQL-friendly identifier."""
        return self.dataset_id.replace("-", "_")

    @property
    def partition_types(self) -> dict[str, str]:
        """Partition types: the year is a number, the rest stays text."""
        return {
            key: "int" if key == "ano" else "varchar" for key in self.partition_keys
        }


def _slug(text: str) -> str:
    """Normalize a header as the lake writer does (``Município`` -> ``municipio``)."""
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-z]+", "_", ascii_text.lower()).strip("_")


def _drop_codigo_suffix(*header_maps: dict) -> dict[str, str]:
    """Rename map that undoes the ``" - Código"`` suffix of the 2026 RAIS format.

    ``Município - Código`` arrives as ``municipio_codigo``; the older layouts call
    the same column ``municipio``. The map is built from the known header names,
    so only columns that exist in the older layouts are renamed.
    """
    names = {_slug(header) for header_map in header_maps for header in header_map}
    return {f"{name}_codigo": name for name in names}


RAIS_RENAME = _drop_codigo_suffix(
    RAIS_VINCULOS_HEADER_NAMES, RAIS_ESTABELECIMENTOS_HEADER_NAMES
)

# ``required`` holds the normalized column names that every published layout of the
# dataset has; a file whose header lacks them (an HTML error page saved as CSV, a
# comma-separated file read as semicolon) is rejected before anything is written.
SPECS: dict[str, DatasetSpec] = {
    spec.dataset_id: spec
    for spec in (
        DatasetSpec(
            "rais-vinculos",
            ("ano", "regiao"),
            required=("tempo_emprego",),
            rename=RAIS_RENAME,
        ),
        DatasetSpec(
            "rais-estabelecimentos",
            ("ano",),
            required=("municipio",),
            rename=RAIS_RENAME,
        ),
        DatasetSpec("caged", ("ano", "mes"), required=("municipio", "sexo")),
        DatasetSpec("caged-ajustes", ("ano", "mes"), required=("municipio", "sexo")),
        # The Novo CAGED files are UTF-8, unlike every other PDET dataset.
        *(
            DatasetSpec(
                dataset_id,
                ("ano", "mes"),
                required=("municipio", "sexo"),
                encoding="utf-8",
            )
            for dataset_id in ("caged-2020-exc", "caged-2020-for", "caged-2020-mov")
        ),
    )
}


@dataclass
class ConvertResult:
    """Counts of a conversion run; ``errors`` holds one message per failure."""

    converted: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


def _year_dir(path: Path) -> str | None:
    """Year taken from the folder holding ``path`` (``<dataset>/2020/file``)."""
    for parent in path.parents:
        if _YEAR.match(parent.name):
            return parent.name
    return None


def ano_regiao_from_path(path: Path | str) -> tuple[str | None, str | None]:
    """Return ``(ano, regiao)`` for a RAIS raw file.

    ``rais-vinculos_2025-sul@20260513.7z`` -> ``("2025", "sul")``;
    ``rais-vinculos_2024.7z`` -> ``("2024", None)``. Without a year in the
    name, the year folder (``rais-vinculos/2024/...``) is used.
    """
    path = Path(path)
    match = _ANO_REGIAO.search(file_key(path))
    if match is None:
        return _year_dir(path), None
    return match["ano"], match["regiao"]


def ano_mes_from_path(path: Path | str) -> tuple[str | None, str | None]:
    """Return ``(ano, mes)`` for a CAGED raw file; ``mes`` is zero-padded.

    ``caged_201901@20260603.7z`` -> ``("2019", "01")``; a ``YYYYMM`` folder
    (``2020/202001/CAGEDMOV202001.7z``) works too. Files published per year
    (``caged-ajustes_2002.7z``) give ``("2002", None)``.
    """
    path = Path(path)
    stem = file_key(path)
    match = _ANO_MES.search(stem.rsplit("_", 1)[-1]) or _ANO_MES.search(stem)
    if match:
        return match["ano"], match["mes"]
    for parent in path.parents:
        folder = _YEAR_MONTH.match(parent.name)
        if folder:
            return folder["ano"], folder["mes"]
    year = re.search(r"_((?:19|20)\d{2})$", stem)
    return (year[1] if year else _year_dir(path)), None


def partitions_for(spec: DatasetSpec, path: Path | str) -> dict[str, str | None]:
    """Partition values of ``path`` for ``spec``, in the dataset's key order."""
    if spec.partition_keys == ("ano", "regiao"):
        ano, regiao = ano_regiao_from_path(path)
        return {"ano": ano, "regiao": regiao}
    if spec.partition_keys == ("ano",):
        return {"ano": ano_regiao_from_path(path)[0]}
    ano, mes = ano_mes_from_path(path)
    return {"ano": ano, "mes": mes}


def _stamp(path: Path) -> str:
    match = _DATE_STAMP.search(path.name.split(".", 1)[0])
    return match["stamp"] if match else ""


def latest_files(dataset_dir: Path) -> list[Path]:
    """Archives under ``dataset_dir``, keeping the newest ``@date`` per file key.

    A republished file (same key, newer stamp) replaces the old one in the lake,
    so converting both would only make them overwrite each other.
    """
    newest: dict[str, Path] = {}
    for path in sorted(dataset_dir.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in ARCHIVE_SUFFIXES:
            continue
        key = file_key(path)
        if key not in newest or _stamp(path) >= _stamp(newest[key]):
            newest[key] = path
    return sorted(newest.values())


class LakeConverter:
    """Converter registered under the ``quantilica.lake`` entry point."""

    source_id = SOURCE_ID

    def datasets(self) -> list[str]:
        """Dataset ids this converter writes to the lake."""
        return list(SPECS)

    def convert(
        self,
        input_dir: Path,
        lake_dir: Path,
        *,
        datasets: list[str] | None = None,
        force: bool = False,
    ) -> ConvertResult:
        """Convert the latest raw file of each key into ``lake_dir``.

        Args:
            input_dir: Root of the raw data tree (``<dataset_id>/<year>/<file>``).
            lake_dir: Root of the lake.
            datasets: Dataset ids to convert; ``None`` converts all.
            force: Reconvert even when the raw file's sha256 is already in the lake.

        Returns:
            ConvertResult: Files converted, skipped and the error messages.
        """
        input_dir = Path(input_dir)
        lake_dir = Path(lake_dir)
        result = ConvertResult()
        if not input_dir.is_dir():
            result.errors.append(f"diretório '{input_dir}' não existe")
            return result

        specs: list[DatasetSpec] = []
        for name in datasets or list(SPECS):
            spec = SPECS.get(name)
            if spec is None:
                result.errors.append(f"dataset desconhecido: {name}")
            elif spec not in specs:
                specs.append(spec)

        for spec in specs:
            self._convert_dataset(input_dir, spec, lake_dir, force, result)
        return result

    def _convert_dataset(
        self,
        input_dir: Path,
        spec: DatasetSpec,
        lake_dir: Path,
        force: bool,
        result: ConvertResult,
    ) -> None:
        dataset_dir = input_dir / spec.dataset_id
        if not dataset_dir.is_dir():
            return
        files = latest_files(dataset_dir)
        if not files:
            return
        writer = LakeWriter(
            lake_dir / SOURCE_ID / spec.dataset_id,
            DatasetDescriptor(
                source_id=SOURCE_ID,
                dataset_id=spec.dataset_id,
                alias=spec.alias,
                partition_keys=list(spec.partition_keys),
                partition_types=spec.partition_types,
            ),
            force=force,
        )
        for path in files:
            try:
                written = self._write(writer, spec, path)
            except (LakeError, OSError, zipfile.BadZipFile, duckdb.Error) as exc:
                message = str(exc).strip()
                first_line = message.splitlines()[0] if message else repr(exc)
                result.errors.append(f"{path.name}: {first_line}")
                continue
            if written.status == "skipped":
                result.skipped += 1
            else:
                result.converted += 1

    @staticmethod
    def _write(writer: LakeWriter, spec: DatasetSpec, path: Path):
        """Write ``path``, retrying RAIS files with a comma when ``;`` finds no header.

        The header check runs before anything is written, so a failed attempt
        leaves no trace in the lake. A real failure (corrupt archive, HTML page)
        fails both attempts and reports the first error.
        """
        separators = [spec.separator]
        if spec.dataset_id.startswith("rais-"):
            separators.append(FALLBACK_SEPARATOR)
        first_error: LakeError | None = None
        for separator in separators:
            try:
                return writer.write_duckdb(
                    path,
                    partitions=partitions_for(spec, path),
                    separator=separator,
                    encoding=spec.encoding,
                    header=spec.header,
                    rename=spec.rename,
                    required=spec.required,
                    converter=f"pdet-fetcher {__version__}",
                )
            except LakeError as exc:
                first_error = first_error or exc
        raise first_error  # type: ignore[misc]


converter = LakeConverter()
