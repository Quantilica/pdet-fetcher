"""Tests for the Hive lake conversion of the PDET microdata (RAIS and CAGED)."""

from __future__ import annotations

import csv
import io
import re
import shutil
import subprocess
import unicodedata
import zipfile
from importlib.metadata import entry_points
from pathlib import Path

import pytest

pytest.importorskip("quantilica.analytics.lake")
pytest.importorskip("duckdb")

from quantilica.analytics.lake import check_dataset, scan_dataset  # noqa: E402
from typer.testing import CliRunner  # noqa: E402

from pdet_fetcher.lake import (  # noqa: E402
    SOURCE_ID,
    SPECS,
    ConvertResult,
    ano_mes_from_path,
    ano_regiao_from_path,
    converter,
    latest_files,
)

DATA_DIR = Path(__file__).parent / "data"

RAIS_VINCULOS_CSV = (
    "Município;Tempo Emprego;Sexo Trabalhador\n355030;12,5;1\n330455;3,0;2\n"
)
RAIS_VINCULOS_2026_CSV = (
    '"Município - Código","Tempo Emprego","Sexo Trabalhador"\n'
    '"355030","12.5","1"\n'
    '"330455","3.0","2"\n'
)
RAIS_ESTAB_CSV = "Município;Qtd Vínculos Ativos\n355030;10\n330455;7\n"
CAGED_CSV = "Município;Sexo;Salário Mensal\n355030;1;1.500,00\n330455;3;2.000,00\n"
NOVO_CAGED_CSV = (
    "competênciamov;município;sexo;salário\n"
    "202001;355030;1;1500,00\n"
    "202001;330455;3;2000,00\n"
)
HTML_PAGE = "<!DOCTYPE html>\n<html><body>Service unavailable</body></html>\n"

SEVEN_ZIP = shutil.which("7z") or shutil.which("7zz") or shutil.which("7za")


def _zip(path: Path, member: str, text: str, encoding: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member, text.encode(encoding))
    return path


def _raw(
    root: Path, dataset_id: str, name: str, text: str, *, encoding: str = "latin-1"
) -> Path:
    """Write ``<root>/<dataset_id>/<year>/<name>`` as a zip holding ``text``."""
    year = name.split("_")[1][:4]
    return _zip(root / dataset_id / year / name, "data.csv", text, encoding)


@pytest.fixture
def raw(tmp_path: Path) -> Path:
    root = tmp_path / "raw"
    _raw(
        root, "rais-vinculos", "rais-vinculos_2025-sul@20260513.zip", RAIS_VINCULOS_CSV
    )
    _raw(root, "rais-vinculos", "rais-vinculos_2025-sp@20260513.zip", RAIS_VINCULOS_CSV)
    _raw(root, "rais-vinculos", "rais-vinculos_2024@20250101.zip", RAIS_VINCULOS_CSV)
    _raw(
        root,
        "rais-estabelecimentos",
        "rais-estabelecimentos_2024@20250101.zip",
        RAIS_ESTAB_CSV,
    )
    _raw(root, "caged", "caged_201901@20260603.zip", CAGED_CSV)
    _raw(root, "caged-ajustes", "caged-ajustes_2002@20171107.zip", CAGED_CSV)
    _raw(
        root,
        "caged-2020-mov",
        "caged-2020-mov_202001@20260608.zip",
        NOVO_CAGED_CSV,
        encoding="utf-8",
    )
    return root


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    return tmp_path / "lake"


def _dataset_dir(lake: Path, dataset_id: str) -> Path:
    return lake / SOURCE_ID / dataset_id


# --- partitions from the file identity ------------------------------------------


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("rais-vinculos_2025-sul@20260513.7z", ("2025", "sul")),
        ("rais-vinculos_2025-centrooeste@20260513.7z", ("2025", "centrooeste")),
        ("rais-vinculos_2024", ("2024", None)),
        ("rais-estabelecimentos_2024@20260513.zip", ("2024", None)),
        ("rais-vinculos/2024/dados.7z", ("2024", None)),
    ],
)
def test_ano_regiao_from_path(name: str, expected: tuple[str, str | None]) -> None:
    assert ano_regiao_from_path(name) == expected


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("caged_201901@20260603.7z", ("2019", "01")),
        ("caged-2020-mov_202012@20260608.7z", ("2020", "12")),
        ("2020/202001/CAGEDMOV202001.7z", ("2020", "01")),
        ("2020/202001/dados.7z", ("2020", "01")),
        ("caged-ajustes_2002@20171107.7z", ("2002", None)),
        ("caged-ajustes_201912@20200124.7z", ("2019", "12")),
    ],
)
def test_ano_mes_from_path(name: str, expected: tuple[str, str | None]) -> None:
    assert ano_mes_from_path(name) == expected


def test_latest_files_keeps_the_newest_stamp_and_ignores_sidecars(
    tmp_path: Path,
) -> None:
    folder = tmp_path / "caged" / "2019"
    folder.mkdir(parents=True)
    for name in (
        "caged_201901@20190201.7z",
        "caged_201901@20260603.7z",
        "caged_201902@20190301.7z",
        "caged_201902@20190301.7z.manifest.json",
        "leia-me.txt",
    ):
        (folder / name).write_bytes(b"x")

    assert [p.name for p in latest_files(tmp_path / "caged")] == [
        "caged_201901@20260603.7z",
        "caged_201902@20190301.7z",
    ]


# --- conversion ----------------------------------------------------------------


def test_convert_writes_hive_layout(raw: Path, lake: Path) -> None:
    result = converter.convert(raw, lake)

    assert isinstance(result, ConvertResult)
    assert (result.converted, result.skipped, result.errors) == (7, 0, [])
    vinculos = _dataset_dir(lake, "rais-vinculos")
    assert (
        vinculos / "ano=2025" / "regiao=sul" / "rais-vinculos_2025-sul.parquet"
    ).is_file()
    assert (
        vinculos / "ano=2025" / "regiao=sp" / "rais-vinculos_2025-sp.parquet"
    ).is_file()
    assert (
        vinculos
        / "ano=2024"
        / "regiao=__HIVE_DEFAULT_PARTITION__"
        / "rais-vinculos_2024.parquet"
    ).is_file()
    assert (
        _dataset_dir(lake, "rais-estabelecimentos")
        / "ano=2024"
        / "rais-estabelecimentos_2024.parquet"
    ).is_file()
    assert (
        _dataset_dir(lake, "caged") / "ano=2019" / "mes=01" / "caged_201901.parquet"
    ).is_file()
    assert (
        _dataset_dir(lake, "caged-2020-mov")
        / "ano=2020"
        / "mes=01"
        / "caged-2020-mov_202001.parquet"
    ).is_file()
    assert (
        _dataset_dir(lake, "caged-ajustes")
        / "ano=2002"
        / "mes=__HIVE_DEFAULT_PARTITION__"
        / "caged-ajustes_2002.parquet"
    ).is_file()
    assert (vinculos / "_dataset.json").is_file()


def test_rais_rows_and_text_columns(raw: Path, lake: Path) -> None:
    converter.convert(raw, lake, datasets=["rais-vinculos"])

    frame = scan_dataset(_dataset_dir(lake, "rais-vinculos")).collect()
    assert frame.height == 6
    assert {"municipio", "tempo_emprego", "ano", "regiao"} <= set(frame.columns)
    assert sorted(frame["ano"].unique().to_list()) == [2024, 2025]
    assert "sul" in frame["regiao"].to_list()


def test_caged_keeps_text_and_zero_padded_month(raw: Path, lake: Path) -> None:
    converter.convert(raw, lake, datasets=["caged"])

    frame = scan_dataset(_dataset_dir(lake, "caged")).collect()
    assert frame.height == 2
    assert frame["mes"].unique().to_list() == ["01"]
    assert sorted(frame["salario_mensal"].to_list()) == ["1.500,00", "2.000,00"]


def test_novo_caged_is_read_as_utf8(raw: Path, lake: Path) -> None:
    result = converter.convert(raw, lake, datasets=["caged-2020-mov"])

    assert result.errors == []
    frame = scan_dataset(_dataset_dir(lake, "caged-2020-mov")).collect()
    assert {"competenciamov", "municipio", "salario"} <= set(frame.columns)
    assert frame.height == 2


def test_rais_2026_comma_format_is_converted(tmp_path: Path, lake: Path) -> None:
    root = tmp_path / "raw"
    _raw(
        root,
        "rais-vinculos",
        "rais-vinculos_2025-norte@20260513.zip",
        RAIS_VINCULOS_2026_CSV,
    )

    result = converter.convert(root, lake)

    assert (result.converted, result.errors) == (1, [])
    frame = scan_dataset(_dataset_dir(lake, "rais-vinculos")).collect()
    assert frame.height == 2
    assert "tempo_emprego" in frame.columns
    assert "municipio" in frame.columns
    assert "municipio_codigo" not in frame.columns


@pytest.mark.skipif(SEVEN_ZIP is None, reason="7z not installed")
def test_7z_archive_is_converted(tmp_path: Path, lake: Path) -> None:
    root = tmp_path / "raw"
    source = tmp_path / "stage" / "CAGEDMOV202001.txt"
    source.parent.mkdir()
    source.write_text(CAGED_CSV, encoding="latin-1")
    archive = root / "caged" / "2020" / "202001" / "CAGEDMOV202001.7z"
    archive.parent.mkdir(parents=True)
    subprocess.run(
        [SEVEN_ZIP, "a", "-bd", str(archive), str(source)],
        check=True,
        capture_output=True,
    )

    result = converter.convert(root, lake)

    assert (result.converted, result.errors) == (1, [])
    parquet = (
        _dataset_dir(lake, "caged") / "ano=2020" / "mes=01" / "CAGEDMOV202001.parquet"
    )
    assert parquet.is_file()
    assert scan_dataset(_dataset_dir(lake, "caged")).collect().height == 2


# --- conformance and idempotence --------------------------------------------------


def test_converted_datasets_conform_to_contract(raw: Path, lake: Path) -> None:
    converter.convert(raw, lake)

    for dataset_id in SPECS:
        dataset_dir = _dataset_dir(lake, dataset_id)
        if dataset_dir.exists():
            assert check_dataset(dataset_dir) == [], dataset_id


def test_second_run_skips_everything(raw: Path, lake: Path) -> None:
    converter.convert(raw, lake)
    again = converter.convert(raw, lake)

    assert (again.converted, again.skipped, again.errors) == (0, 7, [])


def test_force_reconverts(raw: Path, lake: Path) -> None:
    converter.convert(raw, lake)
    forced = converter.convert(raw, lake, force=True)

    assert (forced.converted, forced.skipped) == (7, 0)
    assert check_dataset(_dataset_dir(lake, "rais-vinculos")) == []


def test_republished_file_replaces_instead_of_accumulating(
    raw: Path, lake: Path
) -> None:
    converter.convert(raw, lake)
    _raw(
        raw,
        "caged",
        "caged_201901@20261101.zip",
        CAGED_CSV + "355030;2;900,00\n",
    )

    result = converter.convert(raw, lake, datasets=["caged"])

    assert (result.converted, result.skipped) == (1, 0)
    caged = _dataset_dir(lake, "caged")
    assert len(list(caged.rglob("*.parquet"))) == 1
    assert scan_dataset(caged).collect().height == 3
    assert check_dataset(caged) == []


# --- errors --------------------------------------------------------------------


def test_html_error_page_is_rejected_and_others_continue(raw: Path, lake: Path) -> None:
    _raw(raw, "caged", "caged_201902@20260603.zip", HTML_PAGE)

    result = converter.convert(raw, lake, datasets=["caged"])

    assert result.converted == 1
    assert len(result.errors) == 1
    assert "caged_201902@20260603.zip" in result.errors[0]
    caged = _dataset_dir(lake, "caged")
    assert not (caged / "ano=2019" / "mes=02").exists()
    assert check_dataset(caged) == []


def test_corrupt_archive_is_reported_without_aborting(raw: Path, lake: Path) -> None:
    bad = raw / "caged" / "2019" / "caged_201903@20260603.zip"
    bad.write_bytes(b"not a zip")

    result = converter.convert(raw, lake, datasets=["caged"])

    assert result.converted == 1
    assert len(result.errors) == 1
    assert "caged_201903" in result.errors[0]


def test_dataset_filter(raw: Path, lake: Path) -> None:
    result = converter.convert(raw, lake, datasets=["rais-estabelecimentos"])

    assert result.converted == 1
    assert not _dataset_dir(lake, "caged").exists()


def test_unknown_dataset_is_reported_as_error(raw: Path, lake: Path) -> None:
    result = converter.convert(raw, lake, datasets=["nao-existe"])

    assert result.converted == 0
    assert result.errors == ["dataset desconhecido: nao-existe"]


def test_missing_input_dir_is_reported_as_error(tmp_path: Path, lake: Path) -> None:
    result = converter.convert(tmp_path / "ausente", lake)

    assert result.converted == 0
    assert len(result.errors) == 1


# --- required columns against the published layouts ----------------------------------


def _parse_header(line: str) -> list[str]:
    delimiter = ";" if line.count(";") >= line.count(",") else ","
    return next(csv.reader(io.StringIO(line), delimiter=delimiter))


def _normalized(header: list[str]) -> list[str]:
    names = []
    for column in header:
        text = unicodedata.normalize("NFKD", column).encode("ascii", "ignore").decode()
        names.append(re.sub(r"[^0-9a-zA-Z]+", "_", text).strip("_").lower())
    return names


def _published_headers() -> list[tuple[str, list[str]]]:
    headers = []
    for line in (
        (DATA_DIR / "pdet_headers.tsv").read_text(encoding="utf-8").splitlines()
    ):
        if line and not line.startswith("#"):
            dataset, _first, _last, header = line.split("\t", 3)
            headers.append((dataset, _normalized(_parse_header(header))))
    for line in (
        (DATA_DIR / "rais_vinculos_headers.tsv")
        .read_text(encoding="utf-8")
        .splitlines()
    ):
        if line and not line.startswith("#"):
            _year, header = line.split("\t", 1)
            headers.append(("rais-vinculos", _normalized(_parse_header(header))))
    return headers


@pytest.mark.parametrize(
    ("dataset", "columns"),
    _published_headers(),
)
def test_required_columns_exist_in_every_published_layout(
    dataset: str, columns: list[str]
) -> None:
    spec = SPECS[dataset]
    renamed = [spec.rename.get(name, name) for name in columns]

    assert set(spec.required) <= set(renamed)
    # The rename must not merge two columns of the same file.
    assert len(set(renamed)) == len(set(columns))


# --- registration and CLI ------------------------------------------------------------


def test_entry_point_is_registered() -> None:
    names = {ep.name: ep.value for ep in entry_points(group="quantilica.lake")}
    assert names.get(SOURCE_ID) == "pdet_fetcher.lake:converter"


def test_cli_convert_end_to_end(raw: Path, lake: Path) -> None:
    from pdet_fetcher.plugin import app

    result = CliRunner().invoke(
        app,
        ["convert", "-i", str(raw), "--lake", str(lake), "--dataset", "caged"],
    )

    assert result.exit_code == 0, result.output
    assert check_dataset(_dataset_dir(lake, "caged")) == []


def test_cli_convert_exits_1_on_errors(raw: Path, lake: Path) -> None:
    from pdet_fetcher.plugin import app

    _raw(raw, "caged", "caged_201904@20260603.zip", HTML_PAGE)

    result = CliRunner().invoke(app, ["convert", "-i", str(raw), "--lake", str(lake)])

    assert result.exit_code == 1
