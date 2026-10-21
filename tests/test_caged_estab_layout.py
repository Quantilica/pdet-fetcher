"""CAGED, Novo CAGED and RAIS Estabelecimentos: column names from the file header."""

import csv
import io
from pathlib import Path

import polars as pl
import pytest
from quantilica.core.exceptions import ParseError

from pdet_fetcher.constants import (
    CAGED_2020_EXC_COLUMNS,
    CAGED_2020_FOR_COLUMNS,
    CAGED_2020_MOV_COLUMNS,
    CAGED_AJUSTES_COLUMNS,
    CAGED_COLUMNS,
    RAIS_ESTABELECIMENTOS_COLUMNS,
)
from pdet_fetcher.reader import (
    _resolve_columns,
    convert_columns_dtypes,
    read_caged,
    read_rais,
    resolve_caged_columns,
    resolve_rais_estabelecimentos_columns,
)

HEADERS_FILE = Path(__file__).parent / "data" / "pdet_headers.tsv"

POSITIONAL = {
    "caged": CAGED_COLUMNS,
    "caged-ajustes": CAGED_AJUSTES_COLUMNS,
    "caged-2020-mov": CAGED_2020_MOV_COLUMNS,
    "caged-2020-for": CAGED_2020_FOR_COLUMNS,
    "caged-2020-exc": CAGED_2020_EXC_COLUMNS,
    "rais-estabelecimentos": RAIS_ESTABELECIMENTOS_COLUMNS,
}

# Names that change on purpose in Novo CAGED: "seção" is the CNAE 2.0 section
# (a letter), not the class; "indicador_aprendiz " lost its trailing space.
NOVO_CAGED_RENAMED = {
    "cnae_2": "cnae_2_secao",
    "indicador_aprendiz ": "indicador_aprendiz",
}


def _layouts() -> list[tuple[str, int, int, list[str]]]:
    layouts = []
    for line in HEADERS_FILE.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        dataset, first, last, header = line.split("\t", 3)
        separator = max((";", "\t", ","), key=header.count)
        columns = next(csv.reader(io.StringIO(header), delimiter=separator))
        layouts.append((dataset, int(first), int(last), columns))
    return layouts


LAYOUTS = _layouts()
IDS = [f"{d}-{f}" for d, f, _, _ in LAYOUTS]


def _resolve(dataset: str, columns: list[str]) -> list[str]:
    if dataset == "rais-estabelecimentos":
        return resolve_rais_estabelecimentos_columns(columns)
    return resolve_caged_columns(columns, dataset)


def test_fixture_covers_every_published_layout():
    counts: dict[str, int] = {}
    for dataset, *_ in LAYOUTS:
        counts[dataset] = counts.get(dataset, 0) + 1
    assert counts == {
        "caged": 2,
        "caged-ajustes": 4,
        "caged-2020-mov": 1,
        "caged-2020-for": 1,
        "caged-2020-exc": 1,
        "rais-estabelecimentos": 15,
    }


@pytest.mark.parametrize(("dataset", "first", "last", "columns"), LAYOUTS, ids=IDS)
def test_header_gets_the_positional_names(dataset, first, last, columns):
    """By name, every published layout gets the names it had by position, in
    every period it covers (the RAIS Estabelecimentos layouts alternate: 1990
    has the 1985 header and 1998 the 1996 one), except the two Novo CAGED
    renames."""
    names = _resolve(dataset, columns)
    for period in (first, last):
        expected = list(_resolve_columns(POSITIONAL[dataset], period))
        if dataset.startswith("caged-2020"):
            expected = [NOVO_CAGED_RENAMED.get(n, n) for n in expected]
        assert names == expected, period


def test_rais_estabelecimentos_uf_is_abbreviation_until_2001_and_code_after():
    old = next(
        c for d, f, _, c in LAYOUTS if d == "rais-estabelecimentos" and f == 2001
    )
    new = next(
        c for d, f, _, c in LAYOUTS if d == "rais-estabelecimentos" and f == 2014
    )
    assert "sigla_uf" in resolve_rais_estabelecimentos_columns(old)
    assert "id_uf" in resolve_rais_estabelecimentos_columns(new)


def test_unknown_caged_header_raises():
    with pytest.raises(ParseError, match="Coluna Nova"):
        resolve_caged_columns(["competênciamov", "Coluna Nova"], "caged-2020-mov")


def test_unknown_caged_dataset_raises():
    with pytest.raises(ValueError, match="Unknown CAGED dataset"):
        resolve_caged_columns(["uf"], "caged-2030")


def test_unknown_rais_estabelecimentos_header_raises():
    with pytest.raises(ParseError, match="Coluna Nova"):
        resolve_rais_estabelecimentos_columns(["Município", "Coluna Nova"])


# ---------------------------------------------------------------------------
# Indicator columns
# ---------------------------------------------------------------------------


def test_indicator_nine_is_not_identified_not_true():
    df = pl.DataFrame({"indicador_trabalho_parcial": ["0", "1", "9", None, " 1"]})
    out = convert_columns_dtypes(df)
    assert out["indicador_trabalho_parcial"].to_list() == [
        False,
        True,
        None,
        None,
        True,
    ]
    assert out.schema["indicador_trabalho_parcial"] == pl.Boolean


def test_unexpected_indicator_code_raises():
    df = pl.DataFrame({"indicador_aprendiz": ["0", "2"]})
    with pytest.raises(ParseError, match="indicador_aprendiz"):
        convert_columns_dtypes(df)


# ---------------------------------------------------------------------------
# read_caged / read_rais end to end, on small files
# ---------------------------------------------------------------------------


def test_read_novo_caged(tmp_path):
    path = tmp_path / "CAGEDEXC202608.txt"
    path.write_text(
        "\ufeffcompetênciamov;região;uf;município;seção;subclasse;saldomovimentação;"
        "indicadoraprendiz;competênciadec;competênciaexc;indicadordeexclusão;"
        "salário;valorsaláriofixo\n"
        "202001;5;52;520870;H;4921302;-1;0;202001;202608;1;2116,82;2116,82\n",
        encoding="utf-8",
    )
    row = read_caged(path, date=202608, dataset="caged-2020-exc").row(0, named=True)
    assert row["competencia_movimentacao"] == 202001
    assert row["cnae_2_secao"] == "H"
    assert row["cnae_2_subclasse"] == "4921302"
    assert row["saldo_movimento"] == -1
    assert row["indicador_aprendiz"] is False
    assert row["competencia_declarada"] == 202001
    assert row["competencia_exclusao"] == 202608
    assert row["indicador_exclusao"] is True
    assert row["salario_mensal"] == 2116.82
    assert row["valor_salario_fixo"] == 2116.82


def test_read_caged_classic(tmp_path):
    path = tmp_path / "CAGEDEST_122019.txt"
    path.write_bytes(
        (
            "Admitidos/Desligados;Competência Declarada;Município;Ano Declarado;"
            "Salário Mensal;Saldo Mov;Tempo Emprego;UF\n"
            "01;201912;110060;2019;000998,00;01;0000,0;11\n"
        ).encode("latin-1")
    )
    row = read_caged(path, date=201912, dataset="caged").row(0, named=True)
    assert row["competencia_movimentacao"] == 201912
    assert row["ano_movimentacao"] == 2019
    assert row["salario_mensal"] == 998.0
    assert row["saldo_movimento"] == 1
    assert row["id_uf"] == "11"


def test_read_caged_ajustes_keeps_declared_competence(tmp_path):
    path = tmp_path / "CAGEDEST_AJUSTES_2002.txt"
    path.write_bytes(
        (
            "Competência Movimentação;Ano Movimentação;"
            "Competência Declarada;Tipo Estab\n"
            "200201;2002;200203;01\n"
        ).encode("latin-1")
    )
    row = read_caged(path, date=2002, dataset="caged-ajustes").row(0, named=True)
    assert row["competencia_movimentacao"] == 200201
    assert row["competencia_declarada"] == 200203
    assert row["tipo_estabelecimento"] == "01"


def test_read_rais_estabelecimentos_1985_format(tmp_path):
    path = tmp_path / "ESTB1990.txt"
    path.write_bytes(
        "BAIRR SP;ESTOQUE;MUNICIPIO;TAMESTAB;TIPO IDENT;UF;SUBS IBGE;SUBATIV IBGE\n"
        "00000;151;110020;6;1;RO;INST FINANC;4510\n".encode("latin-1")
    )
    row = read_rais(path, year=1990, dataset="estabelecimentos").row(0, named=True)
    assert row["quantidade_vinculos_ativos"] == 151
    assert row["sigla_uf"] == "RO"
    assert row["id_municipio_6"] == "110020"


def test_read_rais_estabelecimentos_new_format(tmp_path):
    path = tmp_path / "RAIS_ESTAB_PUB.COMT"
    path.write_bytes(
        (
            '"Qtd Vínculos Ativos","Ind Estab Participante SIMPLES - Código",'
            '"Município - Código","CNAE 2.0 Subclasse - Codigo",'
            '"UF - Código","CEP Estab"\n'
            '28,1,351880," 4530703",35,7112000\n'
        ).encode("latin-1")
    )
    row = read_rais(path, year=2024, dataset="estabelecimentos").row(0, named=True)
    assert row["quantidade_vinculos_ativos"] == 28
    assert row["indicador_simples"] is True
    assert row["id_municipio_6"] == "351880"
    assert row["cnae_2_subclasse"] == "4530703"
    assert row["id_uf"] == "35"
