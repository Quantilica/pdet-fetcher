"""RAIS Vínculos: column names from the file header, both PDET formats."""

import csv
import io
from pathlib import Path

import polars as pl
import pytest
from quantilica.core.exceptions import ParseError

from pdet_fetcher.constants import RAIS_VINCULOS_COLUMNS
from pdet_fetcher.reader import (
    _resolve_columns,
    convert_columns_dtypes,
    normalize_header,
    read_rais,
    resolve_rais_vinculos_columns,
)

HEADERS_FILE = Path(__file__).parent / "data" / "rais_vinculos_headers.tsv"

# Canonical names for the 2026 PDET format, in file order (62 columns).
NEW_FORMAT_NAMES = [
    "bairros_sp",
    "bairros_fortaleza",
    "bairros_rj",
    "causa_desligamento_1",
    "causa_desligamento_2",
    "causa_desligamento_3",
    "motivo_desligamento",
    "cbo_2002",
    "cnae_2",
    "cnae_1",
    "distritos_sp",
    "vinculo_ativo_3112",
    "faixa_etaria",
    "faixa_remuneracao_media_sm",
    "faixa_horas_contratadas",
    "faixa_remuneracao_dezembro_sm",
    "faixa_tempo_emprego",
    "grau_instrucao_apos_2005",
    "quantidade_horas_contratadas",
    "idade",
    "indicador_cei_vinculado",
    "indicador_simples",
    "mes_admissao",
    "mes_desligamento",
    "id_municipio_6_trabalho",
    "id_municipio_6",
    "nacionalidade",
    "natureza_juridica",
    "indicador_portador_deficiencia",
    "quantidade_dias_afastamento",
    "raca_cor",
    "regioes_administrativas_df",
    "valor_remuneracao_dezembro",
    "valor_remuneracao_dezembro_sm",
    "valor_remuneracao_media",
    "valor_remuneracao_media_sm",
    "cnae_2_subclasse",
    "sexo",
    "tamanho_estabelecimento",
    "tempo_emprego",
    "tipo_admissao",
    "tipo_estabelecimento",
    "tipo_estabelecimento_desc",
    "tipo_deficiencia",
    "tipo_vinculo",
    "subsetor_ibge",
    "valor_remuneracao_janeiro",
    "valor_remuneracao_fevereiro",
    "valor_remuneracao_marco",
    "valor_remuneracao_abril",
    "valor_remuneracao_maio",
    "valor_remuneracao_junho",
    "valor_remuneracao_julho",
    "valor_remuneracao_agosto",
    "valor_remuneracao_setembro",
    "valor_remuneracao_outubro",
    "valor_remuneracao_novembro",
    "ano_chegada_brasil",
    "indicador_trabalho_intermitente",
    "indicador_trabalho_parcial",
    "indicador_vinculo_abandonado",
    "categoria_trabalhador",
]


def _layouts() -> list[tuple[int, str, list[str]]]:
    layouts = []
    for line in HEADERS_FILE.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        year, header = line.split("\t", 1)
        separator = max((";", "\t", ","), key=header.count)
        columns = next(csv.reader(io.StringIO(header), delimiter=separator))
        layouts.append((int(year), separator, columns))
    return layouts


LAYOUTS = _layouts()
LEGACY = [(y, cols) for y, sep, cols in LAYOUTS if sep == ";"]
NEW = [(y, cols) for y, sep, cols in LAYOUTS if sep == ","]


def test_fixture_covers_every_known_layout():
    assert len(LEGACY) == 16
    assert len(NEW) == 1


@pytest.mark.parametrize(("year", "columns"), LEGACY, ids=[str(y) for y, _ in LEGACY])
def test_legacy_header_matches_positional_names(year, columns):
    """By name, every legacy layout gets exactly the names it had by position."""
    assert resolve_rais_vinculos_columns(columns) == list(
        _resolve_columns(RAIS_VINCULOS_COLUMNS, year)
    )


def test_new_format_header_is_mapped_by_name():
    (_, columns) = NEW[0]
    assert resolve_rais_vinculos_columns(columns) == NEW_FORMAT_NAMES


def test_new_format_reorders_remuneration_bands():
    """2026 files put "Faixa Rem Média" before "Faixa Hora Contrat"; position
    14 of the legacy layout is "Faixa Hora Contrat". Names must follow the
    header, not the position."""
    (_, columns) = NEW[0]
    names = resolve_rais_vinculos_columns(columns)
    assert columns[13].startswith("Faixa Rem Média")
    assert names[13] == "faixa_remuneracao_media_sm"
    assert names[14] == "faixa_horas_contratadas"


def test_normalize_header_drops_code_suffix_and_case():
    assert normalize_header("CNAE 2.0 Subclasse - Codigo") == "cnae 2.0 subclasse"
    assert normalize_header("Escolaridade Após 2005 - Código") == (
        "escolaridade após 2005"
    )
    assert normalize_header("  Vl  Rem Janeiro SC ") == "vl rem janeiro sc"
    assert normalize_header("Tipo Estabelecimento - Nome") == (
        "tipo estabelecimento - nome"
    )


def test_unknown_header_raises():
    with pytest.raises(ParseError, match="Coluna Nova"):
        resolve_rais_vinculos_columns(["Município", "Coluna Nova"])


def test_repeated_header_beyond_known_occurrences_raises():
    with pytest.raises(ParseError, match="Tipo Estab"):
        resolve_rais_vinculos_columns(["Tipo Estab", "Tipo Estab", "Tipo Estab"])


def test_two_headers_with_same_name_raise():
    with pytest.raises(ParseError, match="cbo_2002"):
        resolve_rais_vinculos_columns(["CBO Ocupação 2002", "CBO 2002 Ocupação"])


# ---------------------------------------------------------------------------
# Number parsing
# ---------------------------------------------------------------------------


def test_decimal_comma_keeps_legacy_behaviour():
    df = pl.DataFrame(
        {
            "valor_remuneracao_media": ["0000001437,25", "1.437,25"],
            "tempo_emprego": ["    39,7", "39,7"],
            "idade": ["024", "24"],
        }
    )
    out = convert_columns_dtypes(df, decimal_separator=",")
    assert out["valor_remuneracao_media"].to_list() == [1437.25, 1437.25]
    assert out["tempo_emprego"].to_list() == [39.7, 39.7]
    assert out["idade"].to_list() == [24, 24]


def test_decimal_point_is_not_read_as_thousands_separator():
    df = pl.DataFrame(
        {
            "valor_remuneracao_media": ["2956.01", ".00", None],
            "tempo_emprego": ["4.1", "30.5", "0"],
            "idade": ["24", " 37", "9"],
        }
    )
    out = convert_columns_dtypes(df, decimal_separator=".")
    assert out["valor_remuneracao_media"].to_list() == [2956.01, 0.0, None]
    assert out["tempo_emprego"].to_list() == [4.1, 30.5, 0.0]
    assert out["idade"].to_list() == [24, 37, 9]


def test_unsupported_decimal_separator_raises():
    with pytest.raises(ValueError, match="decimal separator"):
        convert_columns_dtypes(pl.DataFrame({"a": ["1"]}), decimal_separator=";")


# ---------------------------------------------------------------------------
# read_rais end to end, on small files in both formats
# ---------------------------------------------------------------------------


def _write(path: Path, text: str) -> Path:
    path.write_bytes(text.encode("latin1"))
    return path


def test_read_rais_legacy_format(tmp_path):
    path = _write(
        tmp_path / "RAIS_VINC_PUB_NORTE.txt",
        "Município;Vínculo Ativo 31/12;Tipo Estab;Tipo Estab;"
        "Vl Remun Média Nom;Tempo Emprego;Idade;CNAE 2.0 Subclasse\n"
        "150140;1;01;CNPJ;0000001437,25;    39,7;024;8630503\n",
    )
    df = read_rais(path, year=2021, dataset="vinculos")
    row = df.row(0, named=True)
    assert row["id_municipio_6"] == "150140"
    assert row["vinculo_ativo_3112"] is True
    assert row["tipo_estabelecimento"] == "01"
    assert row["tipo_estabelecimento_desc"] == "CNPJ"
    assert row["valor_remuneracao_media"] == 1437.25
    assert row["tempo_emprego"] == 39.7
    assert row["idade"] == 24


def test_read_rais_new_format(tmp_path):
    path = _write(
        tmp_path / "RAIS_VINC_PUB_NORTE.COMT",
        '"Município - Código","Ind Vínculo Ativo 31/12 - Código",'
        '"Faixa Rem Média (SM) - Código","Faixa Hora Contrat - Código",'
        '"Tipo Estabelecimento - Código","Tipo Estabelecimento - Nome",'
        '"Vl Rem Média Nom","Tempo Emprego","Idade",'
        '"CNAE 2.0 Subclasse - Codigo","Ind Vínculo Abandonado - Código",'
        '"Categoria Trabalhador - Código"\n'
        '150140,1,5,6,1,"CNPJ",2956.01,4.1,24," 8630503",1,101\n',
    )
    df = read_rais(path, year=2024, dataset="vinculos")
    row = df.row(0, named=True)
    assert row["id_municipio_6"] == "150140"
    assert row["vinculo_ativo_3112"] is True
    assert row["faixa_remuneracao_media_sm"] == "5"
    assert row["faixa_horas_contratadas"] == "6"
    assert row["tipo_estabelecimento_desc"] == "CNPJ"
    assert row["valor_remuneracao_media"] == 2956.01
    assert row["tempo_emprego"] == 4.1
    assert row["idade"] == 24
    assert row["cnae_2_subclasse"] == "8630503"
    assert row["indicador_vinculo_abandonado"] is True
    assert row["categoria_trabalhador"] == "101"


def test_read_rais_rejects_unknown_header(tmp_path):
    path = _write(tmp_path / "x.txt", "Município;Coluna Nova\n150140;1\n")
    with pytest.raises(ParseError, match="Coluna Nova"):
        read_rais(path, year=2024, dataset="vinculos")
