"""A extração de ZIP/7z não deixa pasta temporária (``pdet*``) para trás."""

from __future__ import annotations

import shutil
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from quantilica.core.exceptions import StorageError

from pdet_fetcher import reader, wrangling

CSV = "Col A;Col B\n1;2\n"


@pytest.fixture
def tmpdir_isolado(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Aponta o temp do processo para uma pasta só deste teste."""
    pasta = tmp_path / "tmpdir"
    pasta.mkdir()
    monkeypatch.setenv("TMPDIR", str(pasta))
    monkeypatch.setattr(tempfile, "tempdir", None)
    return pasta


def _sobras(pasta: Path) -> list[str]:
    return sorted(p.name for p in pasta.iterdir())


def _zip(destino: Path, membro: str = "pasta/dados.csv") -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destino, "w") as zf:
        zf.writestr(membro, CSV)
    return destino


class TestExtractedData:
    def test_remove_a_pasta_em_sucesso(self, tmp_path, tmpdir_isolado):
        raw = _zip(tmp_path / "in" / "caged_202001@20200201.zip")
        with reader.extracted_data({"filepath": raw, "name": "x"}) as meta:
            assert meta["name"] == "x"
            assert meta["decompressed_filepath"].read_text() == CSV
            assert _sobras(tmpdir_isolado)
        assert _sobras(tmpdir_isolado) == []

    def test_remove_a_pasta_quando_o_corpo_lanca(self, tmp_path, tmpdir_isolado):
        raw = _zip(tmp_path / "in" / "caged_202001@20200201.zip")
        with pytest.raises(ValueError, match="boom"):
            with reader.extracted_data({"filepath": raw}):
                raise ValueError("boom")
        assert _sobras(tmpdir_isolado) == []

    def test_falha_de_extracao_vira_runtime_error(self, tmp_path, tmpdir_isolado):
        raw = tmp_path / "in" / "ruim_202001@20200201.zip"
        raw.parent.mkdir()
        raw.write_bytes(b"isto nao e um zip")
        with pytest.raises(RuntimeError, match="7z failed"):
            with reader.extracted_data({"filepath": raw}):
                pass
        assert _sobras(tmpdir_isolado) == []


class TestDecompress:
    def test_remove_o_tmp_dir_quando_a_extracao_falha(self, tmp_path, tmpdir_isolado):
        raw = _zip(tmp_path / "in" / "caged_202001@20200201.zip")
        with patch(
            "pdet_fetcher.reader.decompress_archive",
            side_effect=StorageError("falhou"),
        ):
            with pytest.raises(RuntimeError, match="7z failed"):
                reader.decompress({"filepath": raw})
        assert _sobras(tmpdir_isolado) == []

    def test_remove_o_tmp_dir_em_interrupcao(self, tmp_path, tmpdir_isolado):
        raw = _zip(tmp_path / "in" / "caged_202001@20200201.zip")
        with patch(
            "pdet_fetcher.reader.decompress_archive",
            side_effect=KeyboardInterrupt,
        ):
            with pytest.raises(KeyboardInterrupt):
                reader.decompress({"filepath": raw})
        assert _sobras(tmpdir_isolado) == []

    def test_em_sucesso_a_pasta_fica_com_o_chamador(self, tmp_path, tmpdir_isolado):
        raw = _zip(tmp_path / "in" / "caged_202001@20200201.zip")
        out = reader.decompress({"filepath": raw})
        try:
            assert out["tmp_dir"].parent == tmpdir_isolado
            assert out["decompressed_filepath"].read_text() == CSV
        finally:
            shutil.rmtree(out["tmp_dir"])


class TestConvertRais:
    def _preparar(self, tmp_path: Path) -> tuple[Path, Path]:
        data_dir = tmp_path / "raw"
        (data_dir / "rais-estabelecimentos").mkdir(parents=True)
        _zip(data_dir / "rais-vinculos" / "2020" / "rais-vinculos_2020-ac@20210101.zip")
        return data_dir, tmp_path / "out"

    def test_leitura_que_lanca_nao_deixa_residuo(
        self, tmp_path, tmpdir_isolado, monkeypatch
    ):
        data_dir, dest_dir = self._preparar(tmp_path)

        def boom(*args, **kwargs):
            raise ValueError("leitura falhou")

        monkeypatch.setattr(reader, "read_rais", boom)
        with pytest.raises(ValueError, match="leitura falhou"):
            wrangling.convert_rais(data_dir, dest_dir)
        assert _sobras(tmpdir_isolado) == []

    def test_sucesso_nao_deixa_residuo_e_concatena(
        self, tmp_path, tmpdir_isolado, monkeypatch
    ):
        data_dir, dest_dir = self._preparar(tmp_path)
        escritos = []
        monkeypatch.setattr(
            reader, "read_rais", lambda *a, **k: pl.DataFrame({"a": [1, 2]})
        )
        monkeypatch.setattr(
            reader,
            "write_parquet",
            lambda df, dest, source_path=None: escritos.append((df, dest)),
        )
        wrangling.convert_rais(data_dir, dest_dir)
        assert len(escritos) == 1
        assert escritos[0][0].to_dict(as_series=False) == {"a": [1, 2]}
        assert _sobras(tmpdir_isolado) == []


class TestConvertCaged:
    def _preparar(self, tmp_path: Path) -> tuple[Path, Path]:
        data_dir = tmp_path / "raw"
        _zip(data_dir / "caged" / "caged_202001@20200201.zip")
        return data_dir, tmp_path / "out"

    def test_erro_de_calculo_e_logado_e_nao_deixa_residuo(
        self, tmp_path, tmpdir_isolado, monkeypatch, caplog
    ):
        data_dir, dest_dir = self._preparar(tmp_path)

        def boom(*args, **kwargs):
            raise pl.exceptions.ComputeError("coluna ruim")

        monkeypatch.setattr(reader, "read_caged", boom)
        wrangling.convert_caged(data_dir, dest_dir)
        assert "Error converting" in caplog.text
        assert _sobras(tmpdir_isolado) == []

    def test_outro_erro_sobe_sem_residuo(self, tmp_path, tmpdir_isolado, monkeypatch):
        data_dir, dest_dir = self._preparar(tmp_path)

        def boom(*args, **kwargs):
            raise OSError("disco")

        monkeypatch.setattr(reader, "read_caged", boom)
        with pytest.raises(OSError, match="disco"):
            wrangling.convert_caged(data_dir, dest_dir)
        assert _sobras(tmpdir_isolado) == []


class TestExtractColumns:
    def _preparar(self, tmp_path: Path) -> Path:
        data_dir = tmp_path / "raw"
        _zip(data_dir / "caged" / "caged_202001@20200201.zip")
        return data_dir

    def test_leitura_que_lanca_nao_deixa_residuo(
        self, tmp_path, tmpdir_isolado, monkeypatch
    ):
        data_dir = self._preparar(tmp_path)

        def boom(*args, **kwargs):
            raise pl.exceptions.ComputeError("leitura falhou")

        monkeypatch.setattr(pl, "read_csv", boom)
        with pytest.raises(pl.exceptions.ComputeError):
            wrangling.extract_columns_for_dataset(
                data_dir, "*.zip", tmp_path / "cols.csv"
            )
        assert _sobras(tmpdir_isolado) == []

    def test_sucesso_extrai_colunas_sem_residuo(self, tmp_path, tmpdir_isolado):
        data_dir = self._preparar(tmp_path)
        saida = tmp_path / "cols.csv"
        wrangling.extract_columns_for_dataset(data_dir, "*.zip", saida)
        linhas = saida.read_text(encoding="utf-8").splitlines()
        assert linhas[0] == "column,order,name,date"
        assert linhas[1:] == [
            "Col A,0,caged_202001@20200201,202001",
            "Col B,1,caged_202001@20200201,202001",
        ]
        assert _sobras(tmpdir_isolado) == []
