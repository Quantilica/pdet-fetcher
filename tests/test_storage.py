"""Tests for pdet_fetcher.storage — pure functions, no I/O."""

from datetime import date, datetime

from pdet_fetcher.storage import (
    get_caged_2020_filename,
    get_caged_filename,
    get_rais_filename,
)

MOCK_DATETIME = datetime(2024, 3, 15)
TODAY = date.today()
TODAY_STAMP = TODAY.strftime("%Y%m%d")


def _caged_meta(**kwargs):
    return {
        "dataset": "caged",
        "datetime": MOCK_DATETIME,
        "extension": "parquet",
        **kwargs,
    }


def _caged_2020_meta(**kwargs):
    return {
        "dataset": "caged-2020-mov",
        "datetime": MOCK_DATETIME,
        "extension": "parquet",
        **kwargs,
    }


def _rais_meta(**kwargs):
    return {
        "dataset": "rais-vinculos",
        "datetime": MOCK_DATETIME,
        "extension": "parquet",
        **kwargs,
    }


class TestGetCagedFilename:
    def test_monthly_partition(self):
        meta = _caged_meta(year=2022, month=1)
        result = get_caged_filename(meta)
        assert result == "caged_202201@20240315.parquet"

    def test_yearly_partition_no_month(self):
        meta = _caged_meta(year=2022, month=None)
        result = get_caged_filename(meta)
        assert result == "caged_2022@20240315.parquet"

    def test_includes_modification_date(self):
        meta = _caged_meta(year=2020, month=6)
        result = get_caged_filename(meta)
        assert "20240315" in result


class TestGetCaged2020Filename:
    def test_basic(self):
        meta = _caged_2020_meta(year=2022, month=1)
        result = get_caged_2020_filename(meta)
        assert result == "caged-2020-mov_202201@20240315.parquet"

    def test_extension_reflected(self):
        meta = _caged_2020_meta(year=2022, month=1, extension="7z")
        result = get_caged_2020_filename(meta)
        assert result.endswith(".7z")


class TestGetRaisFilename:
    def test_without_uf(self):
        meta = _rais_meta(year=2022)
        result = get_rais_filename(meta)
        assert result == "rais-vinculos_2022@20240315.parquet"

    def test_with_uf(self):
        meta = _rais_meta(year=2022, uf="SP")
        result = get_rais_filename(meta)
        assert result == "rais-vinculos_2022-SP@20240315.parquet"

    def test_with_region(self):
        meta = _rais_meta(year=2022, region="CO")
        result = get_rais_filename(meta)
        assert result == "rais-vinculos_2022-CO@20240315.parquet"


class TestFallbackTimestamp:
    """Tests verifying fallback to today's date when datetime is None."""

    def _caged_meta_no_datetime(self, **kwargs):
        return {
            "dataset": "caged",
            "datetime": None,
            "extension": "parquet",
            **kwargs,
        }

    def _caged_2020_meta_no_datetime(self, **kwargs):
        return {
            "dataset": "caged-2020-mov",
            "datetime": None,
            "extension": "parquet",
            **kwargs,
        }

    def _rais_meta_no_datetime(self, **kwargs):
        return {
            "dataset": "rais-vinculos",
            "datetime": None,
            "extension": "parquet",
            **kwargs,
        }

    def test_path_for_entry_fallback_timestamp_caged(self):
        """When datetime=None, caged filename gets today's @YYYYMMDD timestamp."""
        meta = self._caged_meta_no_datetime(year=2022, month=1)
        result = get_caged_filename(meta)
        assert result == f"caged_202201@{TODAY_STAMP}.parquet"

    def test_path_for_entry_fallback_timestamp_caged_2020(self):
        """When datetime=None, caged-2020 filename gets today's @YYYYMMDD timestamp."""
        meta = self._caged_2020_meta_no_datetime(year=2022, month=1)
        result = get_caged_2020_filename(meta)
        assert result == f"caged-2020-mov_202201@{TODAY_STAMP}.parquet"

    def test_path_for_entry_fallback_timestamp_rais(self):
        """When datetime=None, rais filename gets today's @YYYYMMDD timestamp."""
        meta = self._rais_meta_no_datetime(year=2022)
        result = get_rais_filename(meta)
        assert result == f"rais-vinculos_2022@{TODAY_STAMP}.parquet"
