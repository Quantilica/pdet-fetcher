import datetime as dt

import pytest

from pdet_fetcher import fetch, plugin
from pdet_fetcher.storage import get_caged_2020_filename, get_caged_2020_filepath

_ROOT = "/pdet/microdados/NOVO CAGED"


def _entry(name: str, size: int | None, dir_path: str = _ROOT) -> dict:
    return {
        "id": name,
        "url": f"{dir_path}/{name}".replace("//", "/"),
        "datetime": dt.datetime(2026, 10, 1, 12, 0),
        "size": size,
        "name": name,
        "extension": "7z" if name.endswith(".7z") else "pdf",
        "full_path": f"{dir_path}/{name}".replace("//", "/"),
    }


def _ftp_listing() -> dict[str, list[dict]]:
    return {
        _ROOT: [
            _entry("2020", None),
            _entry("2025", None),
            _entry("leia-me.pdf", 100),
        ],
        f"{_ROOT}/2020": [_entry("202001", None, _ROOT + "/2020")],
        f"{_ROOT}/2020/202001": [
            _entry("CAGEDMOV202001.7z", 123, _ROOT + "/2020/202001"),
        ],
        f"{_ROOT}/2025": [_entry("202501", None, _ROOT + "/2025")],
        f"{_ROOT}/2025/202501": [
            _entry("CAGEDEXC202501.7z", 456, _ROOT + "/2025/202501"),
            _entry("CAGEDFOR202501.7z", 456, _ROOT + "/2025/202501"),
            _entry("CAGEDMOV202501.7z", 789, _ROOT + "/2025/202501"),
        ],
    }


@pytest.fixture
def ftp_listing(monkeypatch) -> dict[str, list[dict]]:
    listing = _ftp_listing()
    monkeypatch.setattr(fetch, "list_files", lambda directory: listing[directory])
    return listing


def test_list_caged_2020_yields_uppercase_files(ftp_listing):
    entries = list(fetch.list_caged_2020())
    assert len(entries) == 4

    y2025 = [e for e in entries if e["year"] == "2025"]
    assert len(y2025) == 3
    assert {e["dataset"] for e in y2025} == {
        "caged-2020-exc",
        "caged-2020-for",
        "caged-2020-mov",
    }
    assert all(e["month"] == "01" for e in y2025)
    assert all(e["name"].startswith("CAGED") for e in y2025)


def test_list_caged_2020_paths_match_remote(ftp_listing):
    entries = {e["name"]: e for e in fetch.list_caged_2020()}
    assert (
        entries["CAGEDMOV202501.7z"]["url"] == f"{_ROOT}/2025/202501/CAGEDMOV202501.7z"
    )
    assert (
        entries["CAGEDMOV202001.7z"]["url"] == f"{_ROOT}/2020/202001/CAGEDMOV202001.7z"
    )


def test_pdet_path_builder_caged_2020(ftp_listing, tmp_path):
    entry = next(
        e
        for e in fetch.list_caged_2020()
        if e["dataset"] == "caged-2020-mov" and e["year"] == "2025"
    )
    local = plugin.pdet_path_builder(tmp_path, entry, None)
    expected_name = get_caged_2020_filename(entry)
    assert local == tmp_path / "caged-2020-mov" / "2025" / expected_name
    assert local == get_caged_2020_filepath(entry, tmp_path)


def test_pdetcaged_2020_check_entry_download_not_present(ftp_listing, tmp_path):
    entry = next(e for e in fetch.list_caged_2020() if e["dataset"] == "caged-2020-mov")
    item = plugin._pdet_check_entry(entry, tmp_path)
    assert item.action == "download"
    assert item.reason == "not-present"
    assert "path-error" not in item.reason
    assert item.local_path.endswith(".7z")
