import datetime as dt
from pathlib import Path

from quantilica.cli.sdk import CheckPlan
from quantilica.core.manifests import manifest_sidecar_path
from typer.testing import CliRunner

from pdet_fetcher import plugin

ENTRY = {
    "id": "caged_mov_202401.csv",
    "url": "ftp://ftp.mtps.gov.br/pdet/mov/caged_mov_202401.csv",
    "datetime": dt.datetime(2024, 1, 5, 10, 30),
    "size": 1234,
    "name": "caged_mov_202401.csv",
    "extension": "csv",
    "dataset": "caged-2020",
    "group": "caged-2020",
    "year": "2024",
    "month": "01",
}


class _FailingClient:
    """Client que falha se qualquer método for chamado."""

    def __getattr__(self, name):
        raise AssertionError(f"nenhum acesso de rede esperado: {name}")


def _local_path(tmp_path: Path) -> Path:
    return plugin.pdet_path_builder(tmp_path, dict(ENTRY), None)


def test_check_entry_remote_metadata_from_list(tmp_path: Path):
    item = plugin._pdet_check_entry(dict(ENTRY), tmp_path)
    assert item.dataset == "caged-2020"
    assert item.id == "caged_mov_202401.csv"
    assert item.url == ENTRY["url"]
    assert item.remote_size == 1234
    assert item.remote_last_modified == "2024-01-05T10:30:00"
    assert item.remote_etag is None
    assert item.local_path == str(_local_path(tmp_path))


def test_check_entry_no_network_calls(tmp_path: Path):
    original = plugin.fetcher.client
    plugin.fetcher.client = _FailingClient()
    try:
        item = plugin.fetcher.check_entry(dict(ENTRY), tmp_path)
    finally:
        plugin.fetcher.client = original
    assert item.action == "download"
    assert item.reason == "not-present"
    assert item.entry["datetime"] == {
        "__datetime__": True,
        "value": "2024-01-05T10:30:00",
    }


def test_check_plan_round_trip(tmp_path: Path):
    item = plugin._pdet_check_entry(dict(ENTRY), tmp_path)
    plan = CheckPlan(
        fetcher="pdet-fetcher",
        output_dir=str(tmp_path),
        generated_at="2026-10-12T00:00:00+00:00",
        items=[item],
    )
    restored_plan = CheckPlan.from_json(plan.to_json())
    restored_entry = plugin._entry_from_plan_dict(restored_plan.items[0].entry)
    assert isinstance(restored_entry["datetime"], dt.datetime)
    restored_path = plugin.pdet_path_builder(tmp_path, restored_entry, None)
    assert restored_path == Path(restored_plan.items[0].local_path)


def test_check_entry_local_file_exists(tmp_path: Path):
    local = _local_path(tmp_path)
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(b"x")
    item = plugin._pdet_check_entry(dict(ENTRY), tmp_path)
    assert item.action == "skip-up-to-date"
    assert item.reason == "local-exists"
    assert item.local_exists


def test_check_entry_sidecar_exists(tmp_path: Path):
    local = _local_path(tmp_path)
    local.parent.mkdir(parents=True, exist_ok=True)
    manifest_sidecar_path(local).write_text("{}", encoding="utf-8")
    item = plugin._pdet_check_entry(dict(ENTRY), tmp_path)
    assert item.action == "skip-up-to-date"
    assert item.reason == "local-exists"
    assert item.local_exists
    assert not local.exists()


CAGED_ENTRY = {
    "id": "cagedest_012020.7z",
    "url": "/pdet/microdados/CAGED/2020/cagedest_012020.7z",
    "datetime": dt.datetime(2020, 2, 3, 9, 15),
    "size": 4321,
    "name": "cagedest_012020.7z",
    "extension": "7z",
    "dataset": "caged",
    "group": "caged",
    "year": "2020",
    "month": "01",
}

RAIS_ENTRY = {
    "id": "sp2022.7z",
    "url": "/pdet/microdados/RAIS/2022/sp2022.7z",
    "datetime": dt.datetime(2023, 6, 1, 14, 0),
    "size": 999,
    "name": "sp2022.7z",
    "extension": "7z",
    "dataset": "rais-vinculos",
    "group": "rais-vinculos",
    "year": "2022",
    "region": "sp",
}

CAGED_DOC = {
    "id": "leia-me.pdf",
    "url": "/pdet/microdados/CAGED/leia-me.pdf",
    "datetime": dt.datetime(2019, 5, 1, 8, 0),
    "size": 10,
    "name": "leia-me.pdf",
    "extension": "pdf",
    "dataset": "caged",
    "group": "caged",
    "year": None,
    "ext": "pdf",
    "is_doc": True,
}


def test_check_entry_path_error_does_not_raise(tmp_path: Path):
    bad = dict(CAGED_ENTRY, year=None)
    item = plugin._pdet_check_entry(bad, tmp_path)
    assert item.action == "download"
    assert item.reason.startswith("path-error: TypeError:")
    assert item.local_path == ""
    assert not item.local_exists
    assert item.entry["datetime"] == {
        "__datetime__": True,
        "value": "2020-02-03T09:15:00",
    }
    # O item continua serializável no plano.
    plan = CheckPlan(
        fetcher="pdet-fetcher",
        output_dir=str(tmp_path),
        generated_at="2026-10-12T00:00:00+00:00",
        items=[item],
    )
    assert CheckPlan.from_json(plan.to_json()).items[0].reason == item.reason


def test_doc_entries_use_docs_path_without_year(tmp_path: Path):
    path = plugin.pdet_path_builder(tmp_path, dict(CAGED_DOC), None)
    assert "_documentacao" in path.parts
    assert "caged" in path.parts
    item = plugin._pdet_check_entry(dict(CAGED_DOC), tmp_path)
    assert item.reason == "not-present"
    assert item.local_path == str(path)


def test_list_docs_marks_is_doc(monkeypatch):
    from pdet_fetcher import fetch

    def fake_list_files(directory: str) -> list[dict]:
        return [
            {
                "id": "layout.xls",
                "url": f"{directory}/layout.xls",
                "datetime": dt.datetime(2020, 1, 1),
                "size": 1,
                "name": "layout.xls",
                "extension": "xls",
            },
            {
                "id": "subdir",
                "url": f"{directory}/subdir",
                "datetime": dt.datetime(2020, 1, 1),
                "size": None,
                "name": "subdir",
                "extension": None,
            },
        ]

    monkeypatch.setattr(fetch, "list_files", fake_list_files)
    for lister in (
        fetch.list_caged_docs,
        fetch.list_caged_2020_docs,
        fetch.list_rais_docs,
    ):
        entries = list(lister())
        assert entries, lister.__name__
        assert all(e["is_doc"] and e["year"] is None for e in entries)
        assert all(e["name"] == "layout.xls" for e in entries)


def test_sync_from_plan_end_to_end(tmp_path: Path, monkeypatch):
    out = tmp_path / "out"
    items = [
        plugin._pdet_check_entry(dict(CAGED_ENTRY), out),
        plugin._pdet_check_entry(dict(RAIS_ENTRY), out),
    ]
    assert all(i.action == "download" for i in items)
    plan = CheckPlan(
        fetcher="pdet-fetcher",
        output_dir=str(out),
        generated_at="2026-10-12T00:00:00+00:00",
        items=items,
    )
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(plan.to_json(), encoding="utf-8")

    calls: list[tuple[str, Path, bool]] = []

    def fake_download(url, output, progress=None, *, force=False):
        calls.append((url, Path(output), force))
        return Path(output)

    monkeypatch.setattr(plugin.fetcher, "_download_file", fake_download)
    monkeypatch.setattr(plugin.fetcher, "_safe_head_date", lambda url: None)

    result = CliRunner().invoke(
        plugin.fetcher.app,
        ["sync", "--from-plan", str(plan_file), "--force", "--output", str(out)],
    )
    assert result.exit_code == 0, result.output
    assert sorted((u, str(o)) for u, o, _ in calls) == sorted(
        (i.url, i.local_path) for i in items
    )
    assert all(force is True for _, _, force in calls)
