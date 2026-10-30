from pdet_fetcher.fetch import _get_date_dirs


def test_get_date_dirs_single_pattern():
    fi = [
        {"name": "2020", "size": None},
        {"name": "file.txt", "size": 100},
    ]
    dir_pattern = r"^(\d{4})$"
    dir_pattern_groups = ("year",)

    results = _get_date_dirs(fi, dir_pattern, dir_pattern_groups)

    assert len(results) == 1
    assert results[0] == {"dir": "2020", "year": "2020"}


def test_get_date_dirs_multiple_patterns():
    fi = [
        {"name": "2020", "size": None},
        {"name": "202001", "size": None},
        {"name": "invalid", "size": None},
        {"name": "file.txt", "size": 100},
    ]
    dir_pattern = (
        r"^(\d{4})$",
        r"^(\d{4})(\d{2})$",
    )
    dir_pattern_groups = (("year",), ("year", "month"))

    results = _get_date_dirs(fi, dir_pattern, dir_pattern_groups)

    assert len(results) == 2
    assert results[0] == {"dir": "2020", "year": "2020"}
    assert results[1] == {"dir": "202001", "year": "2020", "month": "01"}


def test_get_date_dirs_no_match():
    fi = [
        {"name": "abc", "size": None},
    ]
    dir_pattern = r"^(\d{4})$"
    dir_pattern_groups = ("year",)

    results = _get_date_dirs(fi, dir_pattern, dir_pattern_groups)

    assert len(results) == 0


def test_rais_vinculos_pattern_includes_ni(monkeypatch):
    """Vínculos sem UF identificada (``RAIS_VINC_PUB_NI.7z``) não podem ser
    perdidos em silêncio: o arquivo existe de 2022 a 2025 no FTP.
    """
    from pdet_fetcher import fetch

    listing = [
        {"name": f"RAIS_VINC_PUB_{region}.7z", "year": "2025"}
        for region in (
            "CENTRO_OESTE",
            "MG_ES_RJ",
            "NI",
            "NORDESTE",
            "NORTE",
            "SP",
            "SUL",
        )
    ] + [
        {"name": "RAIS_ESTAB_PUB.7z", "year": "2025"},
        {"name": "Leia-me.txt", "year": "2025"},
    ]
    monkeypatch.setattr(fetch, "_list_variation_files", lambda variation: iter(listing))

    files = list(fetch._list_dataset_files("rais-vinculos"))

    regions = sorted(f["region"] for f in files)
    assert regions == ["centrooeste", "mgesrj", "ni", "nordeste", "norte", "sp", "sul"]
