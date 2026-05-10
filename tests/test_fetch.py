
import pytest
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
