"""Tests for pdet_fetcher analysis-extra import guard."""

import importlib
import sys

import pytest

import pdet_fetcher

MODULES = [
    "pdet_fetcher.reader",
    "pdet_fetcher.wrangling",
]


def test_analysis_extra_mensagem_acionavel():
    # Os módulos originais são restaurados (e não reimportados) ao final: outros
    # testes guardam funções importadas deles e fazem patch por caminho.
    salvos = {name: importlib.import_module(name) for name in MODULES}
    salvo_polars = sys.modules.get("polars")
    try:
        for module_name in MODULES:
            sys.modules["polars"] = None
            sys.modules.pop(module_name, None)
            try:
                with pytest.raises(ImportError, match="requer o extra 'analysis'"):
                    importlib.import_module(module_name)
            finally:
                sys.modules.pop(module_name, None)
    finally:
        if salvo_polars is not None:
            sys.modules["polars"] = salvo_polars
        else:
            sys.modules.pop("polars", None)
        for module_name, module in salvos.items():
            sys.modules[module_name] = module
            setattr(pdet_fetcher, module_name.rpartition(".")[2], module)
