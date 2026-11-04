"""Tipos de partição dos datasets PDET seguem o contrato canônico do core."""

from __future__ import annotations

import pytest

pytest.importorskip("quantilica.analytics.lake")
pytest.importorskip("duckdb")

from pdet_fetcher.lake import SPECS  # noqa: E402


@pytest.mark.parametrize("spec", list(SPECS.values()), ids=list(SPECS))
def test_tipos_iguais_aos_da_regra_anterior(spec) -> None:
    """Regressão: ``ano`` é inteiro e as demais chaves são texto, como antes."""
    antigo = {k: "int" if k == "ano" else "varchar" for k in spec.partition_keys}
    assert spec.partition_types == antigo
