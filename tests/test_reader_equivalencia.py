"""``read_rais`` e ``read_caged`` leem pelo ``read_csv_resilient`` sem mudar a saída.

``data/golden_reader.json`` foi gerado com o ``pl.read_csv`` direto que o leitor
usava (esquema e linhas de cada DataFrame, numa amostra dos formatos publicados).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("quantilica.analytics.reader")

from pdet_fetcher.reader import read_caged, read_rais  # noqa: E402

GOLDEN = Path(__file__).parent / "data" / "golden_reader.json"

_CASES = (
    # (nome, leitor, dataset, codificação, texto)
    (
        "rais-vinculos-legado",
        "rais",
        "vinculos",
        "latin-1",
        "Município;Tempo Emprego;Sexo Trabalhador;Vl Remun Dezembro Nom\n"
        "355030;12,5;1;1.234,56\n330455;3,0;2;{ñ class}\n",
    ),
    (
        "rais-vinculos-2026",
        "rais",
        "vinculos",
        "latin-1",
        '"Município - Código","Tempo Emprego","Sexo Trabalhador"\n'
        '"355030","12.5","1"\n"330455","3.0","2"\n',
    ),
    (
        "rais-estabelecimentos",
        "rais",
        "estabelecimentos",
        "latin-1",
        "Município;Qtd Vínculos Ativos\n355030;10\n330455;7\n",
    ),
    (
        "caged",
        "caged",
        "caged",
        "latin-1",
        "Município;Sexo;Salário Mensal\n355030;1;1.500,00\n330455;3;2.000,00\n",
    ),
    (
        "novo-caged",
        "caged",
        "caged-2020-mov",
        "utf-8",
        "competênciamov;município;sexo;salário\n"
        "202001;355030;1;1500,00\n202001;330455;3;2000,00\n",
    ),
)


def snapshot(tmp: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for name, kind, dataset, encoding, text in _CASES:
        path = tmp / f"{name}.csv"
        path.write_bytes(text.encode(encoding))
        df = (
            read_rais(path, 2025, dataset)
            if kind == "rais"
            else read_caged(path, 202001, dataset)
        )
        out[name] = {
            "schema": [[c, str(t)] for c, t in df.schema.items()],
            "rows": [
                json.dumps(r, ensure_ascii=False, default=str) for r in df.to_dicts()
            ],
        }
    return out


def test_reader_output_matches_golden(tmp_path: Path) -> None:
    assert snapshot(tmp_path) == json.loads(GOLDEN.read_text(encoding="utf-8"))


if __name__ == "__main__":  # pragma: no cover - gera o golden com o leitor atual
    import sys
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        GOLDEN.write_text(
            json.dumps(snapshot(Path(tmp)), ensure_ascii=False, indent=1) + "\n",
            encoding="utf-8",
        )
    sys.stdout.write(f"golden gravado em {GOLDEN}\n")
