# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [0.6.0] - 2026-10-19

### Corrigido
- RAIS Vínculos: as republicações de 2026 no FTP da PDET (2023, 2024 e 2025) mudaram
  de formato: separador `,`, decimal com ponto, campos entre aspas, 62 colunas e outra
  ordem nas faixas das posições 14 a 16 (`Faixa Rem Média (SM)`, `Faixa Hora Contrat`,
  `Faixa Rem Dez (SM)`). Com o mapeamento por posição, `read_rais` falhava no polars 2
  (60 nomes para 62 colunas); num polars que aceitasse, a conversão numérica lia o
  ponto decimal como separador de milhar (2.956,01 virava 295.601) e as três faixas
  ficavam com os nomes trocados.
- `convert_columns_dtypes` ganha `decimal_separator` (`","` por padrão, o comportamento
  anterior; `"."` para o formato novo); `read_rais` escolhe pelo separador do arquivo.

### Adicionado
- `read_rais(dataset="vinculos")` nomeia as colunas pelo cabeçalho do arquivo
  (`RAIS_VINCULOS_HEADER_NAMES`, `normalize_header`, `resolve_rais_vinculos_columns`),
  e não mais pela posição do layout do ano. O mapa cobre os 17 layouts de 1985 a 2025
  e reproduz exatamente os nomes posicionais anteriores em todos os layouts antigos
  (teste com um cabeçalho real de cada layout em `tests/data/`). Cabeçalho
  desconhecido ou dois cabeçalhos com o mesmo nome canônico levantam `ParseError`.
- Colunas novas do formato de 2026: `indicador_vinculo_abandonado` (booleana) e
  `categoria_trabalhador`; incluídas em `sql/rais_vinculos.sql`.
- `write_parquet` grava com `quantilica.analytics.writer.to_parquet` e herda a
  proveniência `quantilica.*` do manifesto do arquivo bruto quando há um só.

### Alterado
- Pisos de dependência: `quantilica-core>=0.12.0` e, no extra `analysis`,
  `quantilica-analytics>=0.4.0`.
- RAIS Estabelecimentos continua nomeada por posição; o formato de 2026 desse dataset
  não foi conferido. Como em Vínculos, arquivo separado por `,` passa a ser lido com
  ponto decimal.

## [0.5.5] - 2026-10-14

### Corrigido
- `plugin.py`: a dica de instalação do extra `analysis` mandava `pip install pdet-fetcher[analysis]`, que não funciona para fetchers do Fluxo B (fora do PyPI), e o Rich ainda engolia o `[analysis]` por tratá-lo como marcação, deixando só `pip install pdet-fetcher`. Agora a mensagem é `quantilica install pdet --extra analysis`.
- Listagem do Novo CAGED (`caged-2020-*`): `_list_variation_files` agora desce no
  diretório `<ano>/<anomês>/` (até 2 níveis de diretórios de data) para alcançar os
  arquivos `CAGEDEXC/CAGEDFOR/CAGEDMOV`; antes a listagem parava no diretório do ano
  e nenhum arquivo do Novo CAGED era coletado. `plugin.pdet_path_builder` também
  passa a rotear as entradas `caged-2020-{exc,for,mov}` para `get_caged_2020_filepath`
  (antes caíam no caminho de fallback).

### Alterado
- `README.md`: a seção de instalação deixa de mandar `pip install` (fetchers do Fluxo B não estão no PyPI) e passa a mostrar `quantilica install pdet` (com `--extra analysis`) e `uv add --index` para uso como biblioteca.
- `cli.py` protege o import do plugin; sem o host a CLI standalone sai com código 1 e orienta `quantilica install pdet`.
- `reader.decompress` agora delega a descompressão de arquivos brutos para `quantilica.core.files.decompress_archive`, eliminando invocação direta de subprocesso `7z`.
- Remoção de instância estática global de `FtpClient` no escopo do módulo `fetch.py`.

## [0.5.4] - 2026-10-02

### Corrigido
- Refatoração de `cli.py` para o padrão canônico de thin wrapper sobre
  `plugin.py` (`from .plugin import app`), eliminando `ImportError` de
  `fetch_caged`/`fetch_rais` na importação do módulo.

## [0.5.3] - 2026-10-02

### Corrigido
- Remoção de `polars>=1.0.0` de `[project.dependencies]`, assegurando a adesão estrita ao princípio de Pure Fetcher.
- Manutenção de `polars>=1.39` exclusivamente em `[project.optional-dependencies].analysis`.
- Atualização do workflow `.github/workflows/test.yml` para instalar `--extra analysis` no `uv sync`.

## [0.5.2] - 2026-08-31
### Corrigido
- Quitação de dívida de lint (E501/docstrings longas) herdada dos sweeps de
  documentação de 2026-08-14; nenhum comportamento alterado.

## [0.5.1] - 2026-08-10
### Corrigido
- Atualizada dependência `quantilica-core` para `>=0.5.0` devido às novas assinaturas requeridas pelo CLI.
- Adicionada dependência implícita do `polars>=1.0.0` requerida pelo `reader.py`.

## [0.5.0] - 2026-08-10
### Alterado
- Migração completa da CLI para utilização da SDK unificada (`FetcherApp`).
- Remoção do gerenciamento manual de cliente FTP (`ftplib`) em favor do `FtpClient` provido pelo `quantilica-core`.

## [0.4.0] - 2026-08-07
### Alterado
- Refatoração arquitetural: Remoção de dependências (`quantilica-cli` e `quantilica-catalog`) e limpeza de imports. Os fetchers agora são pacotes de extração puros, dependendo estritamente do `quantilica-core`.

## [0.2.0] - 2026-05-19

Primeira entrada em formato Keep a Changelog; documenta o estado do pacote nesta
versão.

### Adicionado

- Coleta de microdados de RAIS e CAGED via FTP da PDET, com leitura inteligente
  (detecção de formato, codificação e estrutura) e correção de CSVs "ragged".
- Conversão de tipos (strings → números, booleanos e categóricas) e processamento
  com Polars.
- CLI standalone e plugin Typer para o `quantilica-cli` (`quantilica pdet`).
