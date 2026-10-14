# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [Não lançado]

### Corrigido
- `plugin.py`: a dica de instalação do extra `analysis` mandava `pip install pdet-fetcher[analysis]`, que não funciona para fetchers do Fluxo B (fora do PyPI), e o Rich ainda engolia o `[analysis]` por tratá-lo como marcação, deixando só `pip install pdet-fetcher`. Agora a mensagem é `quantilica install pdet --extra analysis`.
- Listagem do Novo CAGED (`caged-2020-*`): `_list_variation_files` agora desce no
  diretório `<ano>/<anomês>/` (até 2 níveis de diretórios de data) para alcançar os
  arquivos `CAGEDEXC/CAGEDFOR/CAGEDMOV`; antes a listagem parava no diretório do ano
  e nenhum arquivo do Novo CAGED era coletado. `plugin.pdet_path_builder` também
  passa a rotear as entradas `caged-2020-{exc,for,mov}` para `get_caged_2020_filepath`
  (antes caíam no caminho de fallback).

### Alterado
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
