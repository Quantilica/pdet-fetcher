# Changelog

Todas as mudanças notáveis deste projeto serão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [0.8.0] - 2026-10-27

### Alterado
- **Incompatível:** `convert` deixa de gravar um `.parquet` por ano ao lado dos arquivos baixados e passa a escrever num lake de Parquet particionado no estilo Hive: `<lake>/pdet/<dataset>/<chave>=<valor>/<arquivo>.parquet`. Não há modo de layout antigo; reconverta os arquivos baixados no novo layout. As partições vêm do nome do arquivo (ou da pasta) e não são gravadas dentro do Parquet:
  - `rais-vinculos`: `ano` e `regiao` (`rais-vinculos_2025-sul` → `ano=2025/regiao=sul`; sem região, `regiao=__HIVE_DEFAULT_PARTITION__`);
  - `rais-estabelecimentos`: `ano`;
  - `caged`, `caged-ajustes`, `caged-2020-mov`, `caged-2020-for` e `caged-2020-exc`: `ano` e `mes` (`caged_201901` → `ano=2019/mes=01`; arquivos anuais, como `caged-ajustes_2002`, ficam em `mes=__HIVE_DEFAULT_PARTITION__`).
- **Incompatível:** `convert` e `pipeline` passam a usar os comandos padrão do ecossistema. `convert` recebe `-i/--input` (diretório dos arquivos baixados), `--lake` (raiz do lake, com `-o/--output` como sinônimo; padrão `QUANTILICA_LAKE_DIR`), `--dataset` (repetível) e `--force`. `pipeline` ganha `--lake` (`--parquet-dir` segue aceito). O relatório final mostra arquivos convertidos, pulados e com erro, e o comando sai com código 1 se houver erro. Sem o extra `analysis`, `convert` e `pipeline` saem com código 1 e a dica de instalação.
- As colunas do lake são gravadas como texto, com nomes normalizados. Nas republicações de 2026 da RAIS (Vínculos e Estabelecimentos), o sufixo `- Código` dos cabeçalhos é removido (`municipio_codigo` → `municipio`), para que os anos tenham os mesmos nomes.

### Adicionado
- `pdet_fetcher.lake` (requer o extra `analysis`): `converter.convert(input_dir, lake_dir, datasets=None, force=False)` devolve as contagens `converted` e `skipped` e a lista `errors`. A leitura é em streaming (DuckDB), com suporte a `.7z` e `.zip`.
- Idempotência por `sha256`: um arquivo já presente no lake com o mesmo `sha256` é pulado. Quando o PDET republica um arquivo (mesmo nome, outra data `@AAAAMMDD`), só a versão mais recente é convertida e ela substitui o Parquet anterior em vez de acumular arquivos.
- Arquivos sem as colunas obrigatórias (por exemplo, uma página HTML de erro salva como CSV) e arquivos compactados corrompidos são recusados e listados no relatório, sem interromper os demais.
- Ponto de entrada `quantilica.lake` (`pdet`), para ferramentas localizarem o conversor sem importar o pacote.
- Cada dataset do lake ganha um `_dataset.json` com o descritor (partições e tipos), e cada Parquet leva os metadados de proveniência (`quantilica.*`).
- O lake lê o Novo CAGED como UTF-8 (como `read_caged`) e os demais datasets como latin-1; a RAIS é lida com `;` e, se o cabeçalho não bater, com `,` (formato de 2026).
- Dependência `quantilica-core` atualizada para `>=0.13.0` e, no extra `analysis`, `quantilica-analytics` para `>=0.5.0`: são as primeiras versões com o contrato e o escritor do lake.

## [0.7.0] - 2026-10-21

### Alterado
- Novo CAGED (`caged-2020-mov`, `-for`, `-exc`): a coluna `seção` (seção da CNAE 2.0,
  uma letra) passa a se chamar `cnae_2_secao`; antes saía como `cnae_2`, nome que no
  CAGED e na RAIS é a classe da CNAE 2.0 (5 dígitos). Quem lia `cnae_2` do Novo CAGED
  precisa trocar o nome.
- Colunas indicadoras (`BOOLEAN_COLUMNS`): `"0"` → `False`, `"1"` → `True`, `"9"`
  (não identificado) → nulo; qualquer outro código levanta `ParseError`. Antes, todo
  código diferente de zero virava `True`, inclusive o `9` que o Novo CAGED publica em
  `indicador_trabalho_parcial` e `indicador_trabalho_intermitente`, o CAGED ajustes em
  `indicador_aprendiz` e `indicador_portador_deficiencia` e a RAIS Vínculos em
  `indicador_cei_vinculado` e `indicador_simples`.

### Corrigido
- Novo CAGED: `indicador_aprendiz` tinha um espaço no fim do nome
  (`"indicador_aprendiz "`), não era reconhecido como booleano e chegava como texto.
- Novo CAGED: `valor_salario_fixo` passa a numérico (decimal com vírgula, como
  `salario_mensal`); `competencia_declarada` e `competencia_exclusao` passam a inteiro
  (AAAAMM), como `competencia_movimentacao`. `competencia_declarada` do CAGED ajustes
  também passa a inteiro.

### Adicionado
- `read_caged` e `read_rais(dataset="estabelecimentos")` nomeiam as colunas pelo
  cabeçalho do arquivo, como `read_rais(dataset="vinculos")` desde a 0.6.0
  (`CAGED_HEADER_NAMES`, `CAGED_AJUSTES_HEADER_NAMES`, `NOVO_CAGED_HEADER_NAMES`,
  `RAIS_ESTABELECIMENTOS_HEADER_NAMES`, `RAIS_ESTABELECIMENTOS_1985_2001_HEADER_NAMES`;
  `resolve_columns`, `resolve_caged_columns`, `resolve_rais_estabelecimentos_columns`).
  Os mapas cobrem os 24 layouts publicados no FTP da PDET em 2026-10-21 (561 arquivos:
  CAGED 2007-2019, CAGED ajustes 2002-2019, Novo CAGED 2020-2026 e RAIS
  Estabelecimentos 1985-2025) e reproduzem os nomes posicionais anteriores em todos
  eles, salvo as duas mudanças do Novo CAGED acima (teste com um cabeçalho real de cada
  layout em `tests/data/pdet_headers.tsv`). Cabeçalho desconhecido ou nome canônico
  repetido levanta `ParseError`.
- RAIS Estabelecimentos: "UF" é a sigla (`sigla_uf`) nos arquivos de 1985 a 2001, de
  cabeçalhos em maiúsculas, e o código (`id_uf`) de 2002 em diante; o estilo do
  cabeçalho escolhe o mapa.
- `read_caged` detecta o separador como `read_rais` (`","` → ponto decimal).
- `normalize_header` remove o BOM.

As constantes posicionais (`CAGED_COLUMNS`, `CAGED_2020_*_COLUMNS`,
`RAIS_ESTABELECIMENTOS_COLUMNS` etc.) continuam exportadas, mas os leitores não as usam
mais.

## [0.6.1] - 2026-10-20

### Corrigido
- `reader.py` e `wrangling.py`: sem o extra `analysis`, importar esses módulos
  levantava `ModuleNotFoundError` cru de `polars` ou `quantilica.analytics`. Agora
  falham com `ImportError` acionável orientando o extra
  (`uv add 'pdet-fetcher[analysis]' --index https://index.quantilica.com/simple/`),
  no padrão do `anp-fetcher`.

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
