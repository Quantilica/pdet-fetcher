---
dataset:
  id: "rais-estabelecimentos"
  fonte: "pdet"
  titulo: "RAIS - estabelecimentos (microdados)"
  resumo: "Microdados anuais da RAIS por estabelecimento, de 1985 em diante, com o estoque de vínculos e a atividade econômica."
  orgao: "Ministério do Trabalho e Emprego (MTE), Programa de Disseminação das Estatísticas do Trabalho (PDET)"
  pagina_oficial: "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/"
  licenca: "Não informada pela fonte. O Comunicado MTPS 192/12 e a Nota Técnica MTPS 088/12 (ftp://ftp.mtps.gov.br/pdet/microdados/) descrevem os microdados como não identificados e não declaram licença nem termo de uso"
  periodicidade: "anual (o ano-base é publicado no ano seguinte, com republicações)"
  cobertura: {inicio: "1985", fim: "2025"}
  unidade: "estabelecimento declarante da RAIS no ano-base"
  granularidade: {espacial: "município do estabelecimento", temporal: "ano"}
  formato_bruto: "7z (ou ZIP) com um TXT por ano, latin-1, separador ponto e vírgula; republicações de 2023 em diante em CSV com vírgula, aspas e extensão .COMT"
  lake: {estado: convertido, particoes: [ano], chave: [], colunas: 44}
  unidades_de_medida:
    estoque: "vínculos"
    est_clt_out: "vínculos"
    estoque_esta: "vínculos"
    qtd_vinculos_clt: "vínculos"
    qtd_vinculos_ativos: "vínculos"
    qtd_vinculos_estatutarios: "vínculos"
  dados_pessoais: "nenhum"
  eras:
  - {desde: "1995-01", mudanca: "entram classe CNAE/95, natureza jurídica e indicador de RAIS negativa"}
  - {desde: "1996-01", mudanca: "entram bairros de Fortaleza e do Rio e regiões administrativas do DF"}
  - {desde: "1999-01", mudanca: "entram indicadores de CEI vinculado, PAT e SIMPLES"}
  - {desde: "2001-01", mudanca: "o estoque se divide em CLT e outros e estatutários"}
  - {desde: "2002-01", mudanca: "os nomes das colunas passam a ser os longos (Qtd Vínculos…)"}
  - {desde: "2006-01", mudanca: "entra a CNAE 2.0 (classe e subclasse)"}
  - {desde: "2014-01", mudanca: "entram o CEP do estabelecimento e o subsetor IBGE"}
  - {desde: "2023-01", mudanca: "republicações em CSV com vírgula e extensão .COMT; o conversor as lê (ver Conteúdo)"}
  revisado_em: "2026-11-10"
  versao_fetcher: "0.8.3"
---

# RAIS - estabelecimentos (microdados)

## Resumo

Os microdados de estabelecimentos da RAIS trazem um registro por estabelecimento que declarou, com o município, a atividade econômica, o tamanho, a natureza jurídica e o estoque de vínculos ativos em 31/12. Servem para contar estabelecimentos e empregos por município e atividade.

Não há dados pessoais: a unidade é o estabelecimento (CNPJ ou CEI), mas a fonte não traz o número do estabelecimento. O CEP aparece a partir do layout de 2014.

## Ficha técnica

<!-- gerado:ficha -->

| Campo | Valor |
|---|---|
| Dataset | `pdet/rais-estabelecimentos` |
| Órgão | Ministério do Trabalho e Emprego (MTE), Programa de Disseminação das Estatísticas do Trabalho (PDET) |
| Página oficial | ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/ |
| Licença | Não informada pela fonte. O Comunicado MTPS 192/12 e a Nota Técnica MTPS 088/12 (ftp://ftp.mtps.gov.br/pdet/microdados/) descrevem os microdados como não identificados e não declaram licença nem termo de uso |
| Periodicidade | anual (o ano-base é publicado no ano seguinte, com republicações) |
| Cobertura | 1985 a 2025 |
| Unidade (uma linha) | estabelecimento declarante da RAIS no ano-base |
| Granularidade | espacial: município do estabelecimento; temporal: ano |
| Formato bruto | 7z (ou ZIP) com um TXT por ano, latin-1, separador ponto e vírgula; republicações de 2023 em diante em CSV com vírgula, aspas e extensão .COMT |
| Estado no lake | convertido |
| Partições | `ano` |
| Chave | — |
| Colunas | 44 |
| Unidades de medida | `estoque`: vínculos; `est_clt_out`: vínculos; `estoque_esta`: vínculos; `qtd_vinculos_clt`: vínculos; `qtd_vinculos_ativos`: vínculos; `qtd_vinculos_estatutarios`: vínculos |
| Dados pessoais | nenhum |
| Eras | 1995-01: entram classe CNAE/95, natureza jurídica e indicador de RAIS negativa; 1996-01: entram bairros de Fortaleza e do Rio e regiões administrativas do DF; 1999-01: entram indicadores de CEI vinculado, PAT e SIMPLES; 2001-01: o estoque se divide em CLT e outros e estatutários; 2002-01: os nomes das colunas passam a ser os longos (Qtd Vínculos…); 2006-01: entra a CNAE 2.0 (classe e subclasse); 2014-01: entram o CEP do estabelecimento e o subsetor IBGE; 2023-01: republicações em CSV com vírgula e extensão .COMT; o conversor as lê (ver Conteúdo) |
| Revisado em | 2026-11-10 |
| Versão do fetcher | 0.8.3 |

## Origem e coleta

- O fetcher baixa os arquivos do FTP do PDET (`ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/`), um por ano, e guarda em `rais-estabelecimentos/<ano>/rais-estabelecimentos_<ano>@AAAAMMDD.7z` (alguns anos vêm em `.zip`). O lake converte a versão mais recente de cada ano.
- Conferido no bruto em 2026-11-10: 41 arquivos, de 1985 a 2025, uma versão cada.
- Os layouts por período (`RAIS_estabelecimento_layout*.xls`) estão na pasta de documentação do FTP.

## Conteúdo

- Uma linha por estabelecimento e ano. Nos anos amostrados o número de linhas cresce de 1,09 milhão (1985) a 8,3 milhões (2015); a republicação de 2025 tem 13,48 milhões de linhas.
- Cobertura nominal (listagem do bruto, 2026-11-10): ano-base de 1985 a 2025, sem lacunas.
- Amostra convertida (não é o conjunto inteiro): 16 anos, mais 2023, 2024 e 2025 (abaixo): 1985, 1986, 1989, 1991, 1993, 1994, 1995, 1996, 1997, 1998, 1999, 2001, 2002, 2005, 2006, 2015. As colunas e linhas abaixo valem para esses anos.
- Conferência de linhas: em todos os anos convertidos o número de linhas do lake é o do TXT menos o cabeçalho (por exemplo 2015: 8.314.306).
- **2023 a 2025: convertidos e conferidos.** Os três arquivos do período têm o dado em `RAIS_ESTAB_PUB.COMT`. O conversor os lê (a partir do `quantilica-analytics` 0.11.2; antes recusava o arquivo com "contains no data files"). Em cada um o número de linhas do Parquet é igual ao do arquivo menos o cabeçalho: 2023, 11.768.420; 2024, 13.186.059; 2025, 13.481.949. Os três têm as mesmas 23 colunas, listadas abaixo.

## Variáveis

Os nomes vêm do cabeçalho de cada arquivo, normalizado. Os layouts mudam muito entre períodos e o mesmo conceito aparece com nomes diferentes (`estoque`, `qtd_vinculos_ativos`); a coluna Período mostra os anos amostrados em que cada nome aparece. Os rótulos vêm dos layouts oficiais. Todas as colunas são texto.

| Coluna no lake | Na fonte | Tipo | Descrição | Unidade / códigos | Período |
|---|---|---|---|---|---|
| `bairr_sp` | BAIRRO SP | texto | Bairro do município de São Paulo | código de 4 posições | 1985 a 1991, 1994 a 1996, 1998 a 2001 (anos amostrados) |
| `estoque` | ESTOQUE | texto | Estoque de vínculos ativos em 31/12 | vínculos | 1985 a 2001 (anos amostrados) |
| `municipio` | MUNICIPIO | texto | Município de localização do estabelecimento | código de 6 dígitos (IBGE sem dígito verificador) | todo |
| `tamestab` | TAMESTAB | texto | Tamanho do estabelecimento pelos empregados ativos em 31/12 (10 categorias) | 1 zero, 2 até 4, 3 de 5 a 9, … 10 mil ou mais | 1985 a 2001 (anos amostrados) |
| `tipo_ident` | TIPO IDENT | texto | Tipo de estabelecimento (identificação) | CNPJ ou CEI (códigos no layout) | 1985 a 1993 (anos amostrados) |
| `uf` | UF | texto | Unidade da Federação do estabelecimento | código IBGE de 2 dígitos | 1985 a 2001, 2015 (anos amostrados) |
| `subs_ibge` | IBGE Subsetor | texto | Subsetor IBGE 80 do estabelecimento | 1 a 25 | 1985 a 2001 (anos amostrados) |
| `subativ_ibge` | SUBATIV IBGE | texto | Subatividade econômica segundo o IBGE (285 categorias) | código de 4 posições | 1985 a 1994 (anos amostrados) |
| `distrito_sp` | DISTRITO_SP | texto | Distrito do município de São Paulo | código (planilha do layout) | 1986, 1991, 1994 a 1996, 1998 a 2001 (anos amostrados) |
| `clas_cnae_95` | CLAS CNAE 95 | texto | Classe de atividade econômica pela CNAE/95 | 5 dígitos | 1994 a 2001 (anos amostrados) |
| `nat_estb` | NAT ESTB | texto | Natureza do estabelecimento (layout de 1994; o layout oficial lido não a descreve) | — | 1994 (anos amostrados) |
| `tipo_estb` | TIPO ESTB | texto | Tipo do estabelecimento (layout de 1994; o layout lido não o descreve) | — | 1994 (anos amostrados) |
| `ind_rais_neg` | IND RAIS NEG | texto | Indicador de RAIS negativa (estabelecimento sem vínculos) | 0 não, 1 sim | 1995 a 2001 (anos amostrados) |
| `nat_jurid` | NAT JURIDICA | texto | Natureza jurídica do estabelecimento | 4 dígitos | 1995 a 2001 (anos amostrados) |
| `tipo_estbl` | TIPO ESTBL | texto | Tipo de estabelecimento | 1 CNPJ, 3 CEI, 9 não identificado, -1 ignorado | 1995 a 2001 (anos amostrados) |
| `bairro_fort` | BAIRRO FORT | texto | Bairro do município de Fortaleza | código | 1996 a 2001 (anos amostrados) |
| `bairro_rj` | BAIRRO_RJ | texto | Bairro do município do Rio de Janeiro | código | 1996 a 2001 (anos amostrados) |
| `reg_adm_df` | REG ADM DF | texto | Região Administrativa do Distrito Federal | código | 1996 a 2001 (anos amostrados) |
| `ind_cei_vinc` | IND CEI VINC | texto | Indicador de CEI vinculado | 0 não, 1 sim | 1999 a 2001 (anos amostrados) |
| `ind_pat` | IND PAT | texto | Indicador de estabelecimento no Programa de Alimentação do Trabalhador | 0 não, 1 sim | 1999 a 2001 (anos amostrados) |
| `ind_simples` | IND SIMPLES | texto | Indicador de optante pelo SIMPLES | 0 não, 1 sim | 1999 a 2015 (anos amostrados) |
| `est_clt_out` | EST CLT OUT | texto | Estoque de vínculos CLT e outros regimes ativos em 31/12 | vínculos | 2001 (anos amostrados) |
| `estoque_esta` | ESTOQUE ESTA | texto | Estoque de vínculos estatutários ativos em 31/12 | vínculos | 2001 (anos amostrados) |
| `bairros_sp` | Bairros SP | texto | Bairro do município de São Paulo | código | 2002 a 2015 (anos amostrados) |
| `bairros_fortaleza` | Bairros Fortaleza | texto | Bairro de Fortaleza | código | 2002 a 2015 (anos amostrados) |
| `bairros_rj` | Bairros RJ | texto | Bairro do Rio de Janeiro | código | 2002 a 2015 (anos amostrados) |
| `cnae_95_classe` | CNAE 95 Classe | texto | Classe de atividade econômica pela CNAE/95 | 5 dígitos | 2002 a 2015 (anos amostrados) |
| `distritos_sp` | Distritos SP | texto | Distrito de São Paulo | código | 2002 a 2015 (anos amostrados) |
| `qtd_vinculos_clt` | Qtd Vínculos CLT | texto | Estoque de vínculos CLT e outros ativos em 31/12 | vínculos | 2002 a 2015 (anos amostrados) |
| `qtd_vinculos_ativos` | Qtd Vínculos Ativos | texto | Estoque de vínculos ativos em 31/12 | vínculos | 2002 a 2015 (anos amostrados) |
| `qtd_vinculos_estatutarios` | Qtd Vínculos Estatutários | texto | Estoque de vínculos estatutários ativos em 31/12 | vínculos | 2002 a 2015 (anos amostrados) |
| `ind_cei_vinculado` | Ind CEI Vinculado | texto | Indicador de CEI vinculado | 0 não, 1 sim | 2002 a 2015 (anos amostrados) |
| `ind_estab_participa_pat` | Ind Estab Participa PAT | texto | Indicador de estabelecimento participante do PAT | 0 não, 1 sim | 2002 a 2015 (anos amostrados) |
| `ind_rais_negativa` | Ind Rais Negativa | texto | Indicador de RAIS negativa | 0 não, 1 sim | 2002 a 2015 (anos amostrados) |
| `natureza_juridica` | Natureza Jurídica | texto | Natureza jurídica (CONCLA/2002) | 4 dígitos | 2002 a 2015 (anos amostrados) |
| `regioes_adm_df` | Regiões Adm DF | texto | Região Administrativa do DF | código | 2002 a 2015 (anos amostrados) |
| `tamanho_estabelecimento` | Tamanho Estabelecimento | texto | Tamanho do estabelecimento pelos empregados ativos em 31/12 | 1 a 10 | 2002 a 2015 (anos amostrados) |
| `tipo_estab` | Tipo Estab (código) | texto | Tipo de estabelecimento: código | 1 CNPJ, 3 CEI, … | 2002 a 2015 (anos amostrados) |
| `tipo_estab_1` | Tipo Estab (nome) | texto | Tipo de estabelecimento: nome (segunda coluna de mesmo título) | texto | 2002 a 2015 (anos amostrados) |
| `ind_atividade_ano` | Ind Atividade Ano | texto | Indicador de estabelecimento que exerceu atividade no ano | 0 não, 1 sim | 2005 a 2015 (anos amostrados) |
| `cnae_2_0_classe` | CNAE 2.0 Classe | texto | Classe de atividade econômica pela CNAE 2.0 | 5 dígitos | 2006 a 2015 (anos amostrados) |
| `cnae_2_0_subclasse` | CNAE 2.0 Subclasse | texto | Subclasse de atividade econômica pela CNAE 2.0 | 7 dígitos | 2006 a 2015 (anos amostrados) |
| `ibge_subsetor` | IBGE Subsetor | texto | Subsetor IBGE 80 do estabelecimento | 1 a 25 | 2015 (anos amostrados) |
| `cep_estab` | CEP Estb | texto | CEP declarado pelo estabelecimento | 8 dígitos | 2015 (anos amostrados) |

## No lake

- Dataset `pdet/rais-estabelecimentos`, particionado por `ano`: `ano=2015/rais-estabelecimentos_2015.parquet`; um Parquet por ano. O ano vem do nome do arquivo.
- O conversor extrai o TXT (7z ou ZIP), lê com DuckDB (latin-1, `;`) e grava tudo como texto. Se o cabeçalho não tem `municipio`, recusa o arquivo.
- Cabeçalhos com ` - Código` (2023 em diante) perdem o sufixo apenas para nomes conhecidos.
- O cabeçalho de 2002 em diante tem dois títulos `Tipo Estab`; o segundo vira `tipo_estab_1`.
- Chave: não há. Nulos: nenhum nas colunas da amostra.
- Colunas das republicações de 2023 em diante (as mesmas 23 em 2023, 2024 e 2025): `bairros_sp`, `bairros_fortaleza`, `bairros_rj`, `cnae_2_0_classe`, `cnae_95_classe`, `distritos_sp`, `qtd_vinculos_clt`, `qtd_vinculos_ativos`, `qtd_vinculos_estatutarios`, `ind_atividade_ano`, `ind_cei_vinculado`, `ind_estab_participante_pat` (antes `ind_estab_participa_pat`), `ind_rais_negativa`, `ind_estab_participante_simples` (antes `ind_simples`), `municipio`, `natureza_juridica`, `regiao_adm_df` (antes `regioes_adm_df`), `cnae_2_0_subclasse`, `tamanho_estabelecimento`, `tipo_estabelecimento` (antes `tipo_estab`), `uf`, `ibge_subsetor` e `cep_estab`.

## Cuidados de uso

- **Lacuna na amostra convertida.** Dos 41 anos do bruto, 19 foram convertidos e conferidos (os 16 da amostra mais 2023, 2024 e 2025); os outros 22 anos não foram convertidos nesta conferência.
- **Tudo é texto, sem normalização de formato.** O conversor não converte tipos nem tira zeros à esquerda ou espaços: o mesmo código aparece como `1`, `01` ou `000001`, e valores decimais usam vírgula (`0000686,80`). Converta e normalize antes de comparar anos.
- **Nomes diferentes para o mesmo conceito.** O estoque de vínculos é `estoque` (1985 a 2001), `est_clt_out` e `estoque_esta` (2001) e `qtd_vinculos_*` (2002 em diante); `tamestab` e `tamanho_estabelecimento`; `nat_jurid` e `natureza_juridica`.
- **Município vs UF.** `uf` falta nos anos 2002, 2005 e 2006 da amostra: derive a UF dos dois primeiros dígitos de `municipio`.
- **Estoque ≠ vínculos totais.** O estoque conta vínculos ativos em 31/12. Para vínculos do ano, use `rais-vinculos`.
- **Estabelecimentos de RAIS negativa.** `ind_rais_negativa` marca estabelecimentos que declararam não ter vínculos.
- **Cautela com variáveis recentes ou muito desagregadas.** A Nota Técnica MTPS 088/12 avisa que Indicador de Pessoa com Deficiência, Tipo de Deficiência, Subclasse de atividade econômica, Ocupação e Raça/Cor devem ser usadas com cautela: algumas são recentes e, no nível de desagregação fino, podem ter inconsistências; em nível agregado o uso não fica comprometido.

## Exemplos

Estoque de vínculos ativos por município em 2015 (a coluna é texto):

```python
import duckdb

duckdb.sql("""
    SELECT municipio, sum(CAST(trim(qtd_vinculos_ativos) AS BIGINT)) AS vinculos
    FROM read_parquet('lake/pdet/rais-estabelecimentos/ano=2015/*.parquet')
    GROUP BY municipio ORDER BY vinculos DESC LIMIT 10
""").show()
```

## Referências

- Microdados RAIS e CAGED do PDET (FTP): ftp://ftp.mtps.gov.br/pdet/microdados/
- Comunicado MTPS 192/12 e Nota Técnica MTPS 088/12, na mesma pasta.
- Layouts da RAIS estabelecimento (`RAIS_estabelecimento_layout*.xls`), na pasta de documentação do FTP.
