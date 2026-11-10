---
dataset:
  id: "caged"
  fonte: "pdet"
  titulo: "CAGED estatístico (microdados, 2007 a 2019)"
  resumo: "Microdados mensais do CAGED (admissões e desligamentos celetistas) de janeiro de 2007 a dezembro de 2019, não identificados."
  orgao: "Ministério do Trabalho e Emprego (MTE), Programa de Disseminação das Estatísticas do Trabalho (PDET)"
  pagina_oficial: "ftp://ftp.mtps.gov.br/pdet/microdados/CAGED/"
  licenca: "Não informada pela fonte. O Comunicado MTPS 192/12 e a Nota Técnica MTPS 088/12 (ftp://ftp.mtps.gov.br/pdet/microdados/) descrevem os microdados como não identificados e não declaram licença nem termo de uso"
  periodicidade: "mensal; série encerrada em 2019-12, substituída pelo Novo CAGED em 2020"
  cobertura: {inicio: "2007-01", fim: "2019-12"}
  unidade: "movimentação (admissão ou desligamento) de um trabalhador celetista em um mês"
  granularidade: {espacial: "município do estabelecimento", temporal: "mês"}
  formato_bruto: "7z com um TXT por mês (CAGEDEST_MMAAAA.txt), latin-1, separador ponto e vírgula"
  lake: {estado: convertido, particoes: [ano, mes], chave: [], colunas: 42}
  unidades_de_medida:
    salario_mensal: "R$"
    saldo_mov: "movimentações (+1 admissão, -1 desligamento)"
    tempo_emprego: "meses"
    qtd_hora_contrat: "horas por semana"
  dados_pessoais:
  - coluna: "sexo"
    categoria: "sexo"
  - coluna: "raca_cor"
    categoria: "raça/cor"
  - coluna: "idade"
    categoria: "idade"
  - coluna: "ind_portador_defic"
    categoria: "saúde (deficiência); idem tipo_defic"
  - coluna: "grau_instrucao"
    categoria: "escolaridade"
  - coluna: "cbo_2002_ocupacao"
    categoria: "ocupação"
  - coluna: "salario_mensal"
    categoria: "renda individual"
  - coluna: "municipio"
    categoria: "localização (município do estabelecimento); idem uf e as colunas de bairro, distrito e região"
  - coluna: "tempo_emprego"
    categoria: "dados do vínculo (tempo de emprego, competência de admissão ou desligamento)"
  eras:
  - {desde: "2008-01", mudanca: "entra a classe CNAE convertida (CNAE 1.0 a partir da CNAE 2.0)"}
  - {desde: "2017-11", mudanca: "indicadores de trabalho parcial e intermitente passam a existir, preenchidos desde 2018-05 (com retroação a 2017-11)"}
  revisado_em: "2026-11-10"
  versao_fetcher: "0.8.3"
---

# CAGED estatístico (microdados, 2007 a 2019)

## Resumo

O CAGED (Cadastro Geral de Empregados e Desempregados) registra cada admissão e desligamento de trabalhadores sob o regime da CLT, declarados pelos estabelecimentos. Esta é a série "estatística" que o MTE divulgou até dezembro de 2019; em 2020 foi substituída pelo Novo CAGED (datasets `caged-2020-*`), com outra metodologia, e as duas séries não são comparáveis diretamente.

São microdados individuais, mas a fonte os classifica como não identificados: não há nome, CPF nem PIS. As colunas abaixo descrevem pessoas e, combinadas, podem permitir reidentificação; trate como dado pessoal. Este repositório não reproduz valores individuais.

## Ficha técnica

<!-- gerado:ficha -->

| Campo | Valor |
|---|---|
| Dataset | `pdet/caged` |
| Órgão | Ministério do Trabalho e Emprego (MTE), Programa de Disseminação das Estatísticas do Trabalho (PDET) |
| Página oficial | ftp://ftp.mtps.gov.br/pdet/microdados/CAGED/ |
| Licença | Não informada pela fonte. O Comunicado MTPS 192/12 e a Nota Técnica MTPS 088/12 (ftp://ftp.mtps.gov.br/pdet/microdados/) descrevem os microdados como não identificados e não declaram licença nem termo de uso |
| Periodicidade | mensal; série encerrada em 2019-12, substituída pelo Novo CAGED em 2020 |
| Cobertura | 2007-01 a 2019-12 |
| Unidade (uma linha) | movimentação (admissão ou desligamento) de um trabalhador celetista em um mês |
| Granularidade | espacial: município do estabelecimento; temporal: mês |
| Formato bruto | 7z com um TXT por mês (CAGEDEST_MMAAAA.txt), latin-1, separador ponto e vírgula |
| Estado no lake | convertido |
| Partições | `ano`, `mes` |
| Chave | — |
| Colunas | 42 |
| Unidades de medida | `salario_mensal`: R$; `saldo_mov`: movimentações (+1 admissão, -1 desligamento); `tempo_emprego`: meses; `qtd_hora_contrat`: horas por semana |
| Dados pessoais | `sexo` (sexo); `raca_cor` (raça/cor); `idade` (idade); `ind_portador_defic` (saúde (deficiência); idem tipo_defic); `grau_instrucao` (escolaridade); `cbo_2002_ocupacao` (ocupação); `salario_mensal` (renda individual); `municipio` (localização (município do estabelecimento); idem uf e as colunas de bairro, distrito e região); `tempo_emprego` (dados do vínculo (tempo de emprego, competência de admissão ou desligamento)) |
| Eras | 2008-01: entra a classe CNAE convertida (CNAE 1.0 a partir da CNAE 2.0); 2017-11: indicadores de trabalho parcial e intermitente passam a existir, preenchidos desde 2018-05 (com retroação a 2017-11) |
| Revisado em | 2026-11-10 |
| Versão do fetcher | 0.8.3 |

## Origem e coleta

- O fetcher baixa os arquivos mensais do FTP do PDET (`ftp://ftp.mtps.gov.br/pdet/microdados/CAGED/`) e guarda em `caged/<ano>/caged_AAAAMM@AAAAMMDD.7z`. O lake converte a versão mais recente de cada mês.
- Conferido no Drive em 2026-11-10: 156 arquivos, de 2007-01 a 2019-12, sem lacuna, uma versão cada.
- O layout oficial é `CAGEDEST_layout_Atualizado.xls` (atualizado em 2018-06-21), na pasta de documentação do FTP.

## Conteúdo

- Uma linha por movimentação (admissão ou desligamento). Amostra convertida: 2007-01 (2.182.618 linhas, saldo +105.468) e 2019-12 (2.289.007 linhas, saldo -307.311).
- Cobertura nominal: 2007-01 a 2019-12 (156 meses seguidos). O PDET anuncia microdados do CAGED estatístico desde janeiro de 2009 no comunicado MTPS 192/12 (de 2012); a pasta tem 2007 em diante.
- Conferência de linhas: 2007-01: 2.182.618 e 2019-12: 2.289.007, iguais às linhas do TXT menos o cabeçalho.
- As séries `caged-ajustes` trazem as declarações fora do prazo; o saldo "com ajuste" soma as duas.

## Variáveis

Os nomes vêm do cabeçalho do TXT, normalizado. Rótulos de código: layout oficial do CAGED estatístico. A coluna Período mostra em quais dos 2 meses amostrados (2007, 2019) a coluna existe. Todas as colunas são texto.

| Coluna no lake | Na fonte | Tipo | Descrição | Unidade / códigos | Período |
|---|---|---|---|---|---|
| `admitidos_desligados` | ADMITIDOS/DESLIGADOS | texto | Admissão ou desligamento | 1 admissão, 2 desligamento (no lake aparece como `01` e `02`) | todo |
| `competencia_declarada` | COMPETENCIA | texto | Competência (mês/ano) em que a movimentação foi declarada | AAAAMM | todo |
| `municipio` | MUNICIPIO | texto | Município de localização do estabelecimento | código de 6 dígitos (IBGE sem dígito verificador); ignorado como `<UF>9999` ou `-1` | todo |
| `ano_declarado` | ANO COMP | texto | Ano em que a movimentação foi declarada (o layout diz “de 1996 até 2009”, mas o campo traz o ano de cada arquivo) | AAAA | todo |
| `cbo_2002_ocupacao` | CBO 2002 | texto | Ocupação pela CBO 2002 | 6 dígitos; -1 ignorado | todo |
| `cnae_1_0_classe` | CLAS CNAE 10 | texto | Classe de atividade econômica pela CNAE/95, convertida a partir da classe CNAE 2.0, disponível a partir de 01/2008 | 5 dígitos; 0 não informado; -1 ignorado | todo |
| `cnae_2_0_classe` | CLAS CNAE 20 | texto | Classe de atividade econômica pela CNAE 2.0 | 5 dígitos | todo |
| `cnae_2_0_subclas` | SB CLAS 20 | texto | Subclasse de atividade econômica pela CNAE 2.0, a partir de 2006 | 7 dígitos | todo |
| `faixa_empr_inicio_jan` | FX EMP JAN | texto | Tamanho do estabelecimento em janeiro do ano de referência | 1 até 4, 2 de 5 a 9, 3 de 10 a 19, 4 de 20 a 49, 5 de 50 a 99, 6 de 100 a 249, 7 de 250 a 499, 8 de 500 a 999, 9 mil ou mais, -1 ignorado | todo |
| `grau_instrucao` | GRAU INSTR | texto | Grau de instrução ou escolaridade | 1 analfabeto, 2 até 5ª incompleto, 3 5ª completo, 4 6ª a 9ª, 5 fundamental completo, 6 médio incompleto, 7 médio completo, 8 superior incompleto, 9 superior completo, 10 mestrado, 11 doutorado, -1 ignorado | todo |
| `qtd_hora_contrat` | HORAS CONT P | texto | Quantidade de horas contratuais por semana | horas por semana; -1 ignorado | todo |
| `ibge_subsetor` | IBGE subsetor | texto | Subsetor econômico segundo o IBGE | 1 a 25; -1 ignorado | todo |
| `idade` | IDADE P | texto | Idade do trabalhador | anos | todo |
| `ind_aprendiz` | IND APRENDIZ | texto | Indicador de movimentação referente a contrato de aprendizagem | 0 não, 1 sim | todo |
| `ind_portador_defic` | PORT DEFIC | texto | Indicador de empregado portador de deficiência habilitado ou beneficiário reabilitado | 0 não, 1 sim | todo |
| `raca_cor` | RACACOR | texto | Raça e cor do trabalhador, disponível a partir de 01/2007 | 1 indígena, 2 branca, 4 preta, 6 amarela, 8 parda, 9 não identificado, -1 ignorado; `{ñ` em 3.172 linhas de 2007-01 (provável “{ñ class}”, que o layout manda tratar como ignorado) | todo |
| `salario_mensal` | SAL MENSAL | texto | Salário mensal em moeda corrente | R$, vírgula decimal (`000000,00`) | todo |
| `saldo_mov` | SALDO MOV | texto | Saldo de movimentação | 1 para admissão e -1 para desligamento; formato varia (`000001`, `01`, `-1`, `-00001`) | todo |
| `sexo` | SEXO | texto | Sexo do trabalhador | 1 masculino, 2 feminino, -1 ignorado | todo |
| `tempo_emprego` | TEMP EMP P | texto | Tempo de emprego do trabalhador | meses, vírgula decimal (`0600,0`) | todo |
| `tipo_estab` | TIPO ESTBL | texto | Tipo de estabelecimento | 1 CNPJ, 3 CEI, 9 não identificado, -1 ignorado | todo |
| `tipo_defic` | TP DEFIC | texto | Tipo de deficiência ou beneficiário habilitado | 0 não deficiente, 1 física, 2 auditiva, 3 visual, 4 intelectual, 5 múltipla, 6 reabilitado, -1 ignorado; `{ñ` em todas as linhas de 2007-01 | todo |
| `tipo_mov_desagregado` | TP MOV DESAG | texto | Tipo de movimento | 1 primeiro emprego, 2 reemprego, 3 transferência (admissão), 4 a 9 desligamentos (sem justa causa, com justa causa, a pedido, aposentadoria, morte, transferência), 10 reintegração, 11 término de contrato, 25 contrato por prazo determinado, 43 término de prazo determinado, 90 acordo, -1 ignorado | todo |
| `uf` | UF | texto | Unidade da Federação de localização do estabelecimento (o layout intitula o campo como município por engano) | código IBGE de 2 dígitos, ex.: 35 São Paulo | todo |
| `bairros_sp` | BAIRRO_SP | texto | Bairro do município de São Paulo | código (planilha do layout) | todo |
| `bairros_fortaleza` | BAIRRO FORT | texto | Bairro de Fortaleza | código (planilha do layout) | todo |
| `bairros_rj` | BAIRRO_RJ | texto | Bairro do Rio de Janeiro | código (planilha do layout) | todo |
| `distritos_sp` | DISTRITO_SP | texto | Distrito de São Paulo | código (planilha do layout) | todo |
| `regioes_adm_df` | REG ADM DF | texto | Região Administrativa do Distrito Federal | código (planilha do layout) | todo |
| `mesorregiao` | Mesorregiao | texto | Mesorregião | código (planilha do layout) | todo |
| `microrregiao` | Microrregiao | texto | Microrregião | código (planilha do layout) | todo |
| `regiao_adm_rj` | Região Adm RJ | texto | Região administrativa do Rio de Janeiro | código (planilha do layout) | todo |
| `regiao_adm_sp` | Região Adm SP | texto | Região administrativa de São Paulo | código (planilha do layout) | todo |
| `regiao_corede` | Regiao Corede | texto | Região Corede (RS) | código (planilha do layout) | todo |
| `regiao_corede_04` | Regiao Corede 04 | texto | Região Corede 04 (RS) | código (planilha do layout) | todo |
| `regiao_gov_sp` | Regiao Gov SP | texto | Região de governo de São Paulo | código (planilha do layout) | todo |
| `regiao_senac_pr` | Regiao Senac PR | texto | Região Senac (PR) | código (planilha do layout) | todo |
| `regiao_senai_pr` | Regiao Senai PR | texto | Região Senai (PR) | código (planilha do layout) | todo |
| `regiao_senai_sp` | Regiao Senai SP | texto | Região Senai (SP) | código (planilha do layout) | todo |
| `sub_regiao_senai_pr` | Sub-Regiao Senai PR | texto | Sub-região Senai (PR) | código (planilha do layout) | todo |
| `ind_trab_parcial` | IND TRAB PARCIAL | texto | Indicador de movimentação em contrato parcial (a partir de 05/2018, retroagindo a 11/2017) | 0 não, 1 sim; vazio nos meses anteriores | 2019 (anos amostrados) |
| `ind_trab_intermitente` | IND TRAB INTERMITENTE | texto | Indicador de movimentação em contrato intermitente (a partir de 05/2018, retroagindo a 11/2017) | 0 não, 1 sim; vazio nos meses anteriores | 2019 (anos amostrados) |

## No lake

- Dataset `pdet/caged`, particionado por `ano` e `mes`: `ano=2019/mes=12/caged_201912.parquet`. Um Parquet por mês; `ano` é inteiro e `mes` é texto com zero à esquerda.
- A partição vem do nome do arquivo (`caged_201912@…7z`).
- O conversor extrai o TXT com `7z`, lê com DuckDB (latin-1, `;`) e grava tudo como texto. Se o cabeçalho não tem `municipio` e `sexo`, recusa o arquivo.
- Chave: não há. Nulos: as colunas que não existem em um mês aparecem vazias na união dos arquivos (`ind_trab_parcial` e `ind_trab_intermitente` em 2007); nas demais colunas não há vazios na amostra.
- Linhas: iguais às do TXT menos o cabeçalho nos dois meses amostrados.

## Cuidados de uso

- **Tudo é texto, sem normalização de formato.** O conversor não converte tipos nem tira zeros à esquerda ou espaços: o mesmo código aparece como `1`, `01` ou `000001`, e valores decimais usam vírgula (`0000686,80`). Converta e normalize antes de comparar anos.
- **Saldo em formatos diferentes.** `saldo_mov` aparece como `000001` e `-00001` em 2007-01 e como `01` e `-1` em 2019-12 (conferido em 2026-11-10); converta para inteiro antes de somar. O saldo de 2019-12 é -307.311 e o de 2007-01 é +105.468.
- **Valores ignorados.** Os campos usam `-1`, `{ñ` e `0` para ignorado ou não informado; `tipo_defic` de 2007-01 é `{ñ` em todas as linhas. Trate antes de agregar.
- **Declarações fora do prazo ficam em outro dataset.** O saldo do mês aqui não inclui os ajustes (`caged-ajustes`).
- **Descontinuidade em 2020.** A série vai até 2019-12; o Novo CAGED começa em 2020-01 com outra metodologia (eSocial e imputação) e não é comparável direto.
- **Dois níveis de região.** `uf` (código IBGE de 2 dígitos) e `municipio` (6 dígitos) estão em colunas separadas, apesar de o layout oficial intitular o campo `UF` como município.
- **Colunas regionais por estado.** `regiao_senai_pr`, `regiao_corede` e similares são recortes de um estado; fora dele a fonte grava o marcador `{ñ` ou `{ñ c` (o “{ñ class}” do layout, truncado), que significa não classificado. Em 2019-12, `regiao_senai_pr` é `{ñ` em 2.116.676 de 2.289.007 linhas.
- **Cautela com variáveis recentes ou muito desagregadas.** A Nota Técnica MTPS 088/12 avisa que Indicador de Pessoa com Deficiência, Tipo de Deficiência, Subclasse de atividade econômica, Ocupação e Raça/Cor devem ser usadas com cautela: algumas são recentes e, no nível de desagregação fino, podem ter inconsistências; em nível agregado o uso não fica comprometido.
- **Dado pessoal.** Veja o inventário no cabeçalho.

## Exemplos

Saldo (admissões menos desligamentos) por UF em dezembro de 2019, com a conversão do saldo para inteiro:

```python
import duckdb

duckdb.sql("""
    SELECT uf, sum(CAST(saldo_mov AS INTEGER)) AS saldo
    FROM read_parquet('lake/pdet/caged/ano=2019/mes=12/*.parquet')
    GROUP BY uf ORDER BY saldo
""").show()
```

## Referências

- Microdados do PDET (FTP): ftp://ftp.mtps.gov.br/pdet/microdados/
- Comunicado MTPS 192/12 e Nota Técnica MTPS 088/12, na mesma pasta.
- Layout `CAGEDEST_layout_Atualizado.xls`, na pasta de documentação do FTP.
