---
dataset:
  id: "caged-ajustes"
  fonte: "pdet"
  titulo: "CAGED - ajustes de declarações fora do prazo (2002 a 2019)"
  resumo: "Movimentações do CAGED declaradas fora do prazo, de 2002 a 2019, usadas para ajustar o saldo de emprego."
  orgao: "Ministério do Trabalho e Emprego (MTE), Programa de Disseminação das Estatísticas do Trabalho (PDET)"
  pagina_oficial: "ftp://ftp.mtps.gov.br/pdet/microdados/CAGED_AJUSTES/"
  licenca: "Não informada pela fonte. O Comunicado MTPS 192/12 e a Nota Técnica MTPS 088/12 (ftp://ftp.mtps.gov.br/pdet/microdados/) descrevem os microdados como não identificados e não declaram licença nem termo de uso"
  periodicidade: "mensal desde 2010-01; anual de 2002 a 2009; encerrado em 2019-12"
  cobertura: {inicio: "2002", fim: "2019-12"}
  unidade: "movimentação (admissão ou desligamento) declarada fora do prazo"
  granularidade: {espacial: "município do estabelecimento", temporal: "mês da movimentação"}
  formato_bruto: "7z com um TXT por mês (2010 a 2019) ou por ano (2002 a 2009), latin-1, separador ponto e vírgula"
  lake: {estado: convertido, particoes: [ano, mes], chave: [], colunas: 44}
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
  - {desde: "2010-01", mudanca: "os arquivos passam de anuais a mensais"}
  - {desde: "2011-01", mudanca: "o MTE passa a divulgar duas séries do CAGED, com e sem os ajustes (Nota Técnica 082/2011)"}
  revisado_em: "2026-11-10"
  versao_fetcher: "0.8.3"
---

# CAGED - ajustes de declarações fora do prazo (2002 a 2019)

## Resumo

As declarações do CAGED entregues depois do prazo não entram no CAGED estatístico do mês. O MTE as divulga à parte, nos "ajustes", e a partir de janeiro de 2011 passou a publicar duas séries de emprego: uma sem os ajustes (a série tradicional) e outra com eles (fonte: README do pacote, citando a Nota Técnica 082/2011).

São microdados individuais, mas a fonte os classifica como não identificados: não há nome, CPF nem PIS. As colunas abaixo descrevem pessoas e, combinadas, podem permitir reidentificação; trate como dado pessoal. Este repositório não reproduz valores individuais.

## Ficha técnica

<!-- gerado:ficha -->

| Campo | Valor |
|---|---|
| Dataset | `pdet/caged-ajustes` |
| Órgão | Ministério do Trabalho e Emprego (MTE), Programa de Disseminação das Estatísticas do Trabalho (PDET) |
| Página oficial | ftp://ftp.mtps.gov.br/pdet/microdados/CAGED_AJUSTES/ |
| Licença | Não informada pela fonte. O Comunicado MTPS 192/12 e a Nota Técnica MTPS 088/12 (ftp://ftp.mtps.gov.br/pdet/microdados/) descrevem os microdados como não identificados e não declaram licença nem termo de uso |
| Periodicidade | mensal desde 2010-01; anual de 2002 a 2009; encerrado em 2019-12 |
| Cobertura | 2002 a 2019-12 |
| Unidade (uma linha) | movimentação (admissão ou desligamento) declarada fora do prazo |
| Granularidade | espacial: município do estabelecimento; temporal: mês da movimentação |
| Formato bruto | 7z com um TXT por mês (2010 a 2019) ou por ano (2002 a 2009), latin-1, separador ponto e vírgula |
| Estado no lake | convertido |
| Partições | `ano`, `mes` |
| Chave | — |
| Colunas | 44 |
| Unidades de medida | `salario_mensal`: R$; `saldo_mov`: movimentações (+1 admissão, -1 desligamento); `tempo_emprego`: meses; `qtd_hora_contrat`: horas por semana |
| Dados pessoais | `sexo` (sexo); `raca_cor` (raça/cor); `idade` (idade); `ind_portador_defic` (saúde (deficiência); idem tipo_defic); `grau_instrucao` (escolaridade); `cbo_2002_ocupacao` (ocupação); `salario_mensal` (renda individual); `municipio` (localização (município do estabelecimento); idem uf e as colunas de bairro, distrito e região); `tempo_emprego` (dados do vínculo (tempo de emprego, competência de admissão ou desligamento)) |
| Eras | 2010-01: os arquivos passam de anuais a mensais; 2011-01: o MTE passa a divulgar duas séries do CAGED, com e sem os ajustes (Nota Técnica 082/2011) |
| Revisado em | 2026-11-10 |
| Versão do fetcher | 0.8.3 |

## Origem e coleta

- O fetcher baixa do FTP do PDET (`ftp://ftp.mtps.gov.br/pdet/microdados/CAGED_AJUSTES/`) e guarda em `caged-ajustes/<ano>/caged-ajustes_<ano ou AAAAMM>@AAAAMMDD.7z`.
- Conferido no Drive em 2026-11-10: 128 arquivos, 8 anuais (2002 a 2009) e 120 mensais (2010-01 a 2019-12, sem lacuna), uma versão cada.
- O layout oficial (`2CAGEDEST_AjustesForaPrazo_layout_Atualizado.xls`) está no FTP, mas o arquivo não pôde ser lido nesta revisão (erro de codificação). As descrições abaixo vêm do layout do CAGED estatístico, que tem as mesmas variáveis; as colunas exclusivas dos ajustes estão marcadas como inferidas.

## Conteúdo

- Uma linha por movimentação declarada fora do prazo. Amostra convertida: 2002 (arquivo anual, 901.889 linhas) e 2019-12 (80.023 linhas).
- Cobertura nominal: 2002 a 2019-12.
- Conferência de linhas: iguais às do TXT menos o cabeçalho nos dois arquivos.
- Os arquivos anuais (2002 a 2009) caem em `mes=__HIVE_DEFAULT_PARTITION__`, porque o nome não traz o mês.

## Variáveis

Mesmo conjunto de variáveis do `caged`, com `competencia_movimentacao`, `ano_movimentacao` e `regiao_metro_mte` próprias dos ajustes. A coluna Período mostra os anos amostrados (2002 e 2019). Todas as colunas são texto.

| Coluna no lake | Na fonte | Tipo | Descrição | Unidade / códigos | Período |
|---|---|---|---|---|---|
| `admitidos_desligados` | ADMITIDOS/DESLIGADOS | texto | Admissão ou desligamento | 1 admissão, 2 desligamento (no lake aparece como `01` e `02`) | todo |
| `competencia_movimentacao` | COMPETENCIA MOVIMENTACAO (inferido) | texto | Competência da movimentação ajustada; o layout de ajustes não pôde ser lido e o sentido vem do nome | AAAAMM | todo |
| `municipio` | MUNICIPIO | texto | Município de localização do estabelecimento | código de 6 dígitos (IBGE sem dígito verificador); ignorado como `<UF>9999` ou `-1` | todo |
| `ano_movimentacao` | ANO MOVIMENTACAO (inferido) | texto | Ano da movimentação; mesma ressalva | AAAA | todo |
| `cbo_94_ocupacao` | CBO 94 | texto | Ocupação pela CBO 1994 | código | todo |
| `cbo_2002_ocupacao` | CBO 2002 | texto | Ocupação pela CBO 2002 | 6 dígitos; -1 ignorado | todo |
| `cnae_1_0_classe` | CLAS CNAE 10 | texto | Classe de atividade econômica pela CNAE/95, convertida a partir da classe CNAE 2.0, disponível a partir de 01/2008 | 5 dígitos; 0 não informado; -1 ignorado | todo |
| `faixa_empr_inicio_jan` | FX EMP JAN | texto | Tamanho do estabelecimento em janeiro do ano de referência | 1 até 4, 2 de 5 a 9, 3 de 10 a 19, 4 de 20 a 49, 5 de 50 a 99, 6 de 100 a 249, 7 de 250 a 499, 8 de 500 a 999, 9 mil ou mais, -1 ignorado | todo |
| `grau_instrucao` | GRAU INSTR | texto | Grau de instrução ou escolaridade | 1 analfabeto, 2 até 5ª incompleto, 3 5ª completo, 4 6ª a 9ª, 5 fundamental completo, 6 médio incompleto, 7 médio completo, 8 superior incompleto, 9 superior completo, 10 mestrado, 11 doutorado, -1 ignorado | todo |
| `qtd_hora_contrat` | HORAS CONT P | texto | Quantidade de horas contratuais por semana | horas por semana; -1 ignorado | todo |
| `ibge_subsetor` | IBGE subsetor | texto | Subsetor econômico segundo o IBGE | 1 a 25; -1 ignorado | todo |
| `idade` | IDADE P | texto | Idade do trabalhador | anos | todo |
| `ind_aprendiz` | IND APRENDIZ | texto | Indicador de movimentação referente a contrato de aprendizagem | 0 não, 1 sim | todo |
| `salario_mensal` | SAL MENSAL | texto | Salário mensal em moeda corrente | R$, vírgula decimal (`000000,00`) | todo |
| `saldo_mov` | SALDO MOV | texto | Saldo de movimentação | 1 para admissão e -1 para desligamento; formato varia (`000001`, `01`, `-1`, `-00001`) | todo |
| `sexo` | SEXO | texto | Sexo do trabalhador | 1 masculino, 2 feminino, -1 ignorado | todo |
| `tempo_emprego` | TEMP EMP P | texto | Tempo de emprego do trabalhador | meses, vírgula decimal (`0600,0`) | todo |
| `tipo_estab` | TIPO ESTBL | texto | Tipo de estabelecimento | 1 CNPJ, 3 CEI, 9 não identificado, -1 ignorado | todo |
| `tipo_mov_desagregado` | TP MOV DESAG | texto | Tipo de movimento | 1 primeiro emprego, 2 reemprego, 3 transferência (admissão), 4 a 9 desligamentos (sem justa causa, com justa causa, a pedido, aposentadoria, morte, transferência), 10 reintegração, 11 término de contrato, 25 contrato por prazo determinado, 43 término de prazo determinado, 90 acordo, -1 ignorado | todo |
| `uf` | UF | texto | Unidade da Federação de localização do estabelecimento (o layout intitula o campo como município por engano) | código IBGE de 2 dígitos, ex.: 35 São Paulo | todo |
| `competencia_declarada` | COMPETENCIA DECLARADA (inferido) | texto | Competência em que a declaração fora do prazo foi feita; o layout de ajustes não pôde ser lido e o sentido vem do nome | AAAAMM | todo |
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
| `regiao_metro_mte` | Região Metro MTE (inferido) | texto | Região metropolitana segundo o MTE; o layout lido não a descreve | código | todo |
| `cnae_2_0_subclas` | SB CLAS 20 | texto | Subclasse de atividade econômica pela CNAE 2.0, nos meses em que a coluna existe | 7 dígitos | 2019 (anos amostrados) |
| `raca_cor` | RACACOR | texto | Raça e cor do trabalhador, disponível a partir de 01/2007 | 1 indígena, 2 branca, 4 preta, 6 amarela, 8 parda, 9 não identificado, -1 ignorado; `{ñ` em 3.172 linhas de 2007-01 (provável “{ñ class}”, que o layout manda tratar como ignorado) | 2019 (anos amostrados) |
| `ind_portador_defic` | PORT DEFIC | texto | Indicador de empregado portador de deficiência habilitado ou beneficiário reabilitado | 0 não, 1 sim | 2019 (anos amostrados) |
| `tipo_defic` | TP DEFIC | texto | Tipo de deficiência ou beneficiário habilitado | 0 não deficiente, 1 física, 2 auditiva, 3 visual, 4 intelectual, 5 múltipla, 6 reabilitado, -1 ignorado; `{ñ` em todas as linhas de 2007-01 | 2019 (anos amostrados) |
| `ind_trab_parcial` | IND TRAB PARCIAL | texto | Indicador de movimentação em contrato parcial (a partir de 05/2018, retroagindo a 11/2017) | 0 não, 1 sim; vazio nos meses anteriores | 2019 (anos amostrados) |
| `ind_trab_intermitente` | IND TRAB INTERMITENTE | texto | Indicador de movimentação em contrato intermitente (a partir de 05/2018, retroagindo a 11/2017) | 0 não, 1 sim; vazio nos meses anteriores | 2019 (anos amostrados) |

## No lake

- Dataset `pdet/caged-ajustes`, particionado por `ano` e `mes`: `ano=2019/mes=12/…` para os mensais e `ano=2002/mes=__HIVE_DEFAULT_PARTITION__/…` para os anuais.
- A partição vem do nome do arquivo. Texto, sem conversão de tipos; arquivo recusado se o cabeçalho não tem `municipio` e `sexo`.
- Chave: não há. Linhas: iguais às do TXT menos o cabeçalho.

## Cuidados de uso

- **Tudo é texto, sem normalização de formato.** O conversor não converte tipos nem tira zeros à esquerda ou espaços: o mesmo código aparece como `1`, `01` ou `000001`, e valores decimais usam vírgula (`0000686,80`). Converta e normalize antes de comparar anos.
- **Meses de 2002 a 2009 sem partição.** Os arquivos anuais ficam em `mes=__HIVE_DEFAULT_PARTITION__`; o mês da movimentação está em `competencia_movimentacao`.
- **Ajustes não substituem o CAGED.** Os ajustes são movimentações adicionais, de competência anterior; o saldo "com ajuste" de um mês soma `caged` e os ajustes declarados até ele. A fonte documenta as duas séries; não verifiquei a soma.
- **Semântica de duas competências.** `competencia_declarada` e `competencia_movimentacao` têm sentido inferido pelo nome; confirme no layout oficial.
- **Saldo e códigos em formatos variados**, como no `caged` (`saldo_mov` de 2002 e 2019 em formatos diferentes).
- **Cautela com variáveis recentes ou muito desagregadas.** A Nota Técnica MTPS 088/12 avisa que Indicador de Pessoa com Deficiência, Tipo de Deficiência, Subclasse de atividade econômica, Ocupação e Raça/Cor devem ser usadas com cautela: algumas são recentes e, no nível de desagregação fino, podem ter inconsistências; em nível agregado o uso não fica comprometido.
- **Dado pessoal.** Veja o inventário no cabeçalho.

## Exemplos

Saldo dos ajustes de dezembro de 2019 por UF:

```python
import duckdb

duckdb.sql("""
    SELECT uf, sum(CAST(saldo_mov AS INTEGER)) AS saldo
    FROM read_parquet('lake/pdet/caged-ajustes/ano=2019/mes=12/*.parquet')
    GROUP BY uf ORDER BY saldo
""").show()
```

## Referências

- Microdados do PDET (FTP): ftp://ftp.mtps.gov.br/pdet/microdados/
- Nota Técnica 082/2011 (índice de emprego com ajustes), citada pelo PDET.
