---
dataset:
  id: "rais-vinculos"
  fonte: "pdet"
  titulo: "RAIS - vínculos (microdados)"
  resumo: "Microdados anuais da RAIS por vínculo empregatício, não identificados, de 1985 em diante."
  orgao: "Ministério do Trabalho e Emprego (MTE), Programa de Disseminação das Estatísticas do Trabalho (PDET)"
  pagina_oficial: "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/"
  licenca: "Não informada pela fonte. O Comunicado MTPS 192/12 e a Nota Técnica MTPS 088/12 (ftp://ftp.mtps.gov.br/pdet/microdados/) descrevem os microdados como não identificados e não declaram licença nem termo de uso"
  periodicidade: "anual (o ano-base é publicado no ano seguinte, com republicações)"
  cobertura: {inicio: "1985", fim: "2025"}
  unidade: "vínculo empregatício declarado na RAIS no ano-base"
  granularidade: {espacial: "município do estabelecimento (e, a partir de 2002, do local de trabalho)", temporal: "ano"}
  formato_bruto: "7z com um TXT por UF (1985 a 2017) ou por macrorregião (2018 em diante); latin-1, separador ponto e vírgula; republicações de 2023 em diante em CSV com vírgula, aspas e extensão .COMT"
  lake: {estado: parcial, particoes: [ano, regiao], chave: [], colunas: 77}
  unidades_de_medida:
    vl_remun_dezembro_sm: "salários mínimos"
    vl_remun_media_sm: "salários mínimos"
    vl_remun_dezembro_nom: "R$"
    vl_remun_media_nom: "R$"
    vl_salario_contratual: "R$"
    tempo_emprego: "meses"
    qtd_hora_contr: "horas por semana"
    qtd_dias_afastamento: "dias"
    vl_rem_janeiro_cc: "R$ (idem os demais meses, sufixos cc e sc)"
  dados_pessoais:
  - coluna: "sexo_trabalhador"
    categoria: "sexo"
  - coluna: "raca_cor"
    categoria: "raça/cor"
  - coluna: "idade"
    categoria: "idade"
  - coluna: "faixa_etaria"
    categoria: "idade (faixa)"
  - coluna: "nacionalidade"
    categoria: "nacionalidade"
  - coluna: "ano_chegada_brasil"
    categoria: "origem (ano de chegada ao Brasil)"
  - coluna: "ind_portador_defic"
    categoria: "saúde (deficiência)"
  - coluna: "tipo_defic"
    categoria: "saúde (tipo de deficiência)"
  - coluna: "causa_afastamento_1"
    categoria: "saúde/afastamento (acidente, doença, licença); idem causa_afastamento_2 e 3 e qtd_dias_afastamento"
  - coluna: "grau_instrucao_2005_1985"
    categoria: "escolaridade; idem escolaridade_apos_2005"
  - coluna: "cbo_ocupacao"
    categoria: "ocupação; idem cbo_94_ocupacao e cbo_ocupacao_2002"
  - coluna: "vl_remun_dezembro_nom"
    categoria: "renda individual; idem vl_remun_media_nom, as colunas _sm, vl_salario_contratual e vl_rem_<mês>_cc/_sc"
  - coluna: "mes_admissao"
    categoria: "datas do vínculo (mês de admissão e de desligamento, tempo de emprego)"
  - coluna: "municipio"
    categoria: "localização (município do estabelecimento); idem mun_trab, bairros_*, distritos_sp e regioes_adm_df"
  eras:
  - {desde: "1994-01", mudanca: "entram horas contratuais, idade, natureza jurídica do estabelecimento, tipo de admissão e CBO/CNAE de 1994/1995"}
  - {desde: "1996-01", mudanca: "entram bairros de Fortaleza e do Rio de Janeiro e regiões administrativas do DF"}
  - {desde: "1999-01", mudanca: "entram remunerações nominais em R$ e indicador de CEI vinculado"}
  - {desde: "2002-01", mudanca: "entram causas e dias de afastamento e município de trabalho"}
  - {desde: "2004-01", mudanca: "entra a CNAE 2.0 (classe e subclasse)"}
  - {desde: "2006-01", mudanca: "entram escolaridade pós-2005 e raça/cor"}
  - {desde: "2007-01", mudanca: "entram indicador e tipo de deficiência"}
  - {desde: "2015-01", mudanca: "entram as remunerações mensais de janeiro a novembro"}
  - {desde: "2017-01", mudanca: "entram os indicadores de trabalho parcial e intermitente"}
  - {desde: "2018-01", mudanca: "arquivos passam de UF para macrorregião (norte, nordeste, mgesrj, sp, sul, centrooeste)"}
  - {desde: "2022-01", mudanca: "entra a macrorregião ni (não identificado)"}
  - {desde: "2023-01", mudanca: "republicações em CSV com vírgula, aspas, extensão .COMT e outros nomes de coluna; o conversor não as lê"}
  revisado_em: "2026-11-10"
  versao_fetcher: "0.8.3"
---

# RAIS - vínculos (microdados)

## Resumo

A RAIS (Relação Anual de Informações Sociais) é o registro administrativo anual de todos os vínculos formais de emprego declarados pelos estabelecimentos. Os microdados do PDET trazem um registro por vínculo, com o perfil do trabalhador (sexo, idade, raça/cor, escolaridade, nacionalidade), o vínculo (tipo, admissão, desligamento, remuneração, ocupação) e o estabelecimento (atividade econômica, tamanho, município).

São microdados individuais, mas a fonte os classifica como não identificados: não há nome, CPF nem PIS. As colunas abaixo descrevem pessoas e, combinadas, podem permitir reidentificação; trate como dado pessoal. Este repositório não reproduz valores individuais.

## Ficha técnica

<!-- gerado:ficha -->

| Campo | Valor |
|---|---|
| Dataset | `pdet/rais-vinculos` |
| Órgão | Ministério do Trabalho e Emprego (MTE), Programa de Disseminação das Estatísticas do Trabalho (PDET) |
| Página oficial | ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/ |
| Licença | Não informada pela fonte. O Comunicado MTPS 192/12 e a Nota Técnica MTPS 088/12 (ftp://ftp.mtps.gov.br/pdet/microdados/) descrevem os microdados como não identificados e não declaram licença nem termo de uso |
| Periodicidade | anual (o ano-base é publicado no ano seguinte, com republicações) |
| Cobertura | 1985 a 2025 |
| Unidade (uma linha) | vínculo empregatício declarado na RAIS no ano-base |
| Granularidade | espacial: município do estabelecimento (e, a partir de 2002, do local de trabalho); temporal: ano |
| Formato bruto | 7z com um TXT por UF (1985 a 2017) ou por macrorregião (2018 em diante); latin-1, separador ponto e vírgula; republicações de 2023 em diante em CSV com vírgula, aspas e extensão .COMT |
| Estado no lake | parcial |
| Partições | `ano`, `regiao` |
| Chave | — |
| Colunas | 77 |
| Unidades de medida | `vl_remun_dezembro_sm`: salários mínimos; `vl_remun_media_sm`: salários mínimos; `vl_remun_dezembro_nom`: R$; `vl_remun_media_nom`: R$; `vl_salario_contratual`: R$; `tempo_emprego`: meses; `qtd_hora_contr`: horas por semana; `qtd_dias_afastamento`: dias; `vl_rem_janeiro_cc`: R$ (idem os demais meses, sufixos cc e sc) |
| Dados pessoais | `sexo_trabalhador` (sexo); `raca_cor` (raça/cor); `idade` (idade); `faixa_etaria` (idade (faixa)); `nacionalidade` (nacionalidade); `ano_chegada_brasil` (origem (ano de chegada ao Brasil)); `ind_portador_defic` (saúde (deficiência)); `tipo_defic` (saúde (tipo de deficiência)); `causa_afastamento_1` (saúde/afastamento (acidente, doença, licença); idem causa_afastamento_2 e 3 e qtd_dias_afastamento); `grau_instrucao_2005_1985` (escolaridade; idem escolaridade_apos_2005); `cbo_ocupacao` (ocupação; idem cbo_94_ocupacao e cbo_ocupacao_2002); `vl_remun_dezembro_nom` (renda individual; idem vl_remun_media_nom, as colunas _sm, vl_salario_contratual e vl_rem_<mês>_cc/_sc); `mes_admissao` (datas do vínculo (mês de admissão e de desligamento, tempo de emprego)); `municipio` (localização (município do estabelecimento); idem mun_trab, bairros_*, distritos_sp e regioes_adm_df) |
| Eras | 1994-01: entram horas contratuais, idade, natureza jurídica do estabelecimento, tipo de admissão e CBO/CNAE de 1994/1995; 1996-01: entram bairros de Fortaleza e do Rio de Janeiro e regiões administrativas do DF; 1999-01: entram remunerações nominais em R$ e indicador de CEI vinculado; 2002-01: entram causas e dias de afastamento e município de trabalho; 2004-01: entra a CNAE 2.0 (classe e subclasse); 2006-01: entram escolaridade pós-2005 e raça/cor; 2007-01: entram indicador e tipo de deficiência; 2015-01: entram as remunerações mensais de janeiro a novembro; 2017-01: entram os indicadores de trabalho parcial e intermitente; 2018-01: arquivos passam de UF para macrorregião (norte, nordeste, mgesrj, sp, sul, centrooeste); 2022-01: entra a macrorregião ni (não identificado); 2023-01: republicações em CSV com vírgula, aspas, extensão .COMT e outros nomes de coluna; o conversor não as lê |
| Revisado em | 2026-11-10 |
| Versão do fetcher | 0.8.3 |

## Origem e coleta

- O fetcher baixa os arquivos do FTP do PDET (`ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/`), pasta de microdados de vínculos, e guarda cada um sem alteração em `rais-vinculos/<ano>/rais-vinculos_<ano>[-<região>]@AAAAMMDD.7z`. Antes de 2018 o arquivo é por UF (`-ac`, `-sp`…); de 2018 em diante é por macrorregião (`-norte`, `-nordeste`, `-mgesrj`, `-sp`, `-sul`, `-centrooeste`, e `-ni` desde 2022). Há ainda um arquivo sem região em 1985 a 1997 (`IGNORANDOS<ano>.txt`, vínculos de UF ignorada).
- Quando a fonte republica um arquivo, o carimbo `@AAAAMMDD` muda e o lake converte só o mais novo (conferido no Drive em 2026-11-10: 951 arquivos, 950 chaves; a chave `rais-vinculos_1986-sp` tem duas versões, de 2026-04-24 e 2026-11-05).
- O fetcher guarda também a documentação (layouts em XLS por período). O dicionário oficial por ano está nos arquivos `RAIS_vinculos_layout*.xls` da pasta de documentação do FTP.

## Conteúdo

- Unidade: um vínculo declarado no ano-base. O indicador `vinculo_ativo_31_12` separa os vínculos ativos no fim do ano dos já encerrados.
- Cobertura nominal (listagem do bruto, conferido em 2026-11-10): ano-base de 1985 a 2025; 951 arquivos. 1985 a 1997 têm arquivo por UF (25 a 27 UFs por ano, pois o Tocantins só aparece a partir de 1989 e o Maranhão a partir de 1986) e um de UF ignorada; 1998 a 2017, 27 UFs; 2018 a 2021, 6 macrorregiões; 2022 a 2025, 7 (com `ni`).
- Amostra convertida para este documento (não é o conjunto inteiro): 16 arquivos/anos (1985, 1994, 1996, 1998, 1999, 2001, 2002, 2003, 2004, 2006, 2007, 2015, 2016, 2017, 2018, 2022); as colunas e a contagem de linhas valem para esses arquivos.
- Conferência de linhas: em todos os arquivos convertidos o número de linhas do lake é o de linhas do TXT menos o cabeçalho (por exemplo `rais-vinculos_2018-norte`: 3.682.332 linhas).
- **2023 a 2025 não chegam ao lake.** Os 21 arquivos regionais e de `ni` dessas republicações têm o dado dentro de um arquivo `.COMT`, extensão que o conversor não reconhece; ele os recusa com a mensagem "contains no data files (.csv/.txt)". Conferido em 2026-11-10 nos arquivos `rais-vinculos_2023-centrooeste`, `2023-ni` (2.400 linhas), `2024-ni` (16.544 linhas) e `2025-ni` (6.208 linhas); os demais arquivos de 2023 a 2025 não foram abertos, e supõe-se o mesmo formato.
- Eras do layout (datas por ano-base, conferidas nos layouts oficiais e na amostra): ver a lista `eras` do cabeçalho.

## Variáveis

Os nomes das colunas vêm do cabeçalho de cada arquivo, normalizado (minúsculas, sem acento, `_`). O mesmo conceito tem nomes diferentes entre eras (por exemplo `cbo_ocupacao`, `cbo_94_ocupacao` e `cbo_ocupacao_2002`); a coluna Período mostra em quais dos 16 anos amostrados cada nome aparece. Os rótulos de código vêm dos layouts oficiais (`RAIS_vinculos_layout*.xls`). Todas as colunas são texto.

| Coluna no lake | Na fonte | Tipo | Descrição | Unidade / códigos | Período |
|---|---|---|---|---|---|
| `bairros_sp` | BAIRRO_SP / Bairros SP | texto | Bairro do município de São Paulo onde fica o estabelecimento | código de 4 posições (planilha do layout) | todo |
| `motivo_desligamento` | CAUSA DESLI / Motivo Desligamento | texto | Causa do desligamento do vínculo no ano | rótulos abreviados do layout: 0 NAO DESL ANO; 10 DEM COM JC, 11 DEM SEM JC, 12 TERM CONTR; 20 DESL COM JC, 21 DESL SEM JC, 22 POSS OUT CAR; 30 e 31 TRANS C/ONUS e S/ONUS, 32 READAP/REDIS, 33 CESSAO, 34 REDISTRIBUIÇÃO; 40 MUD. REGIME; 50 REFORMA; 60 FALECIMENTO, 62 a 64 falecimentos por acidente ou doença; 70 a 80 APOS…; 90 DESL POR ACORDO; -1 IGNORADO | todo |
| `distritos_sp` | DISTRITO_SP / Distritos SP | texto | Distrito do município de São Paulo | código (planilha do layout) | todo |
| `vinculo_ativo_31_12` | EMP EM 31/12 / Vínculo Ativo 31/12 | texto | Indicador de vínculo ativo em 31/12 | 0 não, 1 sim | todo |
| `faixa_etaria` | FAIXA ETÁRIA | texto | Faixa etária do trabalhador | código de 2 dígitos (planilha FAIXAS do layout); `99` aparece em 7 linhas de 2018-norte | todo |
| `faixa_remun_dezem_sm` | FAIXA REMUN DEZEM (SM) | texto | Faixa da remuneração de dezembro em salários mínimos | código de 2 dígitos (planilha FAIXAS) | todo |
| `faixa_remun_media_sm` | FAIXA REMUN MEDIA (SM) | texto | Faixa da remuneração média do ano em salários mínimos | código de 2 dígitos (planilha FAIXAS) | todo |
| `faixa_tempo_emprego` | FAIXA TEMPO DE EMPREGO | texto | Faixa de tempo de emprego | código de 2 dígitos (planilha FAIXAS) | todo |
| `grau_instrucao_2005_1985` | GR INSTRUCAO 2005 a 1985 | texto | Grau de instrução, nos layouts até 2005 | 1 analfabeto a 11 doutorado (layout); -1 ignorado | 1985 a 2004 (anos amostrados) |
| `mes_admissao` | MÊS ADMISSAO | texto | Mês de admissão do trabalhador | 00 a 12 (`00` quando não admitido no ano) | todo |
| `mes_desligamento` | MÊS DESLIGAMENTO | texto | Mês de desligamento do trabalhador (o layout repete a descrição de admissão por engano) | 00 a 12 | todo |
| `municipio` | MUNICIPIO | texto | Município de localização do estabelecimento | código de 6 dígitos (IBGE sem o dígito verificador) | todo |
| `nacionalidade` | NACIONALIDAD | texto | Nacionalidade do trabalhador | códigos 10 e 20 a 80+ (planilha do layout, ex.: 10 brasileira) | todo |
| `vl_remun_dezembro_sm` | REM DEZEMBRO | texto | Remuneração de dezembro em salários mínimos | salários mínimos, vírgula decimal | todo |
| `vl_remun_media_sm` | REM MEDIA | texto | Remuneração média do ano em salários mínimos | salários mínimos, vírgula decimal | todo |
| `sexo_trabalhador` | SEXO | texto | Sexo do trabalhador | 1 masculino, 2 feminino, -1 ignorado; em 2018-norte o valor vem com espaços à esquerda (`                  01`) | todo |
| `tamanho_estabelecimento` | TAMESTAB | texto | Tamanho do estabelecimento pelos empregados ativos em 31/12 | 1 zero, 2 até 4, 3 de 5 a 9, 4 de 10 a 19, 5 de 20 a 49, 6 de 50 a 99, 7 de 100 a 249, 8 de 250 a 499, 9 de 500 a 999, 10 mil ou mais | todo |
| `tempo_emprego` | TEMP EMPR | texto | Tempo de emprego do trabalhador | meses, vírgula decimal (`  599,9`) | todo |
| `tipo_estab` | TIPO ESTBL (código) | texto | Tipo de estabelecimento: código (a primeira das duas colunas do arquivo com o mesmo título) | 1 CNPJ, 3 CEI, 5 CAEPF, 9 não identificado, -1 ignorado | todo |
| `tipo_estab_1` | TIPO ESTBL (nome) | texto | Tipo de estabelecimento: nome (a segunda coluna do arquivo com o mesmo título; o conversor acrescenta `_1` ao nome repetido) | texto como `CNPJ`, `CEI`, `CAEPF` | todo |
| `tipo_vinculo` | TP VINCULO | texto | Tipo de vínculo empregatício | 15 a 97 (CLT urbano/rural PF/PJ, estatutário, avulso, temporário, aprendiz, diretor, contratos por prazo e por lei); -1 ignorado | todo |
| `ibge_subatividade` | IBGE subatividade | texto | Subatividade do estabelecimento (IBGE) | código de 2 posições (planilha do layout) | 1985 (anos amostrados) |
| `ibge_subsetor` | IBGE Subsetor | texto | Subsetor IBGE 80 do estabelecimento | 1 a 25 (ex.: 1 extrativa mineral) | 1985, 2015 a 2022 (anos amostrados) |
| `cbo_ocupacao` | Ocupação | texto | Ocupação do trabalhador (CBO anterior) | código de 5 posições (planilha do layout) | 1985 (anos amostrados) |
| `cbo_94_ocupacao` | CBO 94 Ocupação | texto | Ocupação pela CBO 1994 | código de 5 posições | 1994 a 2002 (anos amostrados) |
| `cnae_95_classe` | CLAS CNAE 95 / CNAE 95 Classe | texto | Classe de atividade econômica pela CNAE/95 (CNAE 1.0 revisada em 2002, 614 categorias) | 5 dígitos | 1994 a 2022 (anos amostrados) |
| `faixa_hora_contrat` | FAIXA HORA CONTRAT | texto | Faixa de horas contratuais | código de 2 dígitos (planilha FAIXAS) | 1994 a 2022 (anos amostrados) |
| `qtd_hora_contr` | HORAS CONTR / Qtd Hora Contr | texto | Quantidade de horas contratuais por semana | horas por semana (com espaços à esquerda em alguns anos) | 1994 a 2022 (anos amostrados) |
| `idade` | IDADE | texto | Idade do trabalhador | anos (com espaços à esquerda em alguns anos) | 1994 a 2022 (anos amostrados) |
| `natureza_juridica` | NAT JURIDICA | texto | Natureza jurídica do estabelecimento (CONCLA/2002 desde a RAIS 2008) | 4 dígitos | 1994 a 2022 (anos amostrados) |
| `tipo_admissao` | TIPO ADM | texto | Tipo de admissão | 1 primeiro emprego, 2 reemprego, 3 e 4 transferência, 6 reintegração, 7 recondução, 8 reversão, 9 requisição, 10 a 14 serviço público; 0 não admitido no ano; -1 ignorado | 1994 a 2022 (anos amostrados) |
| `bairros_fortaleza` | BAIRRO FORT / Bairros Fortaleza | texto | Bairro do município de Fortaleza | código de 4 posições (planilha do layout) | 1996 a 2022 (anos amostrados) |
| `bairros_rj` | BAIRRO_RJ / Bairros RJ | texto | Bairro do município do Rio de Janeiro | código de 4 posições (planilha do layout) | 1996 a 2022 (anos amostrados) |
| `regioes_adm_df` | REG ADM DF / Regiões Adm DF | texto | Região Administrativa do Distrito Federal | código (planilha do layout) | 1996 a 2022 (anos amostrados) |
| `ind_cei_vinculado` | IND DE CEI VINC | texto | Indicador de CEI vinculado | 0 não, 1 sim | 1999 a 2022 (anos amostrados) |
| `vl_remun_dezembro_nom` | REM DEZ (R$) | texto | Remuneração de dezembro em valor nominal | R$, vírgula decimal | 1999 a 2022 (anos amostrados) |
| `vl_remun_media_nom` | REM MED (R$) | texto | Remuneração média do ano em valor nominal | R$, vírgula decimal | 1999 a 2022 (anos amostrados) |
| `ind_simples` | IND SIMPLES | texto | Indicador de estabelecimento optante pelo SIMPLES | 0 não, 1 sim | 2001 a 2022 (anos amostrados) |
| `causa_afastamento_1` | CAUS AFAST 1 | texto | Causa do primeiro afastamento do empregado no ano-base | rótulos abreviados do layout: 10 ACI TRB TIP, 20 ACI TRB TJT, 30 DOEN REL TR, 40 DOEN NREL TR, 50 LIC MATERNID, 60 SERV MILITAR, 70 LIC SEM VENC, 80 LIC COM VENC, 85 SUSP TEMP DE TRAB, 90 ACID DOENC TRAB, 99 SEM AFASTAMENTOS, -1 ignorado | 2002 a 2022 (anos amostrados) |
| `causa_afastamento_2` | CAUS AFAST 2 | texto | Causa do segundo afastamento no ano-base | mesmos códigos de `causa_afastamento_1` | 2002 a 2022 (anos amostrados) |
| `causa_afastamento_3` | CAUS AFAST 3 | texto | Causa do terceiro afastamento no ano-base | mesmos códigos de `causa_afastamento_1` | 2002 a 2022 (anos amostrados) |
| `mun_trab` | MUN TRAB | texto | Município onde o empregado trabalha ou presta serviço | código de 6 dígitos | 2002 a 2022 (anos amostrados) |
| `qtd_dias_afastamento` | QT DIAS AFAS | texto | Quantidade total de dias de afastamento no ano-base | dias | 2002 a 2022 (anos amostrados) |
| `cbo_ocupacao_2002` | CBO 2002 / CBO Ocupação 2002 | texto | Ocupação pela CBO 2002 | código de 6 posições | 2003 a 2022 (anos amostrados) |
| `cnae_2_0_classe` | CLAS CNAE 20 / CNAE 2.0 Classe | texto | Classe de atividade econômica pela CNAE 2.0 | 5 dígitos | 2004 a 2022 (anos amostrados) |
| `cnae_2_0_subclasse` | SB CLAS 20 / CNAE 2.0 Subclasse | texto | Subclasse de atividade econômica pela CNAE 2.0 | 7 dígitos | 2004 a 2022 (anos amostrados) |
| `escolaridade_apos_2005` | GR INSTRUCAO OU ESCOLARIDADE APÓS 2005 | texto | Grau de instrução (a partir da RAIS 2008) | 1 analfabeto a 11 doutorado; em 2018-norte o valor vem com espaços à esquerda | 2006 a 2022 (anos amostrados) |
| `raca_cor` | RACA_COR | texto | Raça e cor do trabalhador | 1 indígena, 2 branca, 4 preta, 6 amarela, 8 parda, 9 não identificado, -1 ignorado; 99 também aparece em 2018-norte (1,1 milhão de linhas) sem constar do layout lido | 2006 a 2022 (anos amostrados) |
| `ind_portador_defic` | PORT DEFIC | texto | Indicador de empregado portador de deficiência habilitado ou beneficiário reabilitado | 0 não, 1 sim | 2007 a 2022 (anos amostrados) |
| `tipo_defic` | TP DEFIC | texto | Tipo de deficiência ou beneficiário habilitado | 0 não deficiente, 1 física, 2 auditiva, 3 visual, 4 mental, 5 múltipla, 6 reabilitado, -1 ignorado | 2007 a 2022 (anos amostrados) |
| `vl_rem_janeiro_cc` | Vl Rem Janeiro CC | texto | Remuneração do trabalhador em janeiro (valor nominal), nos layouts 2015 a 2018 (sufixo CC) | R$, vírgula decimal | 2015 a 2018 (anos amostrados) |
| `vl_rem_fevereiro_cc` | Vl Rem Fevereiro CC | texto | Remuneração do trabalhador em fevereiro (valor nominal), nos layouts 2015 a 2018 (sufixo CC) | R$, vírgula decimal | 2015 a 2018 (anos amostrados) |
| `vl_rem_marco_cc` | Vl Rem Março CC | texto | Remuneração do trabalhador em março (valor nominal), nos layouts 2015 a 2018 (sufixo CC) | R$, vírgula decimal | 2015 a 2018 (anos amostrados) |
| `vl_rem_abril_cc` | Vl Rem Abril CC | texto | Remuneração do trabalhador em abril (valor nominal), nos layouts 2015 a 2018 (sufixo CC) | R$, vírgula decimal | 2015 a 2018 (anos amostrados) |
| `vl_rem_maio_cc` | Vl Rem Maio CC | texto | Remuneração do trabalhador em maio (valor nominal), nos layouts 2015 a 2018 (sufixo CC) | R$, vírgula decimal | 2015 a 2018 (anos amostrados) |
| `vl_rem_junho_cc` | Vl Rem Junho CC | texto | Remuneração do trabalhador em junho (valor nominal), nos layouts 2015 a 2018 (sufixo CC) | R$, vírgula decimal | 2015 a 2018 (anos amostrados) |
| `vl_rem_julho_cc` | Vl Rem Julho CC | texto | Remuneração do trabalhador em julho (valor nominal), nos layouts 2015 a 2018 (sufixo CC) | R$, vírgula decimal | 2015 a 2018 (anos amostrados) |
| `vl_rem_agosto_cc` | Vl Rem Agosto CC | texto | Remuneração do trabalhador em agosto (valor nominal), nos layouts 2015 a 2018 (sufixo CC) | R$, vírgula decimal | 2015 a 2018 (anos amostrados) |
| `vl_rem_setembro_cc` | Vl Rem Setembro CC | texto | Remuneração do trabalhador em setembro (valor nominal), nos layouts 2015 a 2018 (sufixo CC) | R$, vírgula decimal | 2015 a 2018 (anos amostrados) |
| `vl_rem_outubro_cc` | Vl Rem Outubro CC | texto | Remuneração do trabalhador em outubro (valor nominal), nos layouts 2015 a 2018 (sufixo CC) | R$, vírgula decimal | 2015 a 2018 (anos amostrados) |
| `vl_rem_novembro_cc` | Vl Rem Novembro CC | texto | Remuneração do trabalhador em novembro (valor nominal), nos layouts 2015 a 2018 (sufixo CC) | R$, vírgula decimal | 2015 a 2018 (anos amostrados) |
| `ano_chegada_brasil` | Ano Chegada Brasil | texto | Ano de chegada ao Brasil do trabalhador estrangeiro | AAAA; `0000` quando não se aplica | 2016 a 2022 (anos amostrados) |
| `ind_trab_parcial` | IND TRAB PARCIAL | texto | Indicador de contrato de trabalho parcial (a partir de 2017) | 0 não, 1 sim | 2017 a 2022 (anos amostrados) |
| `ind_trab_intermitente` | IND TRAB INTERMITENTE | texto | Indicador de contrato de trabalho intermitente (a partir de 2017) | 0 não, 1 sim | 2017 a 2022 (anos amostrados) |
| `tipo_salario` | Tipo Salário | texto | Tipo (unidade de pagamento) do salário contratual; o layout oficial lido não o descreve | 1 a 7 (em 2018-norte) | 2018 (anos amostrados) |
| `vl_salario_contratual` | Vl Salário Contratual | texto | Salário contratual; o layout oficial lido não o descreve | valor com ponto como separador de milhar e vírgula decimal (ex.: `9.973.858,20`) em 2018-norte | 2018 (anos amostrados) |
| `vl_rem_janeiro_sc` | Vl Rem Janeiro SC | texto | Remuneração do trabalhador em janeiro (valor nominal), nos layouts a partir de 2020 (sufixo SC; o layout oficial não explica a mudança de CC para SC) | R$, vírgula decimal | 2022 (anos amostrados) |
| `vl_rem_fevereiro_sc` | Vl Rem Fevereiro SC | texto | Remuneração do trabalhador em fevereiro (valor nominal), nos layouts a partir de 2020 (sufixo SC; o layout oficial não explica a mudança de CC para SC) | R$, vírgula decimal | 2022 (anos amostrados) |
| `vl_rem_marco_sc` | Vl Rem Março SC | texto | Remuneração do trabalhador em março (valor nominal), nos layouts a partir de 2020 (sufixo SC; o layout oficial não explica a mudança de CC para SC) | R$, vírgula decimal | 2022 (anos amostrados) |
| `vl_rem_abril_sc` | Vl Rem Abril SC | texto | Remuneração do trabalhador em abril (valor nominal), nos layouts a partir de 2020 (sufixo SC; o layout oficial não explica a mudança de CC para SC) | R$, vírgula decimal | 2022 (anos amostrados) |
| `vl_rem_maio_sc` | Vl Rem Maio SC | texto | Remuneração do trabalhador em maio (valor nominal), nos layouts a partir de 2020 (sufixo SC; o layout oficial não explica a mudança de CC para SC) | R$, vírgula decimal | 2022 (anos amostrados) |
| `vl_rem_junho_sc` | Vl Rem Junho SC | texto | Remuneração do trabalhador em junho (valor nominal), nos layouts a partir de 2020 (sufixo SC; o layout oficial não explica a mudança de CC para SC) | R$, vírgula decimal | 2022 (anos amostrados) |
| `vl_rem_julho_sc` | Vl Rem Julho SC | texto | Remuneração do trabalhador em julho (valor nominal), nos layouts a partir de 2020 (sufixo SC; o layout oficial não explica a mudança de CC para SC) | R$, vírgula decimal | 2022 (anos amostrados) |
| `vl_rem_agosto_sc` | Vl Rem Agosto SC | texto | Remuneração do trabalhador em agosto (valor nominal), nos layouts a partir de 2020 (sufixo SC; o layout oficial não explica a mudança de CC para SC) | R$, vírgula decimal | 2022 (anos amostrados) |
| `vl_rem_setembro_sc` | Vl Rem Setembro SC | texto | Remuneração do trabalhador em setembro (valor nominal), nos layouts a partir de 2020 (sufixo SC; o layout oficial não explica a mudança de CC para SC) | R$, vírgula decimal | 2022 (anos amostrados) |
| `vl_rem_outubro_sc` | Vl Rem Outubro SC | texto | Remuneração do trabalhador em outubro (valor nominal), nos layouts a partir de 2020 (sufixo SC; o layout oficial não explica a mudança de CC para SC) | R$, vírgula decimal | 2022 (anos amostrados) |
| `vl_rem_novembro_sc` | Vl Rem Novembro SC | texto | Remuneração do trabalhador em novembro (valor nominal), nos layouts a partir de 2020 (sufixo SC; o layout oficial não explica a mudança de CC para SC) | R$, vírgula decimal | 2022 (anos amostrados) |

## No lake

- Dataset `pdet/rais-vinculos`, particionado por `ano` e `regiao`: `ano=2017/regiao=ac/rais-vinculos_2017-ac.parquet`. Um Parquet por arquivo da fonte.
- A partição vem do **nome do arquivo**, não das linhas: `rais-vinculos_2025-sul@…7z` vira `ano=2025/regiao=sul`. `regiao` guarda a UF (1985 a 2017) ou a macrorregião (2018 em diante). Arquivo sem região vai para `regiao=__HIVE_DEFAULT_PARTITION__` (os 13 arquivos de UF ignorada, 1985 a 1997).
- `ano` é inteiro; `regiao` é texto.
- O conversor extrai o TXT do `.7z` com o programa `7z`, lê com o DuckDB (latin-1, separador `;`) e grava todas as colunas como texto. Se o cabeçalho não tem `tempo_emprego`, o arquivo é recusado. Se `;` não acha o cabeçalho, tenta com `,` (formato novo).
- Renomeação: cabeçalhos com sufixo ` - Código` (formato de 2023 em diante) perdem o sufixo, mas só para nomes que existem nos layouts antigos; os demais ficam com o sufixo ou com o nome novo (ver abaixo).
- O cabeçalho tem dois títulos iguais (`Tipo Estab`); o segundo vira `tipo_estab_1`.
- Chave: não há; o vínculo não tem identificador. Nulos: nenhum nas colunas da amostra (os campos vêm preenchidos com `0`, `-1` ou espaços).
- Linhas: contagem do lake igual à do TXT menos o cabeçalho nos 16 anos amostrados.

Colunas das republicações de 2023 em diante (formato que o conversor não lê hoje; nomes obtidos por conversão local do arquivo `2025-ni` com a extensão trocada), com a coluna equivalente nos layouts anteriores:

| Nome em 2023 em diante | Equivalente anterior |
|---|---|
| `ind_vinculo_ativo_31_12` | `vinculo_ativo_31_12` |
| `faixa_rem_media_sm` | `faixa_remun_media_sm` |
| `faixa_rem_dez_sm` | `faixa_remun_dezem_sm` |
| `ind_estabelecimento_participante_simples` | `ind_simples` |
| `municipio_trab` | `mun_trab` |
| `regiao_adm_df` | `regioes_adm_df` |
| `vl_rem_dezembro_nom` | `vl_remun_dezembro_nom` |
| `vl_rem_dezembro_sm` | `vl_remun_dezembro_sm` |
| `vl_rem_media_nom` | `vl_remun_media_nom` |
| `vl_rem_media_sm` | `vl_remun_media_sm` |
| `sexo` | `sexo_trabalhador` |
| `tipo_admissao_trabalhador` | `tipo_admissao` |
| `tipo_estabelecimento` | `tipo_estab` |
| `tipo_estabelecimento_nome` | `tipo_estab_1` |
| `tipo_deficiencia` | `tipo_defic` |
| `ind_trabalho_intermitente` | `ind_trab_intermitente` |
| `ind_trabalho_parcial` | `ind_trab_parcial` |
| `cbo_2002_ocupacao` | `cbo_ocupacao_2002` |
| `ind_vinculo_abandonado` | `— (nova)` |
| `categoria_trabalhador` | `— (nova)` |

As demais colunas de 2023 em diante têm os mesmos nomes dos layouts de 2022.

## Cuidados de uso

- **2023 a 2025 não estão no lake** (arquivos `.COMT` recusados pelo conversor). As séries de `rais-vinculos` do lake terminam em 2022. Evidência: ver Conteúdo.
- **Tudo é texto, sem normalização de formato.** O conversor não converte tipos nem tira zeros à esquerda ou espaços: o mesmo código aparece como `1`, `01` ou `000001`, e valores decimais usam vírgula (`0000686,80`). Converta e normalize antes de comparar anos.
- **O mesmo valor tem formatos diferentes entre anos.** Em 2018-norte, `sexo_trabalhador` e `escolaridade_apos_2005` vêm com 18 espaços à esquerda (`                  01`), em 2017 e 2022 não; `idade` e `qtd_hora_contr` têm espaços à esquerda em alguns anos. Aplique `trim` e converta para inteiro antes de juntar anos.
- **Mesmo conceito, nomes diferentes.** Nomes de coluna mudam com o layout (`cbo_ocupacao`, `cbo_94_ocupacao`, `cbo_ocupacao_2002`; `vl_rem_*_cc` e `vl_rem_*_sc`; `vl_salario_contratual` só em 2018). Faça a união por conceito, não por nome.
- **Duas colunas de tipo de estabelecimento.** `tipo_estab` (código) e `tipo_estab_1` (nome) vêm do mesmo título duplicado no arquivo.
- **Vínculos de UF ignorada.** 1985 a 1997 têm um arquivo à parte (`regiao=__HIVE_DEFAULT_PARTITION__`); somas por UF não os incluem.
- **Raça/cor `99`.** Em 2018-norte, 1.119.280 de 3.682.332 linhas têm `raca_cor` `99`, código que o layout lido não lista (conferido em 2026-11-10).
- **Contagem em 31/12.** Para estoque de empregos use `vinculo_ativo_31_12 = 1`; sem o filtro, o arquivo inclui vínculos encerrados antes do fim do ano.
- **Valores com ponto de milhar.** `vl_salario_contratual` em 2018-norte usa ponto de milhar e vírgula decimal, ao contrário das demais colunas.
- **Cautela com variáveis recentes ou muito desagregadas.** A Nota Técnica MTPS 088/12 avisa que Indicador de Pessoa com Deficiência, Tipo de Deficiência, Subclasse de atividade econômica, Ocupação e Raça/Cor devem ser usadas com cautela: algumas são recentes e, no nível de desagregação fino, podem ter inconsistências; em nível agregado o uso não fica comprometido.
- **Dado pessoal.** Veja o inventário no cabeçalho.

## Exemplos

Número de vínculos ativos em 31/12 por macrorregião em um ano de layout recente (colunas em texto, com `trim`):

```python
import duckdb

duckdb.sql("""
    SELECT regiao, count(*) AS vinculos_ativos
    FROM read_parquet('lake/pdet/rais-vinculos/ano=2018/**/*.parquet', hive_partitioning = true)
    WHERE trim(vinculo_ativo_31_12) = '1'
    GROUP BY regiao ORDER BY regiao
""").show()
```

## Referências

- Microdados RAIS e CAGED do PDET (FTP): ftp://ftp.mtps.gov.br/pdet/microdados/
- Comunicado MTPS 192/12: ftp://ftp.mtps.gov.br/pdet/microdados/COMUNICADO_microdados.pdf
- Nota Técnica MTPS 088/12: ftp://ftp.mtps.gov.br/pdet/microdados/NOTA_TECNICA_microdados.pdf
- Layouts da RAIS (`RAIS_vinculos_layout*.xls`), na pasta de documentação do mesmo FTP.
