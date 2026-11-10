---
dataset:
  id: "caged-2020-exc"
  fonte: "pdet"
  titulo: "Novo CAGED - movimentações excluídas (microdados)"
  resumo: "Movimentações do Novo CAGED excluídas (canceladas), de 2020-04 em diante, não identificadas."
  orgao: "Ministério do Trabalho e Emprego (MTE), Programa de Disseminação das Estatísticas do Trabalho (PDET)"
  pagina_oficial: "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/"
  licenca: "Não informada pela fonte. O Comunicado MTPS 192/12 e a Nota Técnica MTPS 088/12 (ftp://ftp.mtps.gov.br/pdet/microdados/) descrevem os microdados como não identificados e não declaram licença nem termo de uso"
  periodicidade: "mensal (por mês de divulgação)"
  cobertura: {inicio: "2020-04", fim: "em-curso"}
  unidade: "movimentação excluída (cancelamento de um evento já declarado)"
  granularidade: {espacial: "município do estabelecimento", temporal: "mês"}
  formato_bruto: "7z com um TXT por mês (CAGEDMOV, CAGEDFOR ou CAGEDEXC + AAAAMM), UTF-8, separador ponto e vírgula, decimal com vírgula"
  lake: {estado: convertido, particoes: [ano, mes], chave: [], colunas: 30}
  unidades_de_medida:
    salario: "R$"
    valorsalariofixo: "R$"
    saldomovimentacao: "movimentações (+1 admissão, -1 desligamento)"
    horascontratuais: "horas por semana"
  dados_pessoais:
  - coluna: "sexo"
    categoria: "sexo"
  - coluna: "racacor"
    categoria: "raça/cor"
  - coluna: "idade"
    categoria: "idade"
  - coluna: "tipodedeficiencia"
    categoria: "saúde (deficiência)"
  - coluna: "graudeinstrucao"
    categoria: "escolaridade"
  - coluna: "cbo2002ocupacao"
    categoria: "ocupação"
  - coluna: "categoria"
    categoria: "categoria do trabalhador (inclui aprendiz, doméstico, intermitente)"
  - coluna: "salario"
    categoria: "renda individual; idem valorsalariofixo"
  - coluna: "municipio"
    categoria: "localização (município do estabelecimento); idem uf e regiao"
  eras:
  - {desde: "2021-10", mudanca: "a metodologia de consolidação dos dados do eSocial, CAGED e Empregador Web é atualizada para captar mais movimentações (competência de outubro de 2021)"}
  revisado_em: "2026-11-10"
  versao_fetcher: "0.8.3"
---

# Novo CAGED - movimentações excluídas (microdados)

## Resumo

A exclusão cancela um evento ou movimentação já declarado (por exemplo, a empresa exclui em agosto uma admissão informada por engano em maio). A exclusão de uma admissão reduz o saldo e a de um desligamento o eleva.

São microdados individuais, mas a fonte os classifica como não identificados: não há nome, CPF nem PIS. As colunas abaixo descrevem pessoas e, combinadas, podem permitir reidentificação; trate como dado pessoal. Este repositório não reproduz valores individuais.

## Ficha técnica

<!-- gerado:ficha -->

| Campo | Valor |
|---|---|
| Dataset | `pdet/caged-2020-exc` |
| Órgão | Ministério do Trabalho e Emprego (MTE), Programa de Disseminação das Estatísticas do Trabalho (PDET) |
| Página oficial | ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/ |
| Licença | Não informada pela fonte. O Comunicado MTPS 192/12 e a Nota Técnica MTPS 088/12 (ftp://ftp.mtps.gov.br/pdet/microdados/) descrevem os microdados como não identificados e não declaram licença nem termo de uso |
| Periodicidade | mensal (por mês de divulgação) |
| Cobertura | 2020-04 a em-curso |
| Unidade (uma linha) | movimentação excluída (cancelamento de um evento já declarado) |
| Granularidade | espacial: município do estabelecimento; temporal: mês |
| Formato bruto | 7z com um TXT por mês (CAGEDMOV, CAGEDFOR ou CAGEDEXC + AAAAMM), UTF-8, separador ponto e vírgula, decimal com vírgula |
| Estado no lake | convertido |
| Partições | `ano`, `mes` |
| Chave | — |
| Colunas | 30 |
| Unidades de medida | `salario`: R$; `valorsalariofixo`: R$; `saldomovimentacao`: movimentações (+1 admissão, -1 desligamento); `horascontratuais`: horas por semana |
| Dados pessoais | `sexo` (sexo); `racacor` (raça/cor); `idade` (idade); `tipodedeficiencia` (saúde (deficiência)); `graudeinstrucao` (escolaridade); `cbo2002ocupacao` (ocupação); `categoria` (categoria do trabalhador (inclui aprendiz, doméstico, intermitente)); `salario` (renda individual; idem valorsalariofixo); `municipio` (localização (município do estabelecimento); idem uf e regiao) |
| Eras | 2021-10: a metodologia de consolidação dos dados do eSocial, CAGED e Empregador Web é atualizada para captar mais movimentações (competência de outubro de 2021) |
| Revisado em | 2026-11-10 |
| Versão do fetcher | 0.8.3 |

## Origem e coleta

- O fetcher baixa do FTP do PDET (`ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/`) e guarda em `caged-2020-exc/<ano>/caged-2020-exc_AAAAMM@AAAAMMDD.7z`; o lake converte a versão mais recente de cada mês.
- Conferido no bruto em 2026-11-10: 78 arquivos, uma versão cada, sem lacuna entre 2020-04 e 2026-09.
- Desde janeiro de 2020 o Caged foi substituído pelo eSocial para parte das empresas (Portaria SEPRT 1.127/2019); o Novo CAGED consolida eSocial, Caged e Empregador Web, com imputação de dados de outras fontes em partes do período (documento "Sobre o Novo CAGED"). Os microdados mensais publicados até a competência de setembro de 2021 foram movidos para uma pasta "Legado" no FTP; os arquivos baixados aqui são os da nova consolidação.
- Há três arquivos por competência: movimentações (`CAGEDMOV`), fora do prazo (`CAGEDFOR`) e excluídas (`CAGEDEXC`), cada um um dataset.

## Conteúdo

- Uma linha por movimentação excluída. Em 2020-04 há 780 linhas; em 2026-09, 4.161.
- Cobertura nominal: 2020-04 a 2026-09 (listagem do bruto), em curso.
- Amostra convertida: 2020, 2026; as contagens valem para os dois meses 2020-04 e 2026-09.
- Conferência de linhas: iguais às do TXT menos o cabeçalho nos arquivos convertidos.
- Competências do Novo CAGED não se somam ao CAGED estatístico: a série é nova.

## Variáveis

Nomes do cabeçalho do TXT, normalizados (sem acento e sem `_`); rótulos de código do layout oficial `Layout Não-identificado Novo Caged Movimentação.xlsx`. Todas as colunas são texto e todas existem em todos os meses amostrados.

| Coluna no lake | Na fonte | Tipo | Descrição | Unidade / códigos | Período |
|---|---|---|---|---|---|
| `competenciamov` | competênciamov | texto | Competência da movimentação | AAAAMM | todo |
| `regiao` | região | texto | Região geográfica (código IBGE) | 1 Norte, 2 Nordeste, 3 Sudeste, 4 Sul, 5 Centro-Oeste, 9 não identificado; `99` também aparece (938 linhas) | todo |
| `uf` | uf | texto | Unidade da Federação | código IBGE de 2 dígitos; 99 não identificado | todo |
| `municipio` | município | texto | Código do município | 6 dígitos (IBGE sem dígito verificador); 999999 não identificado | todo |
| `secao` | seção | texto | Seção da CNAE 2.0 | letra de A a U e Z (não identificado) | todo |
| `subclasse` | subclasse | texto | Subclasse da CNAE 2.0 | 7 dígitos; 9999999 não identificado | todo |
| `saldomovimentacao` | saldomovimentação | texto | Saldo da movimentação | 1 admissão, -1 desligamento | todo |
| `cbo2002ocupacao` | cbo2002ocupação | texto | Ocupação pela CBO 2002 | 6 dígitos; 999999 não identificado | todo |
| `categoria` | categoria | texto | Categoria do trabalhador | 101 empregado geral CLT, 102 rural por pequeno prazo, 103 aprendiz, 104 doméstico, 105 contrato a termo (Lei 9.601), 106 temporário (Lei 6.019), 107 e 108 Verde e Amarelo, 111 intermitente, 999 não identificado | todo |
| `graudeinstrucao` | graudeinstrução | texto | Grau de instrução | 1 analfabeto, 2 até 5ª incompleto, 3 5ª completo, 4 6ª a 9ª, 5 fundamental completo, 6 médio incompleto, 7 médio completo, 8 superior incompleto, 9 superior completo, 10 mestrado, 11 doutorado, 80 pós-graduação completa, 99 não identificado | todo |
| `idade` | idade | texto | Idade do trabalhador | anos | todo |
| `horascontratuais` | horascontratuais | texto | Horas contratuais semanais | horas, vírgula decimal (`,01`) | todo |
| `racacor` | raçacor | texto | Raça ou cor | 1 branca, 2 preta, 3 parda, 4 amarela, 5 indígena, 6 não informada, 9 não identificado | todo |
| `sexo` | sexo | texto | Sexo do trabalhador | 1 homem, 3 mulher, 9 não identificado | todo |
| `tipoempregador` | tipoempregador | texto | Tipo de empregador | 0 CNPJ raiz, 2 CPF, 9 não identificado | todo |
| `tipoestabelecimento` | tipoestabelecimento | texto | Tipo de estabelecimento | 1 CNPJ, 3 CAEPF, 4 CNO, 5 CEI, 9 não identificado | todo |
| `tipomovimentacao` | tipomovimentação | texto | Tipo de movimentação | admissões: 10 primeiro emprego, 20 reemprego, 25 prazo determinado, 35 reintegração, 70 transferência, 97 tipo ignorado; desligamentos: 31 sem justa causa, 32 com justa causa, 33 culpa recíproca, 40 a pedido, 43 término de prazo determinado, 45 término de contrato, 50 aposentadoria, 60 morte, 80 transferência, 90 acordo, 98 tipo ignorado; 99 não identificado | todo |
| `tipodedeficiencia` | tipodedeficiência | texto | Tipo de deficiência | 0 não deficiente, 1 física, 2 auditiva, 3 visual, 4 intelectual, 5 múltipla, 6 reabilitado, 9 não identificado | todo |
| `indtrabintermitente` | indtrabintermitente | texto | Indicador de trabalhador intermitente | 0 não, 1 sim, 9 não identificado | todo |
| `indtrabparcial` | indtrabparcial | texto | Indicador de trabalhador parcial | 0 não, 1 sim, 9 não identificado | todo |
| `salario` | salário | texto | Salário mensal declarado | R$, vírgula decimal (`,00` a `9999,90`) | todo |
| `tamestabjan` | tamestabjan | texto | Faixa de emprego do estabelecimento no início de janeiro | 1 zero, 2 de 1 a 4, 3 de 5 a 9, 4 de 10 a 19, 5 de 20 a 49, 6 de 50 a 99, 7 de 100 a 249, 8 de 250 a 499, 9 de 500 a 999, 10 mil ou mais, 90 não informado, 97 não se aplica, 98 inválido, 99 ignorado | todo |
| `indicadoraprendiz` | indicadoraprendiz | texto | Indicador de trabalhador aprendiz | 0 não, 1 sim, 9 não identificado | todo |
| `origemdainformacao` | origemdainformação | texto | Origem da informação da movimentação | 1 eSocial, 2 CAGED, 3 Empregador Web | todo |
| `competenciadec` | competênciadec | texto | Competência da declaração | AAAAMM | todo |
| `competenciaexc` | competênciaexc | texto | Competência da exclusão (só no arquivo de exclusões) | AAAAMM | todo |
| `indicadordeexclusao` | indicadordeexclusão | texto | Indicador de exclusão (só no arquivo de exclusões) | 0 não, 1 sim | todo |
| `indicadordeforadoprazo` | indicadordeforadoprazo | texto | Indicador de informação declarada fora do prazo | 0 não, 1 sim | todo |
| `unidadesalariocodigo` | unidadesaláriocódigo | texto | Unidade de pagamento da parte fixa da remuneração | 1 hora, 2 dia, 3 semana, 4 quinzena, 5 mês, 6 tarefa, 7 variável, 99 não identificado | todo |
| `valorsalariofixo` | valorsaláriofixo | texto | Salário base, correspondente à parte fixa da remuneração | R$, vírgula decimal; vazio quando não declarado | todo |

## No lake

- Dataset `pdet/caged-2020-exc`, particionado por `ano` e `mes`, com o mesmo tratamento de `caged-2020-mov` (UTF-8, `;`, tudo texto, partição pelo nome do arquivo).
- Chave: não há. Linhas: iguais às do TXT menos o cabeçalho.

## Cuidados de uso

- **Tudo é texto, sem normalização de formato.** O conversor não converte tipos nem tira zeros à esquerda ou espaços: o mesmo código aparece como `1`, `01` ou `000001`, e valores decimais usam vírgula (`0000686,80`). Converta e normalize antes de comparar anos.
- **Mês de divulgação, não de ocorrência.** Os arquivos seguem o mês da divulgação; `competenciamov` é o mês da movimentação e `competenciadec`, o da declaração. Separe os dois.
- **Os três arquivos se combinam.** O saldo oficial de um mês soma as movimentações dentro do prazo (`mov`), as fora do prazo (`for`) e subtrai as excluídas (`exc`): a exclusão de uma admissão reduz o saldo e a de um desligamento o eleva (Leia-me do FTP). Não use só um deles.
- **Faixas “ignorado”.** Em `caged-2020-mov` (amostra de 2020-01 e 2026-09), `tamestabjan` é `99` (ignorado) em 63% das linhas e `regiao` `99` em 938 linhas.
- **Sexo codificado diferente do CAGED antigo.** Aqui `1` homem e `3` mulher (e `9`); no CAGED estatístico `1` e `2`.
- **Salário.** `salario` e `valorsalariofixo` são texto com vírgula; o valor `,00` e a unidade de pagamento (`unidadesalariocodigo`) importam antes de comparar salários.
- **Imputação e mudanças metodológicas.** Os dados de 2020 a 2021 incluem imputação de desligamentos não declarados ao eSocial; a metodologia mudou em outubro de 2021 (documento "Sobre o Novo CAGED").
- **Cautela com variáveis recentes ou muito desagregadas.** A Nota Técnica MTPS 088/12 avisa que Indicador de Pessoa com Deficiência, Tipo de Deficiência, Subclasse de atividade econômica, Ocupação e Raça/Cor devem ser usadas com cautela: algumas são recentes e, no nível de desagregação fino, podem ter inconsistências; em nível agregado o uso não fica comprometido.
- **Dado pessoal.** Veja o inventário no cabeçalho.

## Exemplos

Saldo das movimentações excluídas por UF em setembro de 2026 (a exclusão tem efeito inverso no saldo oficial):

```python
import duckdb

duckdb.sql("""
    SELECT uf, sum(CAST(saldomovimentacao AS INTEGER)) AS saldo
    FROM read_parquet('lake/pdet/caged-2020-exc/ano=2026/mes=09/*.parquet')
    GROUP BY uf ORDER BY saldo DESC
""").show()
```

## Referências

- Microdados do PDET (FTP): ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/
- Sobre o Novo CAGED e Leia-me, na pasta de documentação do FTP.
- Layout dos microdados não identificados do Novo CAGED (`Layout Não-identificado Novo Caged Movimentação.xlsx`).
