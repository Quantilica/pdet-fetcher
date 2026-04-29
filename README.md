# 📊 pdet-data

> Ferramenta para coletar, processar e analisar microdados de emprego e trabalho do Brasil diretamente da **PDET** (Plataforma de Disseminação de Estatísticas do Trabalho).

Acesse dados do **RAIS** e **CAGED** de forma programática, com conversão automática de tipos, tratamento de anomalias e exportação para formatos modernos como Parquet.

---

## ⚡ O que é a PDET?

A **PDET** é a plataforma oficial do Ministério do Trabalho e Previdência Social (MTPS) que disponibiliza os microdados de trabalho do Brasil. Ela mantém as bases:

- 📋 **RAIS** (Relação Anual de Informações Sociais): Dados de todos os vínculos de emprego formal registrados anualmente
- 💼 **CAGED** (Cadastro Geral de Empregados e Desempregados): Movimentações mensais de emprego (admissões, demissões, etc.)

Esses microdados são essenciais para análises de mercado de trabalho, pesquisa acadêmica e inteligência de negócios.

---

## 🎯 Por que usar `pdet-data`?

### Desafios com os dados brutos

Os microdados da PDET vêm em formatos legados (7z, ZIP) com características específicas:

- Colunas com tipos de dados mal definidos (números com espaços e pontos)
- Arquivos CSV "ragged" com número inconsistente de colunas
- Codificações de caracteres variáveis
- Nomes e estruturas de colunas diferentes por ano e dataset

### O que essa ferramenta oferece

✅ **Acesso direto via FTP**: Conecta automaticamente ao servidor da PDET e baixa todos os arquivos  
✅ **Leitura inteligente**: Detecta formato, codificação e estrutura automaticamente  
✅ **Conversão de tipos**: Transforma strings em números, booleanos e categóricas  
✅ **Correção de dados**: Fixa CSVs ragged, trata valores faltantes  
✅ **Performance**: Usa [Polars](https://pola.rs/) para processamento rápido  
✅ **Sem dependências pesadas**: Apenas `polars` e `tqdm`

---

## 📦 Instalação

```bash
pip install pdet-data
```

Ou para desenvolvimento:

```bash
# Clone o repositório
git clone https://github.com/dankkom/pdet-data.git
cd pdet-data

# Instale em modo editável
pip install -e .
```

**Requisitos**: Python 3.10+

---

## 🚀 Uso Rápido

### 1️⃣ Baixar todos os microdados

```python
from pathlib import Path
from pdet_data import fetch

# Conecta ao FTP do MTPS
ftp = fetch.connect()

# Baixa RAIS, CAGED e documentação
fetch.fetch_rais(ftp=ftp, dest_dir=Path("./dados"))
fetch.fetch_caged(ftp=ftp, dest_dir=Path("./dados"))
fetch.fetch_caged_2020(ftp=ftp, dest_dir=Path("./dados"))

ftp.close()
```

Ou pela linha de comando:

```bash
python -m pdet_data run -data-dir ./dados
```

### 2️⃣ Ler dados RAIS

```python
from pathlib import Path
import polars as pl
from pdet_data.reader import read_rais

# Ler vínculos de emprego (ano 2023)
df = read_rais(
    filepath=Path("dados/rais_2023_vinculos.csv"),
    year=2023,
    dataset="vinculos"
)

print(df.schema)
print(df.head())

# Explorar: Qual é o setor com mais empregados?
top_setores = df.group_by("cnae_setor").agg(
    pl.col("id_vinculo").count().alias("n_vinculos")
).sort("n_vinculos", descending=True).head(10)

print(top_setores)
```

### 3️⃣ Ler dados CAGED

```python
from pathlib import Path
import polars as pl
from pdet_data.reader import read_caged, read_caged_2020

# CAGED clássico (até 2019)
df_caged = read_caged(Path("dados/caged_202012.csv"))

# CAGED 2020+ (novo formato)
df_mov = read_caged_2020(Path("dados/caged_mov_202401.csv"))

# Analisar: saldo de emprego por UF
saldo = df_mov.with_columns(
    saldo = pl.col("admissoes") - pl.col("demissoes")
).group_by("uf").agg(
    pl.col("saldo").sum()
).sort("saldo", descending=True)

print(saldo)
```

### 4️⃣ Processar em lote

```python
from pathlib import Path
from pdet_data.reader import read_rais
import polars as pl

# Ler múltiplos anos de RAIS
anos = [2020, 2021, 2022, 2023]
dfs = []

for ano in anos:
    df = read_rais(
        filepath=Path(f"dados/rais_{ano}_vinculos.csv"),
        year=ano,
        dataset="vinculos"
    )
    dfs.append(df)

# Concatenar e analisar série temporal
df_completo = pl.concat(dfs)

evolucao = df_completo.group_by("ano").agg(
    total_vinculos=pl.col("id_vinculo").count()
).sort("ano")

print(evolucao)
```

---

## 📚 Dados Disponíveis

### RAIS (Relação Anual de Informações Sociais)

**O que é**: Base de referência com todos os vínculos de emprego formal do Brasil, declarados anualmente pelos empregadores.

**Frequência**: Anual (dezembro de cada ano)

**Datasets**:

- **Vínculos**: Informações sobre cada relação de emprego (salário, ocupação, setor, etc.)
- **Estabelecimentos**: Dados das empresas/órgãos (CNPJ, endereço, setor econômico)

**Períodos**: 1985 até o presente

**Casos de uso**:

- Análise de renda por região, setor e ocupação
- Estudos de empregabilidade
- Pesquisa sobre desigualdade de gênero/raça no trabalho formal
- Análise de dinâmica de empresas

---

### CAGED (Cadastro Geral de Empregados e Desempregados)

**O que é**: Registro de fluxo mensal de emprego, com informações sobre admissões e demissões declaradas pelos empregadores.

**Frequência**: Mensal

**Datasets** (versão clássica, até dezembro/2019):

- **Ajustes**: Correções de dados fora do prazo
- **Movimentações**: Admissões, demissões e outras movimentações

**Períodos**: 1985 até dezembro de 2019

**Caso de uso**:
- Indicadores conjunturais de emprego (saldo de vagas)
- Análise de flutuações cíclicas do mercado de trabalho
- Monitoramento em tempo real da economia

---

### CAGED 2020 (Novo CAGED)

**O que é**: Nova versão do CAGED com estrutura de dados modernizada, implementada a partir de janeiro/2020.

**Frequência**: Mensal

**Datasets**:

- **Movimentações**: Admissões, demissões, movimentações declaradas dentro do prazo
- **Fora do Prazo**: Movimentações declaradas depois do período de referência
- **Exclusões**: Cancelamento de movimentações (correções feitas retroativamente)

**Períodos**: Janeiro de 2020 até o presente

**Melhorias em relação ao CAGED clássico**:

- Estrutura de dados mais consistente
- Melhor tratamento de correções (exclusões separadas)
- Informações mais detalhadas sobre ocupações

**Caso de uso**:

- Mesmas análises conjunturais da versão anterior
- Análises mais granulares com a nova estrutura

---

## 🏗️ Arquitetura

```
pdet_data/
├── fetch.py           # Conexão FTP e download de arquivos
├── reader.py          # Leitura e processamento de CSVs
├── storage.py         # Gerenciamento de paths e locais de armazenamento
├── constants.py       # Nomes de colunas, valores faltantes, tipos por ano
├── meta.py            # Metadados dos datasets
└── __init__.py
```

### Fluxo de dados

```
FTP (PDET)
    ↓
download (.7z, .zip)
    ↓
extração (.csv)
    ↓
detecção automática (ano, dataset, encoding)
    ↓
leitura com Polars (com fallback para CSV ragged)
    ↓
conversão de tipos (INT, FLOAT, BOOL, CATEGORICAL)
    ↓
DataFrame pronto para análise
```

---

## 🔧 API de Funções Principais

### `fetch.connect()`

Conecta ao servidor FTP da PDET.

```python
ftp = fetch.connect()
```

### `fetch.fetch_rais(ftp, dest_dir)`

Baixa todos os arquivos RAIS disponíveis.

**Parâmetros**:

- `ftp`: Conexão FTP
- `dest_dir`: Diretório de destino

### `fetch.fetch_caged(ftp, dest_dir)` 

Baixa CAGED clássico (até dez/2019).

### `fetch.fetch_caged_2020(ftp, dest_dir)`

Baixa CAGED 2020+ (jan/2020 em diante).

### `read_rais(filepath, year, dataset, **kwargs)`

Lê arquivo RAIS e retorna DataFrame Polars.

**Parâmetros**:

- `filepath`: Path para arquivo CSV
- `year`: Ano dos dados
- `dataset`: `"vinculos"` ou `"estabelecimentos"`

### `read_caged(filepath, **kwargs)`

Lê CAGED clássico.

### `read_caged_2020(filepath, **kwargs)`

Lê CAGED 2020 (movimentações, fora do prazo ou exclusões).

---

## 📊 Exemplos de Análise

### 1. Qual setor criou mais empregos em 2023?

```python
import polars as pl
from pdet_data.reader import read_rais

df = read_rais(Path("rais_2023.csv"), year=2023, dataset="vinculos")

top_setores = df.group_by("cnae_secao").agg(
    pl.col("id_vinculo").count().alias("empregos")
).sort("empregos", descending=True)

print(top_setores)
```

### 2. Evolução do emprego nos últimos 5 anos

```python
import polars as pl
from pdet_data.reader import read_rais
from pathlib import Path

df = pl.concat([
    read_rais(Path(f"rais_{y}.csv"), year=y, dataset="vinculos").with_columns(
        ano=pl.lit(y)
    )
    for y in range(2019, 2024)
])

evolucao = df.group_by("ano").agg(
    empregos=pl.col("id_vinculo").count(),
    salario_medio=pl.col("vl_remun_medio_nominal").mean()
)

print(evolucao.sort("ano"))
```

### 3. Desempenho de mercado em tempo real (CAGED 2020)

```python
import polars as pl
from pdet_data.reader import read_caged_2020
from pathlib import Path
from datetime import datetime

# Ler últimos 12 meses
arquivos = sorted(Path("dados").glob("caged_mov_*.csv"))[-12:]

df = pl.concat([read_caged_2020(f) for f in arquivos])

saldo_mensal = df.with_columns(
    saldo=(pl.col("admissoes") - pl.col("demissoes"))
).group_by("competencia").agg(
    saldo_total=pl.col("saldo").sum()
).sort("competencia")

print(saldo_mensal)
```

### 4. Diferenças salariais por gênero e setor

```python
import polars as pl
from pdet_data.reader import read_rais

df = read_rais(Path("rais_2023.csv"), year=2023, dataset="vinculos")

diferenca = df.group_by(["cnae_secao", "ind_sexo_trabalhador"]).agg(
    salario_medio=pl.col("vl_remun_medio_nominal").mean(),
    qtd_pessoas=pl.col("id_vinculo").count()
).sort(["cnae_secao", "ind_sexo_trabalhador"])

print(diferenca)
```

---

## 🐛 Tratamento de Dados

A ferramenta detecta e corrige automaticamente:

- ✅ **Valores faltantes**: Diferentes representações (espaços, pontos, valores nulos)
- ✅ **Formato de números**: Remove espaçamento e converte separadores decimais
- ✅ **Problemas de encoding**: Suporta latin-1 e utf-8
- ✅ **CSVs ragged**: Fixa linhas com número inconsistente de colunas
- ✅ **Tipos de dados**: Converte strings em INT64, FLOAT64, BOOLEAN conforme necessário

---

## 📈 Performance

Para arquivos RAIS completos (>10GB com múltiplos anos):

| Operação | Tempo | Memória |
|----------|-------|---------|
| Download | ~5-15 min | - |
| Leitura (1 arquivo) | ~10s | ~2GB |
| Agregação simples | <1s | - |

*Especificações: processador moderno, 16GB RAM*

---

## 🤝 Contribuindo

Encontrou um bug? Quer adicionar suporte para novos datasets? Abra uma [issue](https://github.com/dankkom/pdet-data/issues) ou envie um PR!

**Áreas para contribuição**:

- Suporte para novos datasets da PDET
- Otimizações de performance
- Documentação e exemplos de análise
- Tratamento de casos extremos nos dados

---

## 📖 Referências

- **PDET**: [pdet.mte.gov.br](http://pdet.mte.gov.br/microdados-rais-e-caged)
- **RAIS**: [Documentação Oficial](http://pdet.mte.gov.br/rais)
- **CAGED**: [Documentação Oficial](http://pdet.mte.gov.br/caged)
- **Novo CAGED (2020+)**: [Documentação Oficial](http://pdet.mte.gov.br/novo-caged)

---

## 📝 Licença

MIT

---

## 👤 Autor

Daniel Komesu ([github](https://github.com/dankkom))

---

**Última atualização**: Abril de 2026
