# Microdados do PDET (`pdet`)

O `pdet-fetcher` coleta os microdados da RAIS e do CAGED publicados pelo Programa de Disseminação das Estatísticas do Trabalho (PDET) do Ministério do Trabalho e Emprego e os converte para o lake em Parquet. Os microdados vêm do FTP do PDET (`ftp://ftp.mtps.gov.br/pdet/microdados/`) e são descritos pelo próprio PDET como **não identificados**: não há nome, CPF nem PIS, mas o perfil individual (sexo, idade, raça/cor, deficiência, remuneração) está presente e deve ser tratado como dado pessoal.

| Dataset | O que é | Origem (pasta do FTP) | Período | Página |
|---|---|---|---|---|
| `rais-vinculos` | Vínculos empregatícios do ano-base | `RAIS/` | 1985 a 2025 (lake até 2022) | [rais-vinculos](datasets/rais-vinculos.md) |
| `rais-estabelecimentos` | Estabelecimentos do ano-base e o estoque de vínculos | `RAIS/` | 1985 a 2025 (lake até 2022) | [rais-estabelecimentos](datasets/rais-estabelecimentos.md) |
| `caged` | CAGED estatístico: admissões e desligamentos | `CAGED/` | 2007-01 a 2019-12 | [caged](datasets/caged.md) |
| `caged-ajustes` | Declarações do CAGED fora do prazo | `CAGED_AJUSTES/` | 2002 a 2019-12 | [caged-ajustes](datasets/caged-ajustes.md) |
| `caged-2020-mov` | Novo CAGED, movimentações dentro do prazo | `NOVO CAGED/` | 2020-01 em diante | [caged-2020-mov](datasets/caged-2020-mov.md) |
| `caged-2020-for` | Novo CAGED, movimentações fora do prazo | `NOVO CAGED/` | 2020-02 em diante | [caged-2020-for](datasets/caged-2020-for.md) |
| `caged-2020-exc` | Novo CAGED, movimentações excluídas | `NOVO CAGED/` | 2020-04 em diante | [caged-2020-exc](datasets/caged-2020-exc.md) |

## Como o PDET publica

- Os arquivos ficam em um FTP, em `.7z` ou `.zip` com um TXT dentro. A RAIS sai por ano (e, por UF até 2017 e por macrorregião de 2018 em diante); o CAGED, por mês. O PDET guarda ao lado a documentação (layouts em XLS, notas técnicas, comunicados).
- A fonte republica arquivos de anos antigos (por exemplo, `rais-vinculos_1986-sp` teve republicações em 2026). O fetcher guarda cada versão com a data de coleta no nome (`<arquivo>@AAAAMMDD`) e o lake converte a mais recente.
- Desde 2020, o Novo CAGED (eSocial) substitui o CAGED estatístico; as duas séries têm metodologias diferentes.
- A pasta do FTP tem os avisos do PDET (Comunicado MTPS 192/12 e Nota Técnica MTPS 088/12): os microdados são “não identificados”, e variáveis como deficiência, subclasse de atividade, ocupação e raça/cor devem ser usadas com cautela. Nenhum dos dois documentos declara uma licença ou termo de uso; a licença está registrada como “não informada pela fonte” em todos os datasets.

## Como os conjuntos se relacionam

- **CAGED × RAIS.** O CAGED conta movimentações (fluxo); a RAIS conta vínculos do ano (estoque em 31/12 em `vinculo_ativo_31_12`). Os totais não coincidem.
- **CAGED × ajustes.** O saldo do mês em `caged` não inclui as declarações fora do prazo, que estão em `caged-ajustes`.
- **CAGED estatístico × Novo CAGED.** O Novo CAGED (2020 em diante) é outra série; use os três arquivos (`mov`, `for`, `exc`) juntos para o saldo.
- **RAIS vínculos × estabelecimentos.** `rais-estabelecimentos` traz o estoque de vínculos ativos em 31/12 por estabelecimento; `rais-vinculos` traz cada vínculo.

## Quebras que valem para mais de um conjunto

| Desde | Mudança | Afeta |
|---|---|---|
| 2018 | Os arquivos da RAIS passam de UF para macrorregião | `rais-vinculos` |
| 2020-01 | O CAGED estatístico é substituído pelo Novo CAGED | `caged`, `caged-2020-*` |
| 2023 | As republicações da RAIS passam a CSV com vírgula e extensão `.COMT`, que o conversor não lê: 2023 a 2025 não estão no lake | `rais-vinculos`, `rais-estabelecimentos` |

Os códigos categóricos (sexo, raça/cor, escolaridade) não são os mesmos entre RAIS, CAGED estatístico e Novo CAGED; consulte o dicionário de cada página.
