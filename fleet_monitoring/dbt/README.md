# Transformações dbt

A Bronze é produzida pela ingestão PySpark. O dbt lê essas tabelas via
`source()` e materializa Silver e Gold no catálogo da instalação.

| Pasta | Schema padrão | Papel |
|---|---|---|
| `models/staging` | `fleet_silver` | Tipos, chaves e qualidade por fonte |
| `models/intermediate` | `fleet_silver` | Ocorrências, episódios, população e janelas |
| `models/marts` | `fleet_gold` | Features, treino, scoring e medidas |

Use `source()` para entradas e `ref()` para relações do projeto. O macro
`generate_schema_name` limita as escritas aos schemas Silver e Gold
configurados na instalação. O [guia de reprodução](../../docs/reproducao.md)
tem os comandos de configuração, build e teste por etapa, inclusive os
modelos que dependem dos resultados de MLflow.
