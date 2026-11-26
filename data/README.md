# Dados de entrada

O projeto usa as nove entradas completas abaixo. Os arquivos da Vale foram fornecidos no desafio de Análise Avançada de Dados do Programa Desenvolver 2026. Os caminhos na tabela são relativos à raiz do repositório.

| Entrada | Conteúdo e papel no pipeline |
| --- | --- |
| `data/telemetria/telemetry_jan.parquet` | Eventos de telemetria de janeiro de 2025. |
| `data/telemetria/telemetry_feb.parquet` | Eventos de telemetria de fevereiro de 2025. |
| `data/telemetria/telemetry_mar.parquet` | Eventos de telemetria de março de 2025. |
| `data/telemetria/telemetry_abr.parquet` | Eventos de telemetria de abril de 2025. |
| `data/telemetria/telemetry_may.parquet` | Eventos de telemetria de maio de 2025. |
| `data/telemetria/telemetry_jun.parquet` | Eventos de telemetria de junho de 2025. |
| `data/apontamentos/desenvolver_apontamentos.parquet` | Apontamentos operacionais usados para identificar ocorrências e compor episódios de referência. |
| `docs/business/Alarmes - Regra de Negocio_V2.xlsx` | Regras de negócio dos alarmes usadas na classificação de eventos. A entrada utiliza a aba `CMA`. |
| `docs/business/Dicionario_Dados.xlsx` | Definições dos campos documentados nas abas `Apontamentos` e `Telemetria`. |

As duas planilhas tiveram os campos de autoria e última edição removidos. As abas e as células foram preservadas.

## Verificação local

O [manifesto das entradas](../fleet_monitoring/manifests/inputs_v1.json) fixa os caminhos, tamanhos, contagens e hashes SHA-256. Na raiz do clone, depois de preparar o ambiente conforme o [guia de reprodução](../docs/reproducao.md), execute a verificação.

```bash
uv run --frozen python -m fleet_monitoring.src.input_manifest --manifest fleet_monitoring/manifests/inputs_v1.json --repo-root .
```

O comando deve terminar com `INPUT MANIFEST: PASS (36 checks)`. Se houver falha, confira se os arquivos do clone correspondem à revisão do manifesto e restabeleça a cópia correspondente antes de ingerir os dados. O fuso de origem permanece sem confirmação no manifesto. Preserve a interpretação temporal do pipeline ao reproduzir os resultados.

O manifesto `1.0.1` registra as planilhas preparadas para publicação e mantém o contrato `1.0.0`. Se o workspace já contém tabelas Bronze geradas com o manifesto `1.0.0`, configure um novo `FLEET_BRONZE_SCHEMA` e use schemas próprios para as demais camadas da nova instalação. A ingestão só reutiliza tabelas Bronze materializadas com o mesmo manifesto.

## Atribuição e uso

Atribua a base e os materiais de referência à Vale, Programa Desenvolver 2026, tema Análise Avançada de Dados. O uso desses materiais segue as condições do programa. A licença MIT aplica-se ao código-fonte original do projeto. [Aviso de autoria e origem](../NOTICE.md) · [Programa Desenvolver Vale](https://vale.com/pt/programa-desenvolver)
