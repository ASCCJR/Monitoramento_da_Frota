# Publicar um recorte histórico

O dashboard e o Genie leem as avaliações do scoring em lote e os registros
disponíveis na hora de referência. O caso apresentado usa **15/06/2025 às
10h**. A publicação abaixo é manual, em um schema próprio, e mantém a mesma
referência até a próxima publicação deliberada.

## Pré-requisitos

- O scoring de junho de 2025 terminou e retornou `scoring_run_id`. A etapa
  dbt de curadoria do scoring materializou `fct_operational_scores`.
- O schema Silver oferece `int_scoring_grade`, `stg_telemetria` e
  `int_cma_evento_lookup`. O schema Gold oferece `fct_features_scoring`
  e `fct_operational_scores`.
- Tenha um perfil OAuth no Databricks CLI para o seu workspace e um
  SQL warehouse com Unity Catalog, Delta e suporte a SQL scripting.
  Quem executa precisa usar esse warehouse, consultar Silver e Gold e criar
  o schema, as tabelas e as views de produto. Os consumidores precisam das
  permissões de leitura nesse schema e do acesso aos recursos do workspace.

## Preparar e executar o SQL

Defina `FLEET_CATALOG` com o nome do seu catálogo. Os schemas padrão são
`fleet_silver`, `fleet_gold` e `fleet_product`. Para outros nomes, defina
`FLEET_SILVER_SCHEMA`, `FLEET_GOLD_SCHEMA` e `FLEET_PRODUCT_SCHEMA` antes
de renderizar. Use os mesmos catálogo e schemas do pipeline.

Na mesma sessão do [guia de reprodução](../../docs/reproducao.md), defina
`SCORING_RUN_ID` com o valor retornado pelo scoring e `RENDERED_DIR` com
um diretório de trabalho fora do Git. Na raiz do projeto, rode:

```bash
export SCORING_RUN_ID='id_retornado_pelo_scoring'
export RENDERED_DIR='outputs/consumo'

uv run python fleet_monitoring/scripts/render_databricks_consumer.py \
  --scoring-run-id "$SCORING_RUN_ID" --output-dir "$RENDERED_DIR"

for sql_file in "$RENDERED_DIR"/0[1-7]_*.sql; do
  databricks experimental aitools tools query \
    --profile "$DATABRICKS_CONFIG_PROFILE" --warehouse "$DATABRICKS_WAREHOUSE_ID" \
    --file "$sql_file" || exit 1
done
```

A ordem dos arquivos é o contrato de instalação.

| Arquivo | Saída |
|---|---|
| `01_bootstrap.sql` | Seleciona o lote de scoring, prepara as tabelas e cria as views de relógio, frota, histórico de 24h e avisos. |
| `02_publish_snapshot.sql` | Confere chaves, scores, horas e janelas. Grava as avaliações e publica a referência após as verificações. |
| `03_equipment_events.sql` | Disponibiliza os registros das quatro horas anteriores para cada equipamento. |
| `04_fleet_investigation.sql` | Define a fila de investigação e o registro prioritário para conferência. |
| `05_fleet_exploration_history.sql` | Expõe avaliações desde o início de junho até a referência publicada. |
| `06_equipment_event_history.sql` | Expõe os registros para filtros de equipamento e período. |
| `07_validate_consumption.sql` | Verifica o recorte e o grão e apresenta os quatro indicadores de Acompanhamento. |

O identificador da publicação deriva do lote de scoring e da hora de
referência. Ao repetir a mesma publicação, a chave composta por recorte,
equipamento e hora mantém o conjunto único. O relógio de publicação é
preservado. Execute uma publicação por vez em cada schema.

É possível usar `--reference-time` para escolher outra hora cheia, sem fuso,
dentro de junho de 2025 e coberta pelo lote de scoring. O histórico da
publicação começa em 01/06/2025 à 00h. A hora faz parte do dado histórico.
O horário UTC de publicação descreve a execução no seu workspace.

Compare sua execução com o recorte de referência, que mostrou **29 caminhões,
dois sinais para investigar, um equipamento com ocorrência registrada e sete
com dados a verificar**. Na avaliação histórica, os dois candidatos tinham
sete e quatro registros de nível do líquido de arrefecimento e continuavam
prioritários às 10h sem novo aviso.

## Interfaces de leitura

Crie o [Genie](../genie/README.md) e o [dashboard](../dashboards/README.md) no
mesmo workspace depois de validar o SQL. O dashboard pode ser configurado
com o Genie do leitor. A equivalência com a referência depende da base, do
modelo e da política reproduzidos, e deve ser conferida no warehouse.
