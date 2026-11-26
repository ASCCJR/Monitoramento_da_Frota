# Dashboard de acompanhamento da frota

O painel usa três páginas. **Acompanhamento** organiza a fila e os registros
para conferir. O filtro **Equipamento a conferir** recorta essas duas tabelas
e mantém os indicadores da frota. **Equipamento** mostra a avaliação
selecionada e os registros anteriores. **Histórico** distingue avisos do
sistema e ocorrências na telemetria.

## Importar no seu workspace

Prepare as views e gere `acompanhamento_frota.lvdash.json` seguindo a
[publicação do recorte](../demo/README.md). O JSON usa os nomes das views
relativos ao catálogo e ao schema padrão do dashboard.

Opcionalmente, crie o [Genie](../genie/README.md) primeiro e execute o renderizador
novamente com `--genie-space-id` igual ao ID retornado pela criação. A definição
gerada só contém um vínculo ao Genie quando esse parâmetro é fornecido.

Após `07_validate_consumption.sql`, confira os 13 datasets no editor AI/BI
usando as views do recorte. Na consulta de equipamento, use uma tag do
próprio recorte e `Mais recente` como a primeira avaliação.

Defina o catálogo e schema do produto na importação. Se usar a CLI, rode:

```bash
databricks lakeview create \
  --profile "$DATABRICKS_CONFIG_PROFILE" --warehouse-id "$DATABRICKS_WAREHOUSE_ID" \
  --dataset-catalog "$FLEET_CATALOG" --dataset-schema "${FLEET_PRODUCT_SCHEMA:-fleet_product}" \
  --display-name "Acompanhamento da frota" \
  --serialized-dashboard "$(cat "$RENDERED_DIR/acompanhamento_frota.lvdash.json")" \
  --output json
```

Abra o rascunho com o ID retornado e confira indicadores, ordenação, filtro
por equipamento e período. Ao usar `lakeview update`, passe o catálogo e o
schema de produto novamente junto da definição. Publique no workspace quando o acesso aos dados
e as permissões de leitura estiverem conferidos:

```bash
databricks lakeview publish \
  "$DASHBOARD_ID" --profile "$DATABRICKS_CONFIG_PROFILE" --warehouse-id "$DATABRICKS_WAREHOUSE_ID"
```
