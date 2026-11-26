# Assistente Genie de acompanhamento da frota

O Genie consulta seis views validadas do recorte. As instruções distinguem
prioridade de investigação, novo aviso e registro de telemetria. Perguntas
sobre os dados atuais usam a hora histórica publicada.

## Criar no seu workspace

Prepare as views e gere `acompanhamento_frota.genie.json` seguindo a
[publicação do recorte](../demo/README.md). O catálogo e o schema do leitor
são aplicados às fontes, aos exemplos de consultas e aos benchmarks.

Defina `GENIE_PARENT_PATH` com uma pasta existente no seu workspace e use as
mesmas variáveis de perfil, warehouse e diretório da publicação.

```bash
databricks genie create-space \
  "$DATABRICKS_WAREHOUSE_ID" "$(cat "$RENDERED_DIR/acompanhamento_frota.genie.json")" \
  --profile "$DATABRICKS_CONFIG_PROFILE" --parent-path "$GENIE_PARENT_PATH" \
  --title "Assistente de acompanhamento da frota" --output json
```

Abra o espaço criado e pergunte quais caminhões são prioritários e quais
registros devem ser conferidos no corte publicado. Compare a ordem, as
contagens e o horário com o SQL. Use o ID retornado da criação ao renderizar
uma [configuração do dashboard](../dashboards/README.md) com o Genie vinculado.
