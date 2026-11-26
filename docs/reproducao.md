# Reproduzir da ingestão à investigação

Os [nove arquivos de entrada](../data/README.md) já acompanham o clone.
Este roteiro carrega a base no seu workspace, constrói as features, treina
o modelo, calcula a fila e instala as interfaces sobre um recorte histórico.

## Pedir a execução a uma IA

Use um assistente com acesso ao terminal da máquina e aos arquivos do clone.
Ele pode instalar dependências, executar as etapas, acompanhar os Jobs e
registrar os resultados. Você escolhe o workspace, o perfil, o catálogo e o
warehouse, e conclui o login OAuth quando solicitado.

Abra o repositório clonado no assistente ou peça que ele o clone usando a
URL do GitHub. Um pedido que pode ser copiado:

> Siga `docs/reproducao.md` do Monitoramento da frota. Confira o ambiente e
> me ajude a escolher o perfil OAuth, o catálogo e o SQL warehouse antes de
> iniciar os Jobs. Execute da ingestão à avaliação e valide o recorte.
> Crie o dashboard em rascunho e o Genie no meu workspace. Registre
> `freeze_id`, `scoring_run_id`, métricas e
> divergências. Se algo falhar, indique a etapa e conserve os resultados
> já verificados.

Para clonar pelo terminal, defina `FLEET_REPO_URL` com a URL de clone:

```bash
git clone "$FLEET_REPO_URL" Monitoramento_da_Frota
cd Monitoramento_da_Frota
```

## 1. Preparar o ambiente

- Python 3.12, `uv` e Databricks CLI. Em Ubuntu ou WSL, instale
  `libgomp1` para as bibliotecas numéricas (`sudo apt install libgomp1`).
  Os comandos usam Bash. No WSL, instale as ferramentas e configure o perfil
  nesse ambiente.
- Um workspace com Unity Catalog, compute serverless para Spark e Jobs e um
  SQL warehouse habilitado para executar dbt e consultas de consumo.
- Acesso ao SQL warehouse (`CAN USE`) e ao compute serverless, e permissão
  para executar Jobs e criar recursos na sua pasta de workspace. No catálogo,
  o fluxo usa `USE CATALOG` e `CREATE SCHEMA`. Nos schemas, precisa poder criar
  tabelas, views, Volume e modelo, e ler e gravar as entradas e saídas. O
  treino registra o modelo no Unity Catalog e usa um experimento MLflow.
  [Privilégios do Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference)

Ingestão, dbt e treino consomem recursos do seu workspace. O uso de
compute segue a cobrança ou a cota da sua edição. A Free Edition pode
interromper execuções ao alcançar a cota.
[Limites da Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)

Na raiz do clone, instale o ambiente e confira os arquivos.

```bash
uv sync --frozen --all-groups
uv run --frozen python -m fleet_monitoring.src.input_manifest \
  --manifest fleet_monitoring/manifests/inputs_v1.json --repo-root .
```

A sessão aberta na interface do Databricks e o perfil OAuth da CLI são
autenticações distintas. Liste os perfis locais com `databricks auth profiles`.
Escolha um perfil do workspace desejado ou dê um nome a um perfil novo. Use
como host a origem HTTPS da URL do workspace. Se o perfil ainda precisar
de autenticação, rode `auth login` e conclua o fluxo aberto no navegador.
A credencial permanece na sua máquina.
[Autenticação OAuth do Databricks](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m)

```bash
export DATABRICKS_CONFIG_PROFILE='meu_perfil'
export DATABRICKS_WORKSPACE_URL='https://meu-workspace.cloud.databricks.com'

databricks auth login --host "$DATABRICKS_WORKSPACE_URL" \
  --profile "$DATABRICKS_CONFIG_PROFILE"
databricks current-user me --profile "$DATABRICKS_CONFIG_PROFILE"
databricks catalogs list --profile "$DATABRICKS_CONFIG_PROFILE"
databricks warehouses list --profile "$DATABRICKS_CONFIG_PROFILE"
```

Use o nome do catálogo e o ID do warehouse das listagens. Confirme que são
os recursos escolhidos para a execução e que as permissões acima se aplicam.
Mantenha os exports e as funções Bash na mesma sessão da execução.

```bash
export DATABRICKS_WAREHOUSE_ID='id_do_meu_warehouse'
export FLEET_CATALOG='meu_catalogo'

export FLEET_BRONZE_SCHEMA='fleet_bronze'
export FLEET_SILVER_SCHEMA='fleet_silver'
export FLEET_GOLD_SCHEMA='fleet_gold'
export FLEET_ML_SCHEMA='fleet_ml'
export FLEET_PRODUCT_SCHEMA='fleet_product'

export BUNDLE_VAR_catalog="$FLEET_CATALOG"
export BUNDLE_VAR_bronze_schema="$FLEET_BRONZE_SCHEMA"
export BUNDLE_VAR_silver_schema="$FLEET_SILVER_SCHEMA"
export BUNDLE_VAR_gold_schema="$FLEET_GOLD_SCHEMA"
export BUNDLE_VAR_ml_schema="$FLEET_ML_SCHEMA"
```

Use schemas distintos para cada camada da instalação. Use nomes em
minúsculas, com dígitos e sublinhados após a primeira letra. O Volume de
entrada chama-se `raw_inputs` por padrão e pode ser definido em
`FLEET_INPUT_VOLUME`.

## 2. Carregar a Bronze e construir as features

```bash
uv run --frozen --group databricks python fleet_monitoring/notebooks/01_bronze.py
```

A ingestão cria os schemas e o Volume, confere bytes e hashes na origem
e reconcilia as contagens das quatro tabelas. Uma reexecução confere os
arquivos e as tabelas existentes. O resumo local por origem é gravado em
`outputs/bronze/bronze_execution_manifest_v1.json`.

Configure o dbt na mesma sessão. A função abaixo usa a credencial OAuth
temporária do perfil escolhido para cada comando.
[Autenticação OAuth do Databricks](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m)

```bash
export DBT_DATABRICKS_HOST="${DATABRICKS_WORKSPACE_URL#https://}"
export DBT_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/$DATABRICKS_WAREHOUSE_ID"

dbt_frota() {
  local token
  token=$(databricks auth token --profile "$DATABRICKS_CONFIG_PROFILE" --output json |
    uv run --frozen python -c 'import json, sys; print(json.load(sys.stdin)["access_token"])') || return
  DBT_ENV_SECRET_DATABRICKS_TOKEN="$token" uv run --frozen --group dbt dbt "$@" \
    --project-dir fleet_monitoring/dbt --profiles-dir fleet_monitoring/dbt
}

dbt_frota parse
dbt_frota build --indirect-selection cautious \
  --select +fct_training_set +fct_training_assignments +fct_features_scoring \
  +stg_apontamentos +stg_dicionario_dados
dbt_frota test --select assert_staging_row_counts_match_bronze
```

`fct_features_scoring` contém 46 features por equipamento e hora. O
`fct_training_set` acrescenta o alvo calculado para avaliação, e
`fct_training_assignments` determina as partições temporais com purga.
A [arquitetura](arquitetura-e-avaliacao.md) explica as decisões e a
referência histórica.

## 3. Treinar e congelar

Execute os comandos do bundle a partir de `fleet_monitoring/`.

```bash
cd fleet_monitoring
databricks bundle validate --strict --target dev --profile "$DATABRICKS_CONFIG_PROFILE"
databricks bundle deploy --target dev --profile "$DATABRICKS_CONFIG_PROFILE"
databricks bundle run train_and_freeze --target dev --profile "$DATABRICKS_CONFIG_PROFILE"
```

O treino seleciona e ajusta modelos em janeiro–abril, calibra e define
a política em maio e registra o resultado no MLflow. Guarde o
`freeze_id` retornado pelo Job. Cada congelamento registra a versão do
modelo, as features, a política e a identidade dos artefatos da execução.

### Recuperar os IDs de um Job

Se a CLI não mostrar o JSON retornado pelo notebook, anote o ID da execução
do Job na saída ou no link da execução. Se o link também estiver ausente,
consulte `databricks bundle summary --target dev --profile "$DATABRICKS_CONFIG_PROFILE"`
e use o ID do Job selecionado em
`databricks jobs list-runs --job-id "$JOB_ID" --limit 5 --profile "$DATABRICKS_CONFIG_PROFILE"`.
Escolha a execução bem-sucedida desta rodada.

A função recebe o ID da execução do Job, a chave da tarefa e o campo
procurado. Ela lê a saída da tarefa do notebook:

```bash
id_do_notebook() {
  local execucao="$1" tarefa="$2" campo="$3" tarefa_id
  tarefa_id=$(databricks jobs get-run "$execucao" --profile "$DATABRICKS_CONFIG_PROFILE" --output json |
    uv run --frozen python -c 'import json, sys
tarefa = next(t for t in json.load(sys.stdin)["tasks"] if t["task_key"] == sys.argv[1])
if tarefa["state"]["result_state"] != "SUCCESS":
    sys.exit("A tarefa do notebook precisa ter concluído com SUCCESS")
print(tarefa["run_id"])' "$tarefa") || return
  databricks jobs get-run-output "$tarefa_id" --profile "$DATABRICKS_CONFIG_PROFILE" --output json |
    uv run --frozen python -c 'import json, sys
saida = json.loads(json.load(sys.stdin)["notebook_output"]["result"])
print(saida[sys.argv[1]])' "$campo"
}
```

Com `TRAIN_RUN_ID` definido para a execução de treino, obtenha o congelamento:

```bash
FREEZE_ID=$(id_do_notebook "$TRAIN_RUN_ID" train_and_freeze freeze_id) && export FREEZE_ID
```

## 4. Pontuar junho e avaliar

Ainda em `fleet_monitoring/`, defina `FREEZE_ID` com o `freeze_id` retornado
pelo treino ou recuperado com `id_do_notebook`.

```bash
databricks bundle run score_batch --target dev --profile "$DATABRICKS_CONFIG_PROFILE" \
  --params "freeze_id=$FREEZE_ID"
databricks bundle run evaluate_holdout --target dev --profile "$DATABRICKS_CONFIG_PROFILE" \
  --params "freeze_id=$FREEZE_ID,release_holdout=true"
cd ..

dbt_frota build --indirect-selection cautious \
  --select +fct_operational_scores+ +dim_model_feature_importance \
  source:v2_ml_outputs.batch_scoring_runs
dbt_frota build --indirect-selection cautious \
  --select +mart_holdout_daily +mart_holdout_metrics +fct_policy_validation_scores
dbt_frota test --select assert_operational_scores_no_outcome_columns \
  assert_operational_scores_match_frozen_holdout
```

Guarde o `scoring_run_id` do primeiro Job. O scoring usa somente as
features e a política. A avaliação compara as previsões com os episódios de
junho. Compare as métricas com os [resultados de referência](../README.md#avaliação-em-junho-de-2025)
e registre o identificador do seu congelamento junto aos resultados.

Se precisar recuperar o identificador do scoring, defina `SCORE_RUN_ID` com
o ID da execução de `score_batch` e rode:

```bash
SCORING_RUN_ID=$(id_do_notebook "$SCORE_RUN_ID" score_batch scoring_run_id) && export SCORING_RUN_ID
```

## 5. Consultar a fila, o dashboard e o Genie

Defina `SCORING_RUN_ID` com o identificador de scoring retornado ou recuperado.
Use esse valor para [publicar o recorte e validar as views](../fleet_monitoring/demo/README.md).
As instruções daquele guia usam o mesmo catálogo e os mesmos schemas e
orientam a criação de um dashboard e de um Genie ligados à mesma hora de
referência. O [caso demonstrado](../README.md#uma-investigação-na-prática) apresenta as capturas e
as perguntas usadas na investigação.
