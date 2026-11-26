# Como contribuir

O projeto usa Python 3.12 e [uv](https://docs.astral.sh/uv/). No Ubuntu ou WSL com Ubuntu, instale `libgomp1`, a biblioteca de execução OpenMP usada pelo scikit-learn no Linux.

```bash
sudo apt update
sudo apt install libgomp1
```

Na raiz do clone, instale as versões do ambiente registradas em `uv.lock` e rode os testes locais.

```bash
uv sync --frozen --all-groups
uv run --frozen pytest -q
```

Os testes Python rodam sem autenticação no Databricks. Eles cobrem, entre outros contratos, a integridade das nove entradas, a configuração da ingestão, a ordem e a elegibilidade das features, a avaliação temporal e a política de alertas. Veja o conteúdo das entradas e o comando de verificação em [Dados de entrada](data/README.md).

## Exemplo de alteração

Ao alterar a seleção de scores de um lote em [`batch_scoring.py`](fleet_monitoring/src/batch_scoring.py), confira o comportamento nas fronteiras do intervalo. O contexto anterior participa do cálculo da política de alertas, e a saída do lote contém apenas as linhas dentro do intervalo solicitado, com o início incluído e o fim excluído. Rode o teste focal e depois o conjunto da raiz.

```bash
uv run --frozen pytest -q fleet_monitoring/tests/test_batch_scoring.py
uv run --frozen pytest -q
```

## Notebooks e SQL

Edite o arquivo `.py` correspondente em [`fleet_monitoring/notebooks/`](fleet_monitoring/notebooks) e regenere o `.ipynb` com Jupytext. Revise o par antes de incluí-lo no mesmo commit.

```bash
uv run --frozen jupytext --to ipynb fleet_monitoring/notebooks/04_score_batch.py
```

Para validar alterações de SQL, configure o catálogo, os schemas e a função
`dbt_frota` na mesma sessão de shell. O [guia de reprodução](docs/reproducao.md)
mostra como selecionar o workspace e preparar as tabelas.

Na raiz do clone, confira a configuração do projeto com `parse`. Depois de preparar as tabelas no seu workspace, use `build` para executar os modelos e testes de dados afetados. O exemplo seleciona `fct_features_scoring` e seus ancestrais.

```bash
dbt_frota parse
dbt_frota build --indirect-selection cautious --select +fct_features_scoring
```

Na descrição do pull request, indique o comportamento afetado, os comandos executados e os resultados. Para mudanças em dados ou SQL, descreva também o impacto no grão, nas chaves ou nas janelas temporais quando houver. Mantenha credenciais e configurações pessoais do workspace fora do Git.
