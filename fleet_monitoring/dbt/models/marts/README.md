# Marts

Camada Gold para features, training set, fila, histórico de scores e métricas.

`fct_features_scoring` não contém alvo. `fct_training_set` acrescenta labels
somente depois do cálculo das features, por join exato de `scoring_row_id`.
Os marts de scoring e avaliação leem as saídas das respectivas execuções
de MLflow. Construa-os na ordem do [guia de reprodução](../../../../docs/reproducao.md).
