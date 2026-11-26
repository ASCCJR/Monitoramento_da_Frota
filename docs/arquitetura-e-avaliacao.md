# Arquitetura e avaliação

## Da telemetria à investigação

1. **Bronze.** Preserva a telemetria e os apontamentos do desafio com seus identificadores de origem.
2. **Silver.** O dbt padroniza a telemetria, agrupa registros em episódios e representa lacunas de cobertura.
3. **Gold.** Constrói as observações por equipamento e hora e as features disponíveis naquele instante. O modelo LightGBM calibrado foi congelado com o registro de features e a política de avisos no MLflow.
4. **Consumo.** O dashboard AI/BI e o Genie consultam views compartilhadas para apresentar prioridades, registros e histórico com a mesma referência temporal.

A unidade da predição é equipamento e horário. Janelas de features encerram
antes do horário de avaliação. A política aplica score mínimo e capacidade
por horário, seguida do intervalo entre avisos para o mesmo equipamento.
Pontuação de risco e registros de telemetria orientam a investigação.

## Avaliação retrospectiva

A avaliação de junho de 2025 cobre 17.203 observações de 31 equipamentos. A
política foi escolhida em maio e congelada antes de abrir as predições da
avaliação final. Um aviso foi associado a no máximo um episódio de avaliação.

| Medida | Resultado |
|---|---:|
| Episódios elegíveis associados a aviso | 75 de 222 (33,8%) |
| Avisos associados a episódio | 75 de 251 (29,9%) |
| Antecedência mediana entre avisos associados | 93,9 min |
| Episódios elegíveis em caminhões associados a aviso | 75 de 220 (34,1%) |

A cobertura ficou concentrada nos caminhões. Escavadeiras tiveram apenas dois
episódios elegíveis. A formação de episódios, a capacidade por horário e o
intervalo entre avisos são definições analíticas. As predições de junho foram
abertas após o congelamento, e parte dos rótulos agregados do mês já havia
sido consultada na análise anterior. O resultado vale para o recorte
retrospectivo e seus critérios de associação. Desempenho em uso e efeitos na
manutenção dependem de validação em campo.

O [exemplo local](../examples/run_policy.py) executa a política de ranking a
partir de scores sintéticos. Seus parâmetros servem para ilustrar capacidade
e intervalo entre avisos em poucos registros. A avaliação acima usou o pacote
congelado e dados históricos do desafio.
