# Arquitetura e avaliação

## Da telemetria à investigação

1. **Bronze.** Preserva a telemetria e os apontamentos do desafio com seus identificadores de origem.
2. **Silver.** O dbt padroniza a telemetria, agrupa registros em episódios e representa lacunas de cobertura.
3. **Gold.** Constrói as observações por equipamento e hora e as 46 features disponíveis naquele instante. O treino registra o LightGBM calibrado e congela a ordem das features e a política de avisos no MLflow.
4. **Consumo.** O dashboard AI/BI e o Genie consultam views compartilhadas para apresentar prioridades, registros e histórico com a mesma referência temporal.

A unidade da predição é equipamento e horário. Janelas de features encerram
antes do horário de avaliação. A política aplica score mínimo e capacidade
por horário, seguida do intervalo entre avisos para o mesmo equipamento.
O limite por hora representa a capacidade de investigação. O intervalo
evita emitir repetidamente um aviso para um equipamento que continua
prioritário. A fila associa cada prioridade aos registros de telemetria
disponíveis para conferência.

## Avaliação retrospectiva

A política foi escolhida em maio e congelada antes da avaliação final.
Um aviso foi associado a no máximo um episódio. As avaliações usaram
os respectivos pacotes congelados e os dados históricos do desafio.
[Avaliação em junho de 2025](../README.md#avaliação-em-junho-de-2025)

A formação de episódios, a capacidade por horário e o intervalo entre avisos
são definições analíticas. As predições de junho foram
abertas após o congelamento, e parte dos rótulos agregados do mês já havia
sido consultada na análise anterior. Desempenho em uso e efeitos na
manutenção dependem de validação em campo.

O [exemplo local](../examples/run_policy.py) executa a política de ranking a
partir de scores sintéticos. Seus parâmetros servem para ilustrar capacidade
e intervalo entre avisos em poucos registros. O [guia de reprodução](reproducao.md)
descreve como refazer o treino, aplicar a política e comparar uma nova execução
com a referência histórica.
