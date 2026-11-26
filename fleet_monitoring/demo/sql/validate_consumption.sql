BEGIN
  SELECT assert_true(count(*) = 1
    AND max(reference_time) = TIMESTAMP_NTZ'{{reference_time}}'
    AND max(replay_run_id) = '{{snapshot_id}}', 'publicação ativa diferente da solicitada')
  FROM {{product_namespace}}.replay_status;

  SELECT assert_true(count(*) > 0 AND count(*) = count(DISTINCT tag)
    AND min(prediction_timestamp) = TIMESTAMP_NTZ'{{reference_time}}'
    AND max(prediction_timestamp) = TIMESTAMP_NTZ'{{reference_time}}', 'grão ou referência da fila inválidos')
  FROM {{product_namespace}}.fleet_investigation;

  SELECT assert_true(count(*) > 0
    AND count(*) = count(DISTINCT struct(prediction_timestamp, tag))
    AND max(prediction_timestamp) = TIMESTAMP_NTZ'{{reference_time}}', 'histórico vazio, duplicado ou fora da referência')
  FROM {{product_namespace}}.fleet_exploration_history;

  SELECT assert_true(count_if(event_time >= reference_time) = 0, 'eventos fora da janela anterior')
  FROM {{product_namespace}}.equipment_events;

  SELECT max(reference_time) AS dados_ate,
    sum(equipment_count) AS caminhoes,
    sum(investigation_count) AS sinais_para_investigar,
    sum(occurrence_count) AS equipamentos_com_ocorrencia,
    sum(insufficient_count) AS equipamentos_com_dados_a_verificar,
    array_sort(collect_list(CASE WHEN investigation_count = 1 THEN named_struct(
      'ordem', investigation_order, 'alarme', evidence_alarm, 'registros', evidence_records,
      'novo_aviso', is_emitted_alert
    ) END)) AS candidatos
  FROM {{product_namespace}}.fleet_investigation
  WHERE tipo_equipamento = 'Caminhao';
END;
