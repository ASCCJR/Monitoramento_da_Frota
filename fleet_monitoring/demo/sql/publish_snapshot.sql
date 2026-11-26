-- Publicação manual das avaliações de scoring batch até a referência escolhida.
BEGIN
  DECLARE snapshot_id STRING DEFAULT '{{snapshot_id}}';
  DECLARE snapshot_time TIMESTAMP_NTZ DEFAULT TIMESTAMP_NTZ'{{reference_time}}';
  DECLARE expected_rows BIGINT;
  DECLARE expected_cutoffs BIGINT;

  SET expected_rows = (SELECT count(*) FROM {{product_namespace}}.replay_source);
  SET expected_cutoffs = timestampdiff(HOUR, TIMESTAMP_NTZ'2025-06-01 00:00:00', snapshot_time) + 1;

  SELECT assert_true(expected_rows > 0
    AND count(DISTINCT struct(prediction_timestamp, tag)) = expected_rows
    AND count(DISTINCT prediction_timestamp) = expected_cutoffs
    AND max(prediction_timestamp) = snapshot_time
    AND count(risk_score) > 0, 'origem vazia, duplicada, sem scores ou com horas ausentes')
  FROM {{product_namespace}}.replay_source;

  SELECT assert_true(count(*) = 0, 'score, elegibilidade ou contexto temporal inválido')
  FROM {{product_namespace}}.replay_source
  WHERE (risk_score IS NOT NULL AND (risk_score < 0 OR risk_score > 1 OR isnan(risk_score)))
    OR (tipo_equipamento = 'Caminhao' AND NOT (is_scoring_eligible <=> (risk_score IS NOT NULL)))
    OR active_last_event_time >= prediction_timestamp
    OR latest_washout_occurrence_time > prediction_timestamp;

  SELECT assert_true(count(*) = 0, 'a origem mudou para esta execução de scoring e referência')
  FROM {{product_namespace}}.replay_snapshots t
  JOIN {{product_namespace}}.replay_source s
    ON t.prediction_timestamp = s.prediction_timestamp AND t.tag = s.tag
  WHERE t.replay_run_id = snapshot_id
    AND NOT (t.payload_sha256 <=> sha2(to_json(struct(s.*)), 256));

  MERGE INTO {{product_namespace}}.replay_snapshots t
  USING (
    SELECT snapshot_id AS replay_run_id, current_timestamp() AS ingested_at_utc,
      s.*, sha2(to_json(struct(s.*)), 256) AS payload_sha256
    FROM {{product_namespace}}.replay_source s
  ) s ON t.replay_run_id = s.replay_run_id
    AND t.prediction_timestamp = s.prediction_timestamp AND t.tag = s.tag
  WHEN NOT MATCHED THEN INSERT *;

  SELECT assert_true(count(*) = expected_rows
    AND count(DISTINCT struct(prediction_timestamp, tag)) = expected_rows,
    'publicação incompleta ou duplicada')
  FROM {{product_namespace}}.replay_snapshots
  WHERE replay_run_id = snapshot_id;

  -- O ponteiro só muda após as verificações. Repetir a mesma publicação preserva o relógio.
  MERGE INTO {{product_namespace}}.replay_head t
  USING (
    SELECT 'fleet_dashboard' AS consumer_id, snapshot_id AS replay_run_id,
      snapshot_time AS reveal_until, current_timestamp() AS published_at_utc,
      'BATCH_SCORE_SNAPSHOT' AS replay_mode
  ) s ON t.consumer_id = s.consumer_id
  WHEN MATCHED AND (NOT (t.replay_run_id <=> s.replay_run_id)
    OR NOT (t.reveal_until <=> s.reveal_until)) THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;

  SELECT reference_time, published_at_utc, replay_mode FROM {{product_namespace}}.replay_status;
END;
