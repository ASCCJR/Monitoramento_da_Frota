BEGIN
  CREATE SCHEMA IF NOT EXISTS {{product_namespace}}
    COMMENT 'Avaliações preditivas históricas e fontes de leitura do dashboard e do Genie.';

  CREATE OR REPLACE VIEW {{product_namespace}}.replay_source AS
  WITH source_rows AS (
    SELECT
      g.scoring_row_id, g.prediction_timestamp, g.tag, g.tipo_equipamento,
      g.tag_frota, g.active_event_count, g.active_last_event_time,
      g.washout_occurrence_count, g.latest_washout_occurrence_time,
      g.has_complete_global_lookback, g.has_lookback_unavailability,
      g.is_scoring_eligible,
      CASE WHEN g.tipo_equipamento = 'Caminhao' THEN s.risk_score END AS risk_score,
      CASE WHEN g.tipo_equipamento = 'Caminhao' THEN s.rank_at_cutoff END AS rank_at_cutoff,
      g.tipo_equipamento = 'Caminhao' AND coalesce(s.is_policy_candidate, false) AS is_policy_candidate,
      g.tipo_equipamento = 'Caminhao' AND coalesce(s.is_emitted_alert, false) AS is_emitted_alert,
      f.events_4h, f.critical_events_4h, f.noncritical_events_4h,
      f.cma_events_4h, f.hours_since_last_episode_start,
      s.policy_threshold, s.policy_max_alerts_per_cutoff, s.policy_cooldown_hours,
      CASE
        WHEN g.tipo_equipamento <> 'Caminhao' THEN 'Suporte insuficiente'
        WHEN g.washout_occurrence_count > 0 THEN 'Don''t Go recente'
        WHEN NOT g.is_scoring_eligible OR s.risk_score IS NULL THEN 'Dados insuficientes'
        WHEN s.is_emitted_alert THEN 'Alerta emitido'
        WHEN s.is_policy_candidate THEN 'Intervalo entre alertas'
        ELSE 'Sem novo alerta'
      END AS operational_status
    FROM {{silver_namespace}}.int_scoring_grade g
    LEFT JOIN {{gold_namespace}}.fct_operational_scores s
      ON g.scoring_row_id = s.scoring_row_id
      AND s.scoring_run_id = '{{scoring_run_id}}'
    LEFT JOIN {{gold_namespace}}.fct_features_scoring f
      ON g.scoring_row_id = f.scoring_row_id
    WHERE g.prediction_timestamp >= TIMESTAMP_NTZ'2025-06-01 00:00:00'
      AND g.prediction_timestamp <= TIMESTAMP_NTZ'{{reference_time}}'
      AND g.tag_first_event_time <= g.prediction_timestamp
  ), with_previous AS (
    SELECT *,
      lag(risk_score) OVER (PARTITION BY tag ORDER BY prediction_timestamp) AS previous_score,
      lag(prediction_timestamp) OVER (PARTITION BY tag ORDER BY prediction_timestamp) AS previous_cutoff
    FROM source_rows
  )
  SELECT * EXCEPT (previous_score, previous_cutoff),
    CASE WHEN previous_cutoff = timestampadd(HOUR, -1, prediction_timestamp)
      THEN risk_score - previous_score END AS score_delta_1h,
    CASE operational_status WHEN 'Alerta emitido' THEN 1
      WHEN 'Intervalo entre alertas' THEN 2 WHEN 'Sem novo alerta' THEN 3
      WHEN 'Don''t Go recente' THEN 4 WHEN 'Dados insuficientes' THEN 5
      ELSE 6 END AS display_order
  FROM with_previous;

  CREATE TABLE IF NOT EXISTS {{product_namespace}}.replay_snapshots
  USING DELTA AS
  SELECT CAST(NULL AS STRING) AS replay_run_id,
    CAST(NULL AS TIMESTAMP) AS ingested_at_utc,
    s.*, sha2(to_json(struct(s.*)), 256) AS payload_sha256
  FROM {{product_namespace}}.replay_source s WHERE false;

  CREATE TABLE IF NOT EXISTS {{product_namespace}}.replay_head (
    consumer_id STRING, replay_run_id STRING, reveal_until TIMESTAMP_NTZ,
    published_at_utc TIMESTAMP, replay_mode STRING
  ) USING DELTA;

  CREATE OR REPLACE VIEW {{product_namespace}}.fleet_history
  COMMENT 'Contexto de equipamentos já revelado, últimas 24h inclusivas; sem rótulos futuros. Score nulo não significa baixo risco.' AS
  SELECT s.* EXCEPT (payload_sha256, ingested_at_utc),
    h.reveal_until AS reference_time,
    timestampadd(HOUR, -24, h.reveal_until) AS window_start,
    date_format(h.reveal_until, 'dd/MM/yyyy HH:mm') AS reference_label,
    h.published_at_utc, h.replay_mode,
    CASE WHEN s.risk_score IS NOT NULL THEN 1 ELSE 0 END AS scored_count,
    CASE WHEN s.is_emitted_alert THEN 1 ELSE 0 END AS alert_count,
    CASE WHEN s.operational_status = 'Intervalo entre alertas' THEN 1 ELSE 0 END AS cooldown_count,
    CASE WHEN s.operational_status = 'Dados insuficientes' THEN 1 ELSE 0 END AS insufficient_count,
    CASE WHEN s.operational_status = 'Suporte insuficiente' THEN 1 ELSE 0 END AS unsupported_count,
    1 AS equipment_count
  FROM {{product_namespace}}.replay_snapshots s
  JOIN {{product_namespace}}.replay_head h
    ON h.consumer_id = 'fleet_dashboard' AND s.replay_run_id = h.replay_run_id
    AND s.prediction_timestamp <= h.reveal_until
    AND s.prediction_timestamp >= timestampadd(HOUR, -24, h.reveal_until);

  CREATE OR REPLACE VIEW {{product_namespace}}.fleet_current
  COMMENT 'Uma linha por equipamento no relógio publicado. Agora = reference_time; default do painel é Caminhao. Estados não indicam inspeção executada.' AS
  SELECT * FROM {{product_namespace}}.fleet_history
  WHERE prediction_timestamp = reference_time;

  CREATE OR REPLACE VIEW {{product_namespace}}.alert_history
  COMMENT 'Alertas emitidos nas últimas 24h (início exclusivo, fim inclusivo), apenas no estado publicado; sem falhas evitadas ou manutenção executada.' AS
  SELECT * FROM {{product_namespace}}.fleet_history
  WHERE is_emitted_alert
    AND prediction_timestamp > timestampadd(HOUR, -24, reference_time);

  CREATE OR REPLACE VIEW {{product_namespace}}.replay_status AS
  SELECT replay_run_id, reveal_until AS reference_time,
    date_format(reveal_until, 'dd/MM/yyyy HH:mm') AS reference_label,
    published_at_utc, replay_mode
  FROM {{product_namespace}}.replay_head WHERE consumer_id = 'fleet_dashboard';

  SELECT 'BOOTSTRAP_OK' AS status;
END;
