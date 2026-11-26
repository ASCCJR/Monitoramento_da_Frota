-- Histórico de avaliações preservadas na publicação ativa.
CREATE OR REPLACE VIEW {{product_namespace}}.fleet_exploration_history
COMMENT 'Avaliações horárias publicadas desde 01/06/2025 até a referência. Uma linha por equipamento e horário, sem rótulos nem avaliações futuras.' AS
SELECT s.* EXCEPT (replay_run_id, ingested_at_utc, payload_sha256),
  h.reveal_until AS reference_time,
  timestamp_ntz'2025-06-01 00:00:00' AS window_start,
  CAST(s.prediction_timestamp AS DATE) AS history_date,
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
  AND s.prediction_timestamp <= h.reveal_until;
