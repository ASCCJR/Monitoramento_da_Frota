-- Camada de leitura operacional: não altera scores, política ou replay.
CREATE OR REPLACE VIEW {{product_namespace}}.fleet_investigation
COMMENT 'Uma linha por equipamento no relógio publicado. Prioridade de investigação usa candidatos antes do cooldown; evidência é contexto de telemetria, não causa do score nem atendimento.' AS
WITH grouped_events AS (
  SELECT tag, reference_time, alarme, criticidade, is_cma_associated, is_dont_go,
    count(*) AS record_count, max(event_time) AS last_record_time
  FROM {{product_namespace}}.equipment_events
  GROUP BY tag, reference_time, alarme, criticidade, is_cma_associated, is_dont_go
), evidence AS (
  SELECT *, row_number() OVER (
    PARTITION BY tag, reference_time
    ORDER BY is_dont_go DESC, is_cma_associated DESC,
      CASE WHEN criticidade = 'Crítico' THEN 0 ELSE 1 END,
      record_count DESC, alarme
  ) AS evidence_order
  FROM grouped_events
)
SELECT c.tag, c.tipo_equipamento, c.reference_time, c.prediction_timestamp,
  c.risk_score, c.is_policy_candidate, c.is_emitted_alert, c.rank_at_cutoff,
  c.insufficient_count, c.washout_occurrence_count,
  CASE WHEN c.is_policy_candidate THEN c.rank_at_cutoff END AS investigation_order,
  CASE WHEN c.is_policy_candidate THEN 1 ELSE 0 END AS investigation_count,
  CASE WHEN c.tipo_equipamento = 'Caminhao' AND c.washout_occurrence_count > 0 THEN 1 ELSE 0 END AS occurrence_count,
  1 AS equipment_count,
  CASE
    WHEN c.is_policy_candidate THEN 'Investigar sinal de risco'
    WHEN c.washout_occurrence_count > 0 THEN 'Ocorrência registrada'
    WHEN c.insufficient_count = 1 THEN 'Verificar dados'
    WHEN c.tipo_equipamento <> 'Caminhao' THEN 'Sem avaliação para este tipo'
    ELSE 'Sem sinal priorizado agora'
  END AS investigation_status,
  CASE
    WHEN c.is_policy_candidate AND p.is_policy_candidate THEN concat(
      'Sinal de risco presente ',
      CASE WHEN to_date(p.prediction_timestamp) = to_date(c.prediction_timestamp)
        THEN concat('às ', date_format(p.prediction_timestamp, 'HH:mm'))
        ELSE concat('em ', date_format(p.prediction_timestamp, 'dd/MM HH:mm')) END,
      ' e mantido ',
      CASE WHEN to_date(p.prediction_timestamp) = to_date(c.prediction_timestamp)
        THEN concat('às ', date_format(c.prediction_timestamp, 'HH:mm'))
        ELSE concat('em ', date_format(c.prediction_timestamp, 'dd/MM HH:mm')) END)
    WHEN c.is_policy_candidate THEN concat('Sinal de risco identificado às ', date_format(c.prediction_timestamp, 'HH:mm'))
    WHEN c.washout_occurrence_count > 0 THEN 'Há ocorrência registrada na telemetria'
    WHEN NOT c.has_complete_global_lookback THEN 'Histórico ainda incompleto'
    WHEN c.has_lookback_unavailability THEN 'Há uma lacuna na captura dos dados'
    WHEN c.active_event_count = 0 THEN 'Sem telemetria nas últimas 4h'
    WHEN c.risk_score IS NULL THEN 'Avaliação indisponível'
    ELSE 'Sem sinal priorizado nesta avaliação'
  END AS investigation_reason,
  CASE
    WHEN c.is_policy_candidate THEN 'Conferir os registros e o histórico do equipamento'
    WHEN c.washout_occurrence_count > 0 THEN 'Conferir a ocorrência e seu atendimento com a equipe'
    WHEN c.insufficient_count = 1 THEN 'Verificar a disponibilidade dos dados na origem'
    ELSE 'Acompanhar as próximas atualizações'
  END AS suggested_check,
  e.alarme AS evidence_alarm_original,
  COALESCE(CASE e.alarme
    WHEN 'Engine Coolant Level - Active' THEN 'Nível do líquido de arrefecimento'
    WHEN 'Transmission Oil Level - Active' THEN 'Nível do óleo da transmissão'
    ELSE e.alarme END, 'Sem registro na janela') AS evidence_alarm,
  e.record_count AS evidence_records,
  e.last_record_time AS evidence_last_time,
  COALESCE(e.criticidade, '—') AS evidence_severity,
  COALESCE(CAST(e.record_count AS STRING), '—') AS evidence_records_label,
  COALESCE(date_format(e.last_record_time, 'dd/MM HH:mm:ss'), '—') AS evidence_time_label,
  concat(date_format(timestampadd(HOUR, -4, c.reference_time), 'dd/MM HH:mm'),
    ' → ', date_format(c.reference_time, 'dd/MM HH:mm')) AS evidence_period,
  date_format(c.reference_time, 'dd/MM/yyyy HH:mm') AS reference_label
FROM {{product_namespace}}.fleet_current c
LEFT JOIN {{product_namespace}}.fleet_history p
  ON p.tag = c.tag AND p.reference_time = c.reference_time
  AND p.prediction_timestamp = timestampadd(HOUR, -1, c.reference_time)
LEFT JOIN evidence e
  ON e.tag = c.tag AND e.reference_time = c.reference_time AND e.evidence_order = 1;
