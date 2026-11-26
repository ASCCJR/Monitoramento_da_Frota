-- Registros para investigação de uma avaliação passada. Não avança o replay.
CREATE OR REPLACE VIEW {{product_namespace}}.equipment_event_history
COMMENT 'Telemetria histórica para investigação por equipamento e avaliação. Limitada ao head publicado; não atribui causa ao score. Consultar com tag e janela anterior ao horário escolhido.' AS
SELECT c.tag, c.tipo_equipamento, c.reference_time,
  t.evento_telemetria_id, t.event_time, t.alarme_id,
  t.alarme, t.criticidade, t.is_dont_go,
  l.cma_evento_id IS NOT NULL AS is_cma_associated
FROM {{product_namespace}}.fleet_current c
JOIN {{silver_namespace}}.stg_telemetria t
  ON t.tag = c.tag
  AND t.event_time >= TIMESTAMP_NTZ'2025-05-31 20:00:00'
  AND t.event_time < c.reference_time
LEFT JOIN {{silver_namespace}}.int_cma_evento_lookup l
  ON upper(trim(t.alarme)) = l.evento_key;
