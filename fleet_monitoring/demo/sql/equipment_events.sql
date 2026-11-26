-- Fonte de investigação: sem campos pessoais e sem acesso a eventos futuros.
-- Usa o mesmo registro Silver que alimenta as features; o head só avança
-- quando todo o lote e o snapshot foram publicados com sucesso.
CREATE OR REPLACE VIEW {{product_namespace}}.equipment_events
COMMENT 'Telemetria em [reference_time-4h,reference_time), limitada ao head publicado. CMA é associação por nome, não execução de regra nem contribuição causal ao score.' AS
SELECT
  c.replay_run_id, c.reference_time, c.tag, c.tipo_equipamento,
  t.evento_telemetria_id, t.event_time, t.alarme_id,
  t.alarme, t.criticidade, t.is_dont_go,
  l.cma_evento_id IS NOT NULL AS is_cma_associated,
  timestampadd(HOUR, -4, c.reference_time) AS window_start
FROM {{product_namespace}}.fleet_current c
JOIN {{silver_namespace}}.stg_telemetria t
  ON t.tag = c.tag
  AND t.event_time >= timestampadd(HOUR, -4, c.reference_time)
  AND t.event_time < c.reference_time
LEFT JOIN {{silver_namespace}}.int_cma_evento_lookup l
  ON upper(trim(t.alarme)) = l.evento_key;
