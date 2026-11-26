select
    cma_regra_id,
    quantidade,
    janela_minutos,
    regra_imediata
from {{ ref('stg_cma_regras') }}
where
    quantidade <= 0
    or janela_minutos < 0
    or regra_imediata <> (janela_minutos = 0)
    or (janela_minutos = 0 and quantidade <> 1)
