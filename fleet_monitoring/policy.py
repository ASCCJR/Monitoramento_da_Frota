"""Interface local para a mesma política usada pelo scoring em lote."""

from fleet_monitoring.src.batch_scoring import apply_operational_policy

__all__ = ["apply_operational_policy"]
