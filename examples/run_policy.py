"""Demonstra a política de avisos com scores sintéticos."""

from pathlib import Path

import pandas as pd

from fleet_monitoring.policy import apply_operational_policy


def main() -> None:
    inputs = pd.read_csv(Path(__file__).with_name("scores_sinteticos.csv"))
    scored = apply_operational_policy(
        inputs, threshold=0.65, max_alerts_per_cutoff=2, cooldown_hours=4
    )
    columns = {
        "prediction_timestamp": "Avaliação",
        "tag": "Equipamento",
        "risk_score": "Score",
        "rank_at_cutoff": "Posição",
        "is_policy_candidate": "Candidato",
        "is_emitted_alert": "Aviso emitido",
    }
    report = scored.loc[:, list(columns)].rename(columns=columns)
    report["Avaliação"] = report["Avaliação"].dt.strftime("%d/%m/%Y %H:%M")
    for column in ["Candidato", "Aviso emitido"]:
        report[column] = report[column].map({True: "Sim", False: "Não"})

    print("Exemplo sintético com três equipamentos e duas avaliações")
    print("Limiar de 0,65, até dois candidatos por avaliação e intervalo de 4 h entre avisos")
    print(report.to_string(index=False))


if __name__ == "__main__":
    main()
