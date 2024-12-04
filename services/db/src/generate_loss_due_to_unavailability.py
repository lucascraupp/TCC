import json
import os

import pandas as pd
import structlog
from joblib import Parallel, delayed

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))


def process_day(
    unavailability_profile: pd.DataFrame, loss_table: pd.DataFrame, equation: str
) -> pd.DataFrame | None:
    if loss_table.empty or "CSI" not in loss_table.columns:
        return

    csi = loss_table["CSI"].iloc[0]

    unavailability_profile = unavailability_profile.sort_values("Ângulo médio (°)")

    losses = []
    for _, row in unavailability_profile.iterrows():
        angle = row["Ângulo médio (°)"]
        unavailability_percentage = row["Porcentagem de indisponibilidade (%)"]

        loss = (
            eval(equation, {"CSI": csi, "Ângulo": angle})
            * unavailability_percentage
            / 100
        )

        losses.append(
            {
                "Data": row["Data"],
                "CSI": csi,
                "Ângulo médio (°)": angle,
                "Porcentagem de indisponibilidade (%)": unavailability_percentage,
                "Perda por indisponibilidade (%)": loss,
            }
        )

    return pd.DataFrame(losses)


def generate_loss_due_to_unavailability(solar_plant: str) -> None:
    log = structlog.get_logger()

    if not os.path.exists(
        PLANTS_PARAM[solar_plant]["datalake"]["unavailability_profile"]
    ):
        log.error(
            f"Arquivo de perfil de indisponibilidade não encontrado para a usina {solar_plant}"
        )
        return

    log.info("Gerando variável", var="Perda por indisponibilidade (%)")

    unavailability_profile = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datalake"]["unavailability_profile"]
    )
    loss_table = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"]["loss_table"]
    )

    equation = PLANTS_PARAM[solar_plant]["equation"]
    equation = equation.replace("^", "**")

    date_range = unavailability_profile["Data"].unique()

    loss_list = Parallel(n_jobs=-1)(
        delayed(process_day)(
            unavailability_profile[unavailability_profile["Data"] == date],
            loss_table[loss_table["Data"] == date],
            equation,
        )
        for date in date_range
    )

    loss_due_to_unavailability = pd.concat(loss_list)
    loss_due_to_unavailability = loss_due_to_unavailability.reset_index(drop=True)

    path = PLANTS_PARAM[solar_plant]["datawarehouse"]["loss_due_to_unavailability"]

    loss_table.to_parquet(path)

    log.info("Dados salvos", filename=path)
