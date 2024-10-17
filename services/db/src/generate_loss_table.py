import json

import pandas as pd
from joblib import Parallel, delayed

PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))


def get_csi(
    ghi: pd.DataFrame,
    ghi_class: pd.DataFrame,
    clearsky: pd.DataFrame,
) -> float | None:
    # Exclui os dias que o GHI não está disponível
    if ghi_class["GHI"].ne("Disponível").any():
        csi = None
    else:
        clearsky_only_irradiance = clearsky[clearsky["GHI teórico (clearsky)"] > 0]
        ghi_limited = ghi.loc[clearsky_only_irradiance.index]

        # Cálculo do índice de céu limpo
        day_irradiation = ghi_limited["GHI"].sum()
        teoric_irradiation = clearsky["GHI teórico (clearsky)"].sum()

        csi = round(day_irradiation / teoric_irradiation, 2)

    return csi if csi <= 1 else 1


def get_day_loss(
    ghi: pd.DataFrame,
    classification: pd.DataFrame,
    clearsky: pd.DataFrame,
    teoric_power: pd.DataFrame,
    stopped_trackers_power: pd.DataFrame,
) -> pd.DataFrame | None:
    csi = get_csi(ghi, classification[["GHI"]], clearsky)

    gti_classification = classification.drop("GHI", axis=1)

    """ Verifica se é possível utilizar o índice de céu limpo do dia.
    Também verifica se o classification possui pelo menos uma coluna "Disponível" em cada uma de suas linhas """
    if (
        csi is None
        or gti_classification.apply(
            lambda row: "Disponível" not in row.values, axis=1
        ).any()
    ):
        return None
    else:
        day_power = teoric_power["Potência teórica"].sum()
        percentual_loss = round(
            (day_power - stopped_trackers_power.sum()) / day_power * 100, 2
        )

        angle_list = [
            int(col.split(" ")[-1].split("°")[0])
            for col in stopped_trackers_power.columns
        ]

        return pd.DataFrame(
            {
                "Data": ghi.index.date[0],
                "CSI": csi,
                "Angulação (°)": angle_list,
                "Perda (%)": percentual_loss.values,
            }
        )


def generate_loss_table(solar_plant: str) -> None:
    ghi = pd.read_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"]["ghi_avg"])

    classification = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"]["classification"]
    )

    clearsky = pd.read_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"]["clearsky"])

    teoric_power = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"]["teoric_power_avg"]
    )
    stopped_trackers_power = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"]["stopped_trackers_power"]
    )

    begin = teoric_power.index.min()
    end = teoric_power.index.max()

    dates = pd.date_range(begin, end, freq="D")

    loss_list = Parallel(n_jobs=-1)(
        delayed(get_day_loss)(
            ghi[ghi.index.date == date.date()],
            classification[classification.index.date == date.date()],
            clearsky[clearsky.index.date == date.date()],
            teoric_power[teoric_power.index.date == date.date()],
            stopped_trackers_power[stopped_trackers_power.index.date == date.date()],
        )
        for date in dates
    )

    loss_table = pd.concat(loss_list)
    loss_table = loss_table.sort_values(["Data", "Angulação (°)"])
    loss_table = loss_table.reset_index(drop=True)

    loss_table.to_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"]["loss_table"])
