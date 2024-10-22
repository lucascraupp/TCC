import json

import pandas as pd
import structlog
from joblib import Parallel, delayed

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))


def get_period_irradiances(
    gti: pd.DataFrame,
    classification: pd.DataFrame,
    begin: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame | None:
    gti_period = gti.loc[begin:end]
    classification_period = classification.loc[begin:end]

    classification_period = classification_period.loc[
        :, (classification_period == "Disponível").all()
    ]

    if not classification_period.empty:
        gti_period = gti_period[classification_period.columns]

        teoric_gti = gti_period.median(axis=1)

        return pd.DataFrame({"GTI teórico": teoric_gti})


def process_day(
    gti: pd.DataFrame,
    classification: pd.DataFrame,
    date: pd.Timestamp,
) -> pd.DataFrame | None:
    gti_day = gti.loc[gti.index.date == date.date()]
    classification_day = classification.loc[classification.index.date == date.date()]

    limits = [
        (gti_day.index.min(), classification_day.index[1] - pd.Timedelta(seconds=1)),
        (classification_day.index[1], gti_day.index.max()),
    ]

    teoric_irradiances_list = [
        irradiance
        for begin, end in limits
        if (
            irradiance := get_period_irradiances(
                gti_day, classification_day, begin, end
            )
        )
        is not None
    ]

    if teoric_irradiances_list:
        return pd.concat(teoric_irradiances_list)


def generate_teoric_irradiance(solar_plant: str, avg: bool) -> None:
    log = structlog.get_logger()

    status = "avg" if avg else "original"

    log.info("Gerando variável", var="GTI teórico", AVG=True if avg else False)

    gti = pd.read_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"][f"gti_{status}"])
    classification = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"]["classification"]
    ).drop(columns=["GHI"])

    begin = gti.index.min()
    end = gti.index.max()

    date_range = pd.date_range(begin, end, freq="D")

    teoric_irradiance_list = Parallel(n_jobs=-1)(
        delayed(process_day)(gti, classification, date) for date in date_range
    )

    teoric_irradiance = pd.concat(teoric_irradiance_list)

    path = PLANTS_PARAM[solar_plant]["datawarehouse"][f"teoric_irradiance_{status}"]

    teoric_irradiance.to_parquet(path)

    log.info("Dados salvos", filename=path)
