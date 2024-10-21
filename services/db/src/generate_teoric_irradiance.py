import json

import pandas as pd
from joblib import Parallel, delayed

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))


def get_period_irradiances(
    gti: pd.DataFrame,
    classification: pd.DataFrame,
    begin: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    gti_period = gti.loc[begin:end]
    classification_period = classification.loc[begin:end]

    classification_period = classification_period.loc[
        :, (classification_period == "Disponível").all()
    ]

    if classification_period.empty:
        return pd.DataFrame(
            {"GTI teórico": 0},
            index=gti_period.index,
        )
    else:
        gti_period = gti_period[classification_period.columns]

        teoric_gti = gti_period.median(axis=1)

        return pd.DataFrame({"GTI teórico": teoric_gti})


def process_day(
    gti: pd.DataFrame,
    classification: pd.DataFrame,
    date: pd.Timestamp,
) -> pd.DataFrame:
    gti_day = gti.loc[gti.index.date == date.date()]
    classification_day = classification.loc[classification.index.date == date.date()]

    limits = [
        (gti_day.index.min(), classification_day.index[1] - pd.Timedelta(seconds=1)),
        (classification_day.index[1], gti_day.index.max()),
    ]

    teoric_irradiances_list = [
        get_period_irradiances(gti_day, classification_day, begin, end)
        for begin, end in limits
    ]

    return pd.concat(teoric_irradiances_list)


def process_irradiances(
    gti_data: pd.DataFrame,
    classification: pd.DataFrame,
    path: str,
    date_range: pd.DatetimeIndex,
) -> None:
    teoric_irradiances_list = Parallel(n_jobs=-1)(
        delayed(process_day)(gti_data, classification, date) for date in date_range
    )

    pd.concat(teoric_irradiances_list).to_parquet(path)


def generate_teoric_irradiance(solar_plant: str, moving_averange: bool) -> None:
    status = "avg" if moving_averange else "original"

    gti = pd.read_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"][f"gti_{status}"])
    classification = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"]["classification"]
    ).drop(columns=["GHI"])

    date_range = pd.date_range(gti.index.min(), gti.index.max(), freq="D")

    process_irradiances(
        gti,
        classification,
        PLANTS_PARAM[solar_plant]["datawarehouse"][f"teoric_irradiance_{status}"],
        date_range,
    )
