import json

import pandas as pd
from joblib import Parallel, delayed

PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))


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

    gti_period = gti_period[classification_period.columns]

    max_irradiances = gti_period.max(axis=1)
    mean_irradiance = gti_period.mean(axis=1)
    median_irradiance = gti_period.median(axis=1)

    return pd.DataFrame(
        {
            "GTI teórica máxima": max_irradiances,
            "GTI teórica média": mean_irradiance,
            "GTI teórica mediana": median_irradiance,
        }
    )


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


def generate_teoric_irradiances(solar_plant: str) -> None:
    gti_avg = pd.read_parquet(PLANTS_PARAM[solar_plant]["gti_avg"])
    gti_original = pd.read_parquet(PLANTS_PARAM[solar_plant]["gti_original"])

    classification = pd.read_parquet(PLANTS_PARAM[solar_plant]["classification"])
    classification = classification.drop(columns=["GHI"])

    begin = gti_avg.index.min()
    end = gti_avg.index.max()

    date_range = pd.date_range(begin, end, freq="D")

    teoric_irradiances_list = Parallel(n_jobs=-1)(
        delayed(process_day)(gti_avg, classification, date) for date in date_range
    )

    teoric_irradiances = pd.concat(teoric_irradiances_list)

    teoric_irradiances.to_parquet(PLANTS_PARAM[solar_plant]["teoric_irradiances_avg"])

    teoric_irradiances_list = Parallel(n_jobs=-1)(
        delayed(process_day)(gti_original, classification, date) for date in date_range
    )

    teoric_irradiances = pd.concat(teoric_irradiances_list)

    teoric_irradiances.to_parquet(
        PLANTS_PARAM[solar_plant]["teoric_irradiances_original"]
    )


def generate_teoric_irradiances(solar_plant: str) -> None:
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

    gti_avg = pd.read_parquet(PLANTS_PARAM[solar_plant]["gti_avg"])
    gti_original = pd.read_parquet(PLANTS_PARAM[solar_plant]["gti_original"])
    classification = pd.read_parquet(PLANTS_PARAM[solar_plant]["classification"]).drop(
        columns=["GHI"]
    )

    date_range = pd.date_range(gti_avg.index.min(), gti_avg.index.max(), freq="D")

    process_irradiances(
        gti_avg,
        classification,
        PLANTS_PARAM[solar_plant]["teoric_irradiances_avg"],
        date_range,
    )
    process_irradiances(
        gti_original,
        classification,
        PLANTS_PARAM[solar_plant]["teoric_irradiances_original"],
        date_range,
    )


if __name__ == "__main__":
    generate_teoric_irradiances("Apolo")
