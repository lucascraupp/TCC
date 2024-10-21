import json
import math

import pandas as pd
import structlog
from joblib import Parallel, delayed

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))


def read_data(solar_plant: str, type_data: str) -> pd.DataFrame:
    path = PLANTS_PARAM[solar_plant]["datalake"][type_data]

    try:
        data = pd.read_parquet(path)

        data = data.set_index("timestamp")
        data.index = pd.to_datetime(data.index)

        data = data.apply(pd.to_numeric, errors="coerce")
    except FileNotFoundError:
        data = pd.DataFrame()

    return data


def round_minutes(hour: pd.Timestamp, percet: float) -> pd.Timedelta:
    hour_seconds = hour.total_seconds()
    # Arredondando o horário para baixo, para o múltiplo de 10 minutos mais próximo
    hour_seconds = math.floor(percet * hour_seconds / 600) * 600
    return pd.Timedelta(seconds=hour_seconds)


def calculate_period_limits(clearsky: pd.DataFrame) -> tuple[dict[str, tuple], tuple]:
    begin_irradiance = (clearsky[clearsky.columns[0]] > 0).idxmax()
    end_irradiance = (clearsky[clearsky.columns[0]] > 0).iloc[::-1].idxmax()
    max_irradiance = clearsky[clearsky.columns[0]].idxmax()

    begin_morning = max_irradiance - begin_irradiance
    begin_morning = round_minutes(begin_morning, 0.75)

    end_afternoon = end_irradiance - max_irradiance + pd.Timedelta(hours=10)
    end_afternoon = round_minutes(end_afternoon, 1.15)

    date = clearsky.index[0]

    irradiance_limits = {
        "morning": (
            date + begin_morning,
            date + pd.Timedelta(hours=12, minutes=0),
        ),
        "afternoon": (
            date + pd.Timedelta(hours=12, seconds=1),
            date + end_afternoon,
        ),
    }

    ghi_limits = (begin_irradiance, end_irradiance)

    return irradiance_limits, ghi_limits


def process_day(irradiance: pd.DataFrame, clearsky: pd.DataFrame) -> pd.DataFrame:
    limits, _ = calculate_period_limits(clearsky)

    mask = (limits["morning"][0] <= irradiance.index) & (
        irradiance.index <= limits["afternoon"][1]
    )
    irradiance_period = irradiance.copy()
    irradiance_period.loc[~mask] = 0

    return irradiance_period


def solar_filter(irradiance: pd.DataFrame, clearsky: pd.DataFrame) -> pd.DataFrame:
    begin = irradiance.index.min()
    end = irradiance.index.max()

    # Gerando uma lista de datas para o intervalo
    date_range = pd.date_range(begin, end, freq="D")

    # Processando os dias em paralelo usando joblib
    irradiance_filtered_list = Parallel(n_jobs=-1)(
        delayed(process_day)(
            irradiance[irradiance.index.date == date.date()],
            clearsky[clearsky.index.date == date.date()],
        )
        for date in date_range
    )

    irradiance_filtered = pd.concat(irradiance_filtered_list)

    return irradiance_filtered


def apply_filters(
    data: pd.DataFrame, avg: bool, clearsky: pd.DataFrame
) -> pd.DataFrame:
    data = solar_filter(data, clearsky)

    if avg:
        window = 11

        data = data.rolling(window=window).mean()
        data = data.shift(-((window - 1) // 2))

    data = data.fillna(0)

    return data


def generate_gti_ghi_ca(solar_plant: str, avg: bool) -> None:
    clearsky = pd.read_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"]["clearsky"])

    log = structlog.get_logger()

    for type_data in ["gti", "ghi", "ca_power"]:
        data = read_data(solar_plant, type_data)

        status = "avg" if avg else "original"

        log.info("Gerando variável", var=type_data, AVG=True if avg else False)

        data = apply_filters(data, avg, clearsky)

        match (type_data):
            case "gti":
                data.columns = [
                    f"Piranômetro {chr(65 + i)}" for i in range(data.shape[1])
                ]
            case "ghi":
                data.columns = ["GHI"]
            case "ca_power":
                data.columns = ["Potência CA"]

        path = PLANTS_PARAM[solar_plant]["datawarehouse"][f"{type_data}_{status}"]

        data.to_parquet(path)

        log.info("Dados salvos", filename=path)
