import json
import math
import os
import sys

sys.path.append("services/")

import pandas as pd
from joblib import Parallel, delayed
from pvlib.location import Location

PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))


def read_data(solar_plant: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    path = f"services/resources/datalake/{solar_plant}"

    gti = pd.read_parquet(f"{path}/gti.parquet")
    ghi = pd.read_parquet(f"{path}/ghi.parquet")

    if os.path.exists(f"{path}/ca_power.parquet"):
        ca_power = pd.read_parquet(f"{path}/ca_power.parquet")
        ca_power = ca_power.set_index("timestamp")
        ca_power = ca_power.apply(pd.to_numeric, errors="coerce")
    else:
        ca_power = pd.DataFrame()

    gti = gti.set_index("timestamp")
    ghi = ghi.set_index("timestamp")

    gti = gti.apply(pd.to_numeric, errors="coerce")
    ghi = ghi.apply(pd.to_numeric, errors="coerce")

    return gti, ghi, ca_power


def get_location(solar_plant: str) -> Location:
    match (solar_plant):
        case "CFPA":
            latitude = -17.22129
            longitude = -47.08851
            tz = "Brazil/East"
            altitude = 698.7
        case "FVAE":
            latitude = -5.571281337989798
            longitude = -37.02877824468482
            tz = "Brazil/East"
            altitude = 71

    return Location(latitude, longitude, tz=tz, altitude=altitude)


def get_clear_sky(solar_plant: str, date: pd.Timestamp) -> pd.Series:
    loc = PLANTS_PARAM[solar_plant]["location"]

    times = pd.date_range(
        start=date,
        end=date + pd.Timedelta(days=1),
        freq="10min",
        inclusive="left",
        tz=loc["tz"],
    )

    location = Location(
        latitude=loc["latitude"],
        longitude=loc["longitude"],
        tz=loc["tz"],
        altitude=loc["altitude"],
    )

    clearsky = location.get_clearsky(times)
    clearsky.index = pd.to_datetime(clearsky.index)
    clearsky.index = clearsky.index.tz_localize(None)

    return clearsky["ghi"]


def calculate_period_limits(solar_plant: str, date: pd.Timestamp) -> dict[str, tuple]:
    clearsky = get_clear_sky(solar_plant, date)

    begin_irradiance = (clearsky > 0).idxmax()
    end_irradiance = (clearsky > 0).iloc[::-1].idxmax()
    max_irradiance = clearsky.idxmax()

    def round_minutes(hour: pd.Timestamp, percet: float) -> pd.Timedelta:
        hour_seconds = hour.total_seconds()
        # Arredondando o horário para baixo, para o múltiplo de 10 minutos mais próximo
        hour_seconds = math.floor(percet * hour_seconds / 600) * 600
        return pd.Timedelta(seconds=hour_seconds)

    begin_morning = max_irradiance - begin_irradiance
    begin_morning = round_minutes(begin_morning, 0.75)

    end_afternoon = end_irradiance - max_irradiance + pd.Timedelta(hours=10)
    end_afternoon = round_minutes(end_afternoon, 1.15)

    return {
        "morning": (
            date + begin_morning,
            date + pd.Timedelta(hours=12, minutes=0),
        ),
        "afternoon": (
            date + pd.Timedelta(hours=12, seconds=1),
            date + end_afternoon,
        ),
    }


def process_day(
    solar_plant: str, date: pd.Timestamp, irradiance: pd.DataFrame
) -> pd.DataFrame:
    limits = calculate_period_limits(solar_plant, date)
    irradiance_day = irradiance[irradiance.index.date == date.date()]

    mask = (limits["morning"][0] <= irradiance_day.index) & (
        irradiance_day.index <= limits["afternoon"][1]
    )
    irradiance_period = irradiance_day.copy()
    irradiance_period.loc[~mask] = 0

    return irradiance_period


def sun_filter(
    solar_plant: str, irradiance: pd.DataFrame, n_jobs: int = -1
) -> pd.DataFrame:
    begin = irradiance.index.min()
    end = irradiance.index.max()

    # Gerando uma lista de datas para o intervalo
    dates = pd.date_range(begin, end, freq="D")

    # Processando os dias em paralelo usando joblib
    irradiance_filtered_list = Parallel(n_jobs=n_jobs)(
        delayed(process_day)(solar_plant, date, irradiance) for date in dates
    )

    irradiance_filtered = pd.concat(irradiance_filtered_list)

    return irradiance_filtered


def apply_filters(solar_plant: str, data: pd.DataFrame, avg: bool) -> pd.DataFrame:
    data = sun_filter(solar_plant, data)

    if avg:
        window = 11

        data = data.rolling(window=window).mean()
        data = data.shift(-((window - 1) // 2))

    data = data.fillna(0)

    return data


def generate_gti_ghi_ca(solar_plant: str, moving_averange: bool) -> None:
    status = "avg" if moving_averange else "original"

    gti, ghi, ca_power = read_data(solar_plant)

    gti = apply_filters(solar_plant, gti, moving_averange)
    ghi = apply_filters(solar_plant, ghi, moving_averange)

    gti.columns = [f"Piranômetro {chr(65 + i)}" for i in range(gti.shape[1])]
    ghi.columns = ["GHI"]

    gti.to_parquet(PLANTS_PARAM[solar_plant][f"gti_{status}"])
    ghi.to_parquet(PLANTS_PARAM[solar_plant][f"ghi_{status}"])

    if not ca_power.empty:
        ca_power = apply_filters(solar_plant, ca_power, moving_averange)

        ca_power.columns = ["Potência CA"]

        ca_power.to_parquet(PLANTS_PARAM[solar_plant][f"ca_power_{status}"])
