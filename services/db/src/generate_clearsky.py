import json

import pandas as pd
from joblib import Parallel, delayed
from pvlib.location import Location

PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))


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

    return clearsky[["ghi"]]


def generate_clearsky(solar_plant: str) -> None:
    ghi = pd.read_parquet(PLANTS_PARAM[solar_plant]["datalake"]["ghi"])

    begin = ghi["timestamp"].min()
    end = ghi["timestamp"].max()

    dates = pd.date_range(start=begin, end=end, freq="D")

    clearsky = Parallel(n_jobs=-1)(
        delayed(get_clear_sky)(solar_plant, date) for date in dates
    )

    clearsky = pd.concat(clearsky)
    clearsky = clearsky.rename(
        columns={"index": "timestamp", "ghi": "GHI teórico (clearsky)"}
    )

    clearsky.to_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"]["clearsky"])
