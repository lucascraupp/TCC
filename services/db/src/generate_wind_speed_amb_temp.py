import json

import pandas as pd

PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))


def process_data(solar_plant: str, type_data: str) -> pd.DataFrame:
    data = pd.read_parquet(PLANTS_PARAM[solar_plant]["datalake"][type_data])

    data = data.set_index("timestamp")
    data.index = pd.to_datetime(data.index)

    data = data.apply(pd.to_numeric, errors="coerce")

    data = data.fillna(0)

    type_data = data.columns[0].split("\\")[-1]

    data = data.rename(columns={data.columns[0]: type_data})

    return data


def generate_wind_speed_amb_temp(solar_plant: str) -> None:
    for type_data in ["wind_speed", "amb_temp"]:
        data = process_data(solar_plant, type_data)

        data.to_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"][type_data])
