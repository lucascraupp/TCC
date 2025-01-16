import json

import pandas as pd
import structlog

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))


def process_data(solar_plant: str, type_data: str) -> pd.DataFrame:
    data = pd.read_parquet(PLANTS_PARAM[solar_plant]["datalake"][type_data])

    data = data.set_index("timestamp")
    data.index = pd.to_datetime(data.index)

    data = data.apply(pd.to_numeric, errors="coerce")

    type_data = data.columns[0].split("\\")[-1]

    data = data.rename(columns={data.columns[0]: type_data})

    return data


def generate_wind_speed_amb_temp(solar_plant: str) -> None:
    log = structlog.get_logger()

    for type_data in ["wind_speed", "amb_temp"]:
        log.info("Gerando variável", var=type_data)

        data = process_data(solar_plant, type_data)

        path = PLANTS_PARAM[solar_plant]["datawarehouse"][type_data]

        data.to_parquet(path)

        log.info("Dados salvos", filename=path)
