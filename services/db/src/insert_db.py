import concurrent.futures
import json

import pandas as pd
from src.access_data import *

PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))


def insert_solar_plant_data(data: pd.DataFrame, status: str):
    id_status = Status.get_or_create(name=status)[0].get_id()

    # Função para processar cada sensor
    def process_sensor(sensor: str):
        data_sensor = data[sensor]

        # Divisão do sensor
        split_sensor = sensor.split("\\")
        solar_plant = split_sensor[0]
        type_sensor = split_sensor[1]
        name = "\\".join(split_sensor[2:])

        print(
            f"Populando o sensor {name} do parque {solar_plant} com tipo {type_sensor}..."
        )

        # Obter ou criar os IDs relevantes
        id_solar_plant = SolarPlant.get_or_create(name=solar_plant)[0].get_id()
        id_sensor = Sensor.get_or_create(name=name, type=type_sensor)[0].get_id()
        id_solar_plant_has_sensor = SolarPlantHasSensor.get_or_create(
            id_solar_plant=id_solar_plant, id_sensor=id_sensor
        )[0].get_id()

        # Inserir múltiplas linhas de uma vez para o sensor
        SolarPlantData.insert_many(
            [
                {
                    "id_solar_plant": id_solar_plant_has_sensor[0],
                    "id_sensor": id_solar_plant_has_sensor[1],
                    "id_status": id_status,
                    "timestamp": timestamp,
                    "value": value,
                }
                for timestamp, value in zip(data_sensor.index, data_sensor.values)
            ]
        ).execute()

    # Utilizando ThreadPoolExecutor para processar múltiplas colunas (sensores) em paralelo
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_sensor, data.columns)
