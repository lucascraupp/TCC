import os

from src.irradiance_data import populate_solar_plant_data

USINE_LIST = [
    f
    for f in os.listdir("services/resources/datalake")
    if os.path.isdir(os.path.join("services/resources/datalake", f))
]

if __name__ == "__main__":
    for usine in USINE_LIST:
        populate_solar_plant_data(usine, True)
        populate_solar_plant_data(usine, False)
