import os

from src.irradiance_data import populate_irradiance_data

USINE_LIST = os.environ["USINE_LIST"].split(", ")

if __name__ == "__main__":
    for usine in USINE_LIST:
        populate_irradiance_data(usine, True)
        populate_irradiance_data(usine, False)
