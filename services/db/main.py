import json

from src.generate_classification import generate_classification
from src.generate_clearsky import generate_clearsky
from src.generate_gti_ghi_ca import generate_gti_ghi_ca
from src.generate_teoric_irradiance import generate_teoric_irradiance
from src.generate_teoric_power import generate_teoric_power
from src.generate_wind_speed_amb_temp import generate_wind_speed_amb_temp

PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))

if __name__ == "__main__":
    for solar_plant in PLANTS_PARAM.keys():
        print(f"\nPopulando os dados da usina {solar_plant}...\n")

        print("\nPopulando os dados com média móvel...\n")
        generate_gti_ghi_ca(solar_plant, True)

        print("\nPopulando os dados sem média móvel...\n")
        generate_gti_ghi_ca(solar_plant, False)

        print("\nPopulando os dados de céu limpo...\n")
        generate_clearsky(solar_plant)

        print("\nPopulando a classificação...\n")
        generate_classification(solar_plant)

        print("\nPopulando a irradância teórica...\n")
        generate_teoric_irradiance(solar_plant)

        print("\nPouplando a velocidade do vento e temperatura ambiente...\n")
        generate_wind_speed_amb_temp(solar_plant)

        print("\nPopulando a potência teórica com média móvel...\n")
        generate_teoric_power(solar_plant, True)

        print("\nPopulando a potência teórica sem média móvel...\n")
        generate_teoric_power(solar_plant, False)
