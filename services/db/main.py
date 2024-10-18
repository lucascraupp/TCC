import json

from src.generate_classification import generate_classification
from src.generate_clearsky import generate_clearsky
from src.generate_gti_ghi_ca import generate_gti_ghi_ca
from src.generate_loss_table import generate_loss_table
from src.generate_stopped_trackers_power import generate_stopped_trackers_power
from src.generate_teoric_irradiance import generate_teoric_irradiance
from src.generate_teoric_power import generate_teoric_power
from src.generate_wind_speed_amb_temp import generate_wind_speed_amb_temp

PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))

if __name__ == "__main__":
    for solar_plant in PLANTS_PARAM.keys():
        print(f"\nPopulando os dados da usina {solar_plant}...\n")

        print("Populando os dados de céu limpo...")
        generate_clearsky(solar_plant)

        print("Populando os dados de GTI, GHI e potência CA com média móvel...")
        generate_gti_ghi_ca(solar_plant, True)

        print("Populando os dados de GTI, GHI e potência CA sem média móvel...")
        generate_gti_ghi_ca(solar_plant, False)

        print("Pouplando a velocidade do vento e temperatura ambiente...")
        generate_wind_speed_amb_temp(solar_plant)

        print("Populando a classificação...")
        generate_classification(solar_plant)

        print("Populando a irradância teórica...")
        generate_teoric_irradiance(solar_plant)

        print("Populando a potência teórica com média móvel...")
        generate_teoric_power(solar_plant, True)

        print("Populando a potência teórica sem média móvel...")
        generate_teoric_power(solar_plant, False)

        print("Populando a potência dos trackers parados...")
        generate_stopped_trackers_power(solar_plant)

        print("Populando a tabela de perdas...")
        generate_loss_table(solar_plant)

        print(f"\nDados da usina {solar_plant} populados com sucesso!\n")
