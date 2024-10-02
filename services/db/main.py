import os

from src.populate_solar_plant_data import populate_solar_plant_data

USINE_LIST = [
    f
    for f in os.listdir("services/resources/datalake")
    if os.path.isdir(os.path.join("services/resources/datalake", f))
]

if __name__ == "__main__":
    for usine in USINE_LIST:
        print(f"\nPopulando os dados da usina {usine}...\n")

        print("\nPopulando os dados com média móvel...\n")
        populate_solar_plant_data(usine, True)

        print("\nPopulando os dados sem média móvel...\n")
        populate_solar_plant_data(usine, False)
