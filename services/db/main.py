import os

from src.generate_clearsky import generate_clearsky
from src.generate_gti_ghi_ca import generate_gti_ghi_ca

USINE_LIST = [
    f
    for f in os.listdir("services/resources/datalake")
    if os.path.isdir(os.path.join("services/resources/datalake", f))
]


if __name__ == "__main__":
    for usine in USINE_LIST:
        print(f"\nPopulando os dados da usina {usine}...\n")

        print("\nPopulando os dados com média móvel...\n")
        generate_gti_ghi_ca(usine, True)

        print("\nPopulando os dados sem média móvel...\n")
        generate_gti_ghi_ca(usine, False)

        print("\nPopulando os dados de céu limpo...\n")
        generate_clearsky(usine)
