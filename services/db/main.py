import json

import structlog
from src.generate_classification import generate_classification
from src.generate_clearsky import generate_clearsky
from src.generate_gti_ghi_ca import generate_gti_ghi_ca
from src.generate_loss_table import generate_loss_table
from src.generate_stopped_trackers_power import generate_stopped_trackers_power
from src.generate_teoric_irradiance import generate_teoric_irradiance
from src.generate_teoric_power import generate_teoric_power
from src.generate_wind_speed_amb_temp import generate_wind_speed_amb_temp

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))

if __name__ == "__main__":
    log = structlog.get_logger()

    for solar_plant in PLANTS_PARAM.keys():
        log.info(f"Populando os dados da usina {solar_plant}...\n")

        generate_clearsky(solar_plant)

        generate_gti_ghi_ca(solar_plant, True)
        generate_gti_ghi_ca(solar_plant, False)

        generate_wind_speed_amb_temp(solar_plant)

        generate_classification(solar_plant)

        generate_teoric_irradiance(solar_plant, True)
        generate_teoric_irradiance(solar_plant, False)

        generate_teoric_power(solar_plant, True)
        generate_teoric_power(solar_plant, False)

        generate_stopped_trackers_power(solar_plant)

        generate_loss_table(solar_plant)

        print("\n")
