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

        log.info("Gerando variáveis:", var="clearsky")
        generate_clearsky(solar_plant)
        log.info(
            "Dados salvos",
            filename=PLANTS_PARAM[solar_plant]["datawarehouse"]["clearsky"],
        )

        log.info("Gerando variáveis:", var="GTI, GHI e potência CA", AVG=True)
        generate_gti_ghi_ca(solar_plant, True)
        log.info(
            "Dados salvos",
            gti_filename=PLANTS_PARAM[solar_plant]["datawarehouse"]["gti_avg"],
            ghi_filename=PLANTS_PARAM[solar_plant]["datawarehouse"]["ghi_avg"],
            ca_power_filename=PLANTS_PARAM[solar_plant]["datawarehouse"][
                "ca_power_avg"
            ],
        )

        log.info("Gerando variáveis:", var="GTI, GHI e potência CA", AVG=False)
        generate_gti_ghi_ca(solar_plant, False)
        log.info(
            "Dados salvos",
            gti_filename=PLANTS_PARAM[solar_plant]["datawarehouse"]["gti_original"],
            ghi_filename=PLANTS_PARAM[solar_plant]["datawarehouse"]["ghi_original"],
            ca_power_filename=PLANTS_PARAM[solar_plant]["datawarehouse"][
                "ca_power_original"
            ],
        )

        log.info("Gerando variáveis:", var="wind_speed e amb_temp")
        generate_wind_speed_amb_temp(solar_plant)
        log.info(
            "Dados salvos",
            wind_speed_filename=PLANTS_PARAM[solar_plant]["datawarehouse"][
                "wind_speed"
            ],
            amb_temp_filename=PLANTS_PARAM[solar_plant]["datawarehouse"]["amb_temp"],
        )

        log.info("Gerando variáveis:", var="classification")
        generate_classification(solar_plant)
        log.info(
            "Dados salvos",
            filename=PLANTS_PARAM[solar_plant]["datawarehouse"]["classification"],
        )

        log.info("Gerando variáveis:", var="GTI teórico", AVG=True)
        generate_teoric_irradiance(solar_plant, True)
        log.info(
            "Dados salvos",
            filename=PLANTS_PARAM[solar_plant]["datawarehouse"][
                "teoric_irradiance_avg"
            ],
        )

        log.info("Gerando variáveis:", var="GTI teórico", AVG=False)
        generate_teoric_irradiance(solar_plant, False)
        log.info(
            "Dados salvos",
            filename=PLANTS_PARAM[solar_plant]["datawarehouse"][
                "teoric_irradiance_original"
            ],
        )

        log.info("Gerando variáveis:", var="Potência teórica", AVG=True)
        generate_teoric_power(solar_plant, True)
        log.info(
            "Dados salvos",
            filename=PLANTS_PARAM[solar_plant]["datawarehouse"]["teoric_power_avg"],
        )

        log.info("Gerando variáveis:", var="Potência teórica", AVG=False)
        generate_teoric_power(solar_plant, False)
        log.info(
            "Dados salvos",
            filename=PLANTS_PARAM[solar_plant]["datawarehouse"][
                "teoric_power_original"
            ],
        )

        log.info("Gerando variáveis:", var="Potência teórica N")
        generate_stopped_trackers_power(solar_plant)
        log.info(
            "Dados salvos",
            filename=PLANTS_PARAM[solar_plant]["datawarehouse"][
                "stopped_trackers_power"
            ],
        )

        log.info("Gerando variáveis:", var="Data, CSI, Angulação (°), Perda (%)")
        generate_loss_table(solar_plant)
        log.info(
            "Dados salvos",
            filename=PLANTS_PARAM[solar_plant]["datawarehouse"]["loss_table"],
        )
