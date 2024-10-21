import json
import os

import pandas as pd
import pvpowerplants.plant as pvp
import structlog

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))
SU_DATA = json.load(open("pvIFSC/pvpowerplants/plants.json"))

BEGIN_ANGLE = -60
END_ANGLE = 60
ANGLE_STEP = 5


def generate_helio_power(conditions: pd.DataFrame, angle: int) -> pd.DataFrame:
    ivp_list = []
    su_map = {key: value for key, value in SU_DATA["plants"].items() if "SU" in key}

    for su, details in su_map.items():
        inverter = details["gen_units"][0]["inverter"]
        n_strings = sum(details["gen_units"][0]["n_strings_per_input"])

        if "?????" not in inverter:
            fault_tracker = [(n_strings, angle)]

            ivp = pvp.power(
                plant_config=su,
                conditions=conditions,
                fault_tracker=fault_tracker,
                multiprocess=True,
            )["total"]

            ivp_list.append(ivp["Pac"])

    ivp = pd.concat(ivp_list, axis=1).sum(axis=1)

    return pd.DataFrame(ivp)


def select_power_plant(
    solar_plant: str, conditions: pd.DataFrame, angle: int
) -> pd.DataFrame:
    if solar_plant == "Hélio":
        ivp = generate_helio_power(conditions, angle)
    else:
        fault_tracker = [(PLANTS_PARAM[solar_plant]["n_strings"], angle)]

        ivp = pvp.power(
            PLANTS_PARAM[solar_plant]["name"],
            conditions,
            fault_tracker=fault_tracker,
            multiprocess=True,
        )["total"]

        ivp = ivp[["Pac"]]

    ivp = ivp.rename(columns={ivp.columns[0]: f"Potência teórica {angle}°"})

    return ivp


def generate_stopped_trackers_power(solar_plant: str) -> None:
    ghi = pd.read_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"]["ghi_avg"])
    wind_speed = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"]["wind_speed"]
    )
    amb_temp = pd.read_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"]["amb_temp"])

    conditions = pd.DataFrame(
        {
            "ghi": ghi[ghi.columns[0]],
            "wind_speed": wind_speed[wind_speed.columns[0]],
            "air_temp": amb_temp[amb_temp.columns[0]],
        }
    )

    begin = conditions.index.min()
    end = conditions.index.max()

    tz = PLANTS_PARAM[solar_plant]["location"]["tz"]

    date_range = pd.date_range(start=begin, end=end, freq="10min", tz=tz)

    conditions = conditions.set_index(date_range)

    angle_list = list(range(BEGIN_ANGLE, END_ANGLE + ANGLE_STEP, ANGLE_STEP))

    path = PLANTS_PARAM[solar_plant]["datawarehouse"]["stopped_trackers_power"]

    if os.path.exists(path):
        ivp = pd.read_parquet(path)
    else:
        ivp = pd.DataFrame()

    log = structlog.get_logger()

    for angle in angle_list:
        if not f"Potência teórica {angle}°" in ivp.columns:
            log.info(f"Gerando potência teórica para ângulo {angle}°...")

            ivp_angle = select_power_plant(solar_plant, conditions, angle)

            ivp_angle.index = ivp_angle.index.tz_localize(None)

            ivp = pd.concat([ivp, ivp_angle], axis=1)

            ivp.to_parquet(path)
        else:
            log.info(f"Potência teórica para ângulo {angle}° já existe")

    log.info("Dados salvos", filename=path)
