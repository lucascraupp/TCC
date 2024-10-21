import json

import pandas as pd
import pvpowerplants.plant as pvp

PLANTS_PARAM = json.load(open("resources/solar_plants.json"))
SU_DATA = json.load(open("pvIFSC/pvpowerplants/plants.json"))


def generate_helio_power(conditions: pd.DataFrame) -> pd.DataFrame:
    ivp_list = []
    su_map = {key: value for key, value in SU_DATA["plants"].items() if "SU" in key}

    for su, details in su_map.items():
        inverter = details["gen_units"][0]["inverter"]

        if "?????" not in inverter:
            ivp = pvp.power(
                plant_config=su,
                conditions=conditions,
                irradiance_source="gti",
                multiprocess=True,
            )["total"]

            ivp_list.append(ivp["Pac"])

    ivp = pd.concat(ivp_list, axis=1).sum(axis=1)

    return pd.DataFrame(ivp)


def generate_teoric_power(solar_plant: str, avg: bool) -> None:
    sufix = "avg" if avg else "original"

    teoric_irradiance = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"][f"teoric_irradiance_{sufix}"]
    )
    wind_speed = pd.read_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"]["wind_speed"]
    )
    amb_temp = pd.read_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"]["amb_temp"])

    conditions = pd.DataFrame(
        {
            "gti": teoric_irradiance[teoric_irradiance.columns[0]],
            "wind_speed": wind_speed[wind_speed.columns[0]],
            "air_temp": amb_temp[amb_temp.columns[0]],
        }
    )

    begin = teoric_irradiance.index.min()
    end = teoric_irradiance.index.max()

    tz = PLANTS_PARAM[solar_plant]["location"]["tz"]

    data_range = pd.date_range(start=begin, end=end, freq="10min", tz=tz)

    conditions = conditions.set_index(data_range)

    if solar_plant == "Hélio":
        ivp = generate_helio_power(conditions)
    else:
        ivp = pvp.power(
            PLANTS_PARAM[solar_plant]["name"],
            conditions,
            irradiance_source="gti",
            multiprocess=True,
        )["total"]

        ivp = ivp[["Pac"]]

    ivp = ivp.rename(columns={ivp.columns[0]: "Potência teórica"})

    ivp.index = ivp.index.tz_localize(None)

    ivp.to_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"][f"teoric_power_{sufix}"])
