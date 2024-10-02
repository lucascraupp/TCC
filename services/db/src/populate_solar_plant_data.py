import json
import math
import os

import pandas as pd
import pymysql
import pymysql.cursors
from joblib import Parallel, delayed
from pvlib.location import Location

HOST = os.environ["DB_HOST"]
USER = os.environ["DB_USER"]
PASSWORD = os.environ["DB_PASSWORD"]
DATABASE = os.environ["DB_DATABASE"]
PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))


def get_cached_id(cache, key, query, cursor, params) -> int | None:
    if key not in cache:
        cursor.execute(query, params)
        result = cursor.fetchone()
        cache[key] = result[0] if result else None
    return cache[key]


def insert_solar_plant(
    solar_plant: str, cursor: pymysql.cursors.Cursor, cache: dict
) -> None:
    key = f"solar_plant_{solar_plant}"

    if (
        get_cached_id(
            cache,
            key,
            "SELECT id_solar_plant FROM solar_plant WHERE name = %s",
            cursor,
            (solar_plant,),
        )
        is None
    ):
        cursor.execute(
            "INSERT INTO solar_plant (name) VALUES (%s)",
            (solar_plant),
        )
        cursor.execute("SELECT LAST_INSERT_ID()")
        cache[key] = cursor.fetchone()[0]


def insert_sensor(
    name: str, type_sensor: str, cursor: pymysql.cursors.Cursor, cache: dict
) -> None:
    key = f"sensor_{name}_{type_sensor}"

    if (
        get_cached_id(
            cache,
            key,
            "SELECT id_sensor FROM sensor WHERE name = %s AND type = %s",
            cursor,
            (name, type_sensor),
        )
        is None
    ):
        cursor.execute(
            "INSERT INTO sensor (name, type) VALUES (%s, %s)",
            (name, type_sensor),
        )
        cursor.execute("SELECT LAST_INSERT_ID()")
        cache[key] = cursor.fetchone()[0]


def insert_solar_plant_has_sensor(
    solar_plant: str,
    sensor: str,
    cursor: pymysql.cursors.Cursor,
    cache: dict,
) -> None:
    id_solar_plant = get_cached_id(
        cache,
        f"solar_plant_{solar_plant}",
        "SELECT id_solar_plant FROM solar_plant WHERE nickname = %s",
        cursor,
        (solar_plant),
    )
    id_sensor = get_cached_id(
        cache,
        f"sensor_{sensor}_sensor",
        "SELECT id_sensor FROM sensor WHERE name = %s",
        cursor,
        (sensor),
    )

    key = f"solar_plant_has_sensor_{solar_plant}_{id_sensor}"
    if key not in cache:
        try:
            cursor.execute(
                "INSERT INTO solar_plant_has_sensor (id_solar_plant, id_sensor) VALUES (%s, %s)",
                (id_solar_plant, id_sensor),
            )
            cache[key] = True
        except:
            pass


def insert_status(status: str, cursor: pymysql.cursors.Cursor, cache: dict) -> None:
    key = f"status_{status}"

    if (
        get_cached_id(
            cache,
            key,
            "SELECT id_status FROM status WHERE status = %s",
            cursor,
            (status,),
        )
        is None
    ):
        cursor.execute("INSERT INTO status (status) VALUES (%s)", (status,))
        cursor.execute("SELECT LAST_INSERT_ID()")
        cache[key] = cursor.fetchone()[0]


def insert_solar_plant_data(
    solar_plant: str,
    sensor: str,
    status: str,
    data_sensor: pd.Series,
    cursor: pymysql.cursors.Cursor,
    cache: dict,
) -> None:
    id_solar_plant = get_cached_id(
        cache,
        f"solar_plant_{solar_plant}",
        "SELECT id_solar_plant FROM solar_plant WHERE nickname = %s",
        cursor,
        (solar_plant,),
    )
    id_sensor = get_cached_id(
        cache,
        f"sensor_{sensor}",
        "SELECT id_sensor FROM sensor WHERE name = %s",
        cursor,
        (sensor,),
    )
    id_status = get_cached_id(
        cache,
        f"status_{status}",
        "SELECT id_status FROM status WHERE status = %s",
        cursor,
        (status,),
    )

    values = [
        (id_solar_plant, id_sensor, id_status, timestamp, value)
        for timestamp, value in zip(data_sensor.index, data_sensor.values)
    ]
    cursor.executemany(
        "INSERT IGNORE INTO solar_plant_data (id_solar_plant, id_sensor, id_status, timestamp, value) VALUES (%s, %s, %s, %s, %s)",
        values,
    )


def populate_db(
    data: pd.DataFrame, status: str, cursor: pymysql.cursors.Cursor
) -> None:
    cache = {}

    for sensor in data.columns:
        data_sensor = data[sensor]

        # Divisão do sensor
        split_sensor = sensor.split("\\")
        solar_plant = split_sensor[0]
        type_sensor = split_sensor[1]
        name = "\\".join(split_sensor[2:])

        print(f"Populando o sensor {name} com tipo {type_sensor}...")

        # Popula as outras tabelas
        insert_solar_plant(solar_plant, cursor, cache)
        insert_sensor(name, type_sensor, cursor, cache)
        insert_solar_plant_has_sensor(solar_plant, name, cursor, cache)
        insert_status(status, cursor, cache)

        # Popula a tabela data
        insert_solar_plant_data(solar_plant, name, status, data_sensor, cursor, cache)


def populate(data: pd.DataFrame, status: str) -> None:
    connection = pymysql.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
    )

    try:
        with connection.cursor() as cursor:
            populate_db(data, status, cursor)
            connection.commit()
    finally:
        connection.close()


# ------------------------------------------------------------------------------------


def read_data(solar_plant: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    path = f"services/resources/datalake/{solar_plant}"

    gti = pd.read_parquet(f"{path}/gti.parquet")
    ghi = pd.read_parquet(f"{path}/ghi.parquet")

    if os.path.exists(f"{path}/ca_power.parquet"):
        ca_power = pd.read_parquet(f"{path}/ca_power.parquet")
        ca_power = ca_power.set_index("timestamp")
        ca_power = ca_power.apply(pd.to_numeric, errors="coerce")
    else:
        ca_power = pd.DataFrame()

    gti = gti.set_index("timestamp")
    ghi = ghi.set_index("timestamp")

    gti = gti.apply(pd.to_numeric, errors="coerce")
    ghi = ghi.apply(pd.to_numeric, errors="coerce")

    return gti, ghi, ca_power


def get_location(solar_plant: str) -> Location:
    match (solar_plant):
        case "CFPA":
            latitude = -17.22129
            longitude = -47.08851
            tz = "Brazil/East"
            altitude = 698.7
        case "FVAE":
            latitude = -5.571281337989798
            longitude = -37.02877824468482
            tz = "Brazil/East"
            altitude = 71

    return Location(latitude, longitude, tz=tz, altitude=altitude)


def get_clear_sky(solar_plant: str, date: pd.Timestamp) -> pd.Series:
    loc = PLANTS_PARAM[solar_plant]["location"]

    times = pd.date_range(
        start=date,
        end=date + pd.Timedelta(days=1),
        freq="10min",
        inclusive="left",
        tz=loc["tz"],
    )

    location = Location(
        latitude=loc["latitude"],
        longitude=loc["longitude"],
        tz=loc["tz"],
        altitude=loc["altitude"],
    )

    clearsky = location.get_clearsky(times)
    clearsky.index = pd.to_datetime(clearsky.index)
    clearsky.index = clearsky.index.tz_localize(None)

    return clearsky["ghi"]


def calculate_period_limits(solar_plant: str, date: pd.Timestamp) -> dict[str, tuple]:
    clearsky = get_clear_sky(solar_plant, date)

    begin_irradiance = (clearsky > 0).idxmax()
    end_irradiance = (clearsky > 0).iloc[::-1].idxmax()
    max_irradiance = clearsky.idxmax()

    def round_minutes(hour: pd.Timestamp, percet: float) -> pd.Timedelta:
        hour_seconds = hour.total_seconds()
        # Arredondando o horário para baixo, para o múltiplo de 10 minutos mais próximo
        hour_seconds = math.floor(percet * hour_seconds / 600) * 600
        return pd.Timedelta(seconds=hour_seconds)

    begin_morning = max_irradiance - begin_irradiance
    begin_morning = round_minutes(begin_morning, 0.75)

    end_afternoon = end_irradiance - max_irradiance + pd.Timedelta(hours=10)
    end_afternoon = round_minutes(end_afternoon, 1.15)

    return {
        "morning": (
            date + begin_morning,
            date + pd.Timedelta(hours=12, minutes=0),
        ),
        "afternoon": (
            date + pd.Timedelta(hours=12, seconds=1),
            date + end_afternoon,
        ),
    }


def process_day(
    solar_plant: str, date: pd.Timestamp, irradiance: pd.DataFrame
) -> pd.DataFrame:
    limits = calculate_period_limits(solar_plant, date)
    irradiance_day = irradiance[irradiance.index.date == date.date()]

    mask = (limits["morning"][0] <= irradiance_day.index) & (
        irradiance_day.index <= limits["afternoon"][1]
    )
    irradiance_period = irradiance_day.copy()
    irradiance_period.loc[~mask] = 0

    return irradiance_period


def sun_filter(
    solar_plant: str, irradiance: pd.DataFrame, n_jobs: int = -1
) -> pd.DataFrame:
    begin = irradiance.index.min()
    end = irradiance.index.max()

    # Gerando uma lista de datas para o intervalo
    dates = pd.date_range(begin, end, freq="D")

    # Processando os dias em paralelo usando joblib
    irradiance_filtered_list = Parallel(n_jobs=n_jobs)(
        delayed(process_day)(solar_plant, date, irradiance) for date in dates
    )

    irradiance_filtered = pd.concat(irradiance_filtered_list)

    return irradiance_filtered


def apply_filters(solar_plant: str, data: pd.DataFrame, avg: bool) -> pd.DataFrame:
    data = sun_filter(solar_plant, data)

    if avg:
        window = 11

        data = data.rolling(window=window).mean()
        data = data.shift(-((window - 1) // 2))

    data = data.fillna(0)

    return data


def populate_solar_plant_data(solar_plant: str, moving_averange: bool) -> None:
    status = "AVG" if moving_averange else "Original"

    print("Iniciando o tratamento dos dados...")

    gti, ghi, ca_power = read_data(solar_plant)

    gti = apply_filters(solar_plant, gti, moving_averange)
    ghi = apply_filters(solar_plant, ghi, moving_averange)

    print("Iniciando a população do GTI...")
    populate(gti, status)

    print("Iniciando a população do GHI...")
    populate(ghi, status)

    if not ca_power.empty:
        ca_power = apply_filters(solar_plant, ca_power, moving_averange)

        print("Iniciando a população da Potência CA...")
        populate(ca_power, status)
