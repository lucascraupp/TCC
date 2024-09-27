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
USINES_PARAM = json.load(open("services/resources/solar_usines.json"))


# Função otimizada para buscar IDs e evitar repetidas consultas ao banco de dados
def get_cached_id(cache, key, query, cursor, params) -> int | None:
    if key not in cache:
        cursor.execute(query, params)
        result = cursor.fetchone()
        cache[key] = result[0] if result else None
    return cache[key]


def populate_solar_usine(
    solar_usine: str, cursor: pymysql.cursors.Cursor, cache: dict
) -> None:
    name = os.environ[solar_usine]
    key = f"solar_usine_{solar_usine}"

    if (
        get_cached_id(
            cache,
            key,
            "SELECT id_solar_usine FROM solar_usine WHERE nickname = %s",
            cursor,
            (solar_usine,),
        )
        is None
    ):
        cursor.execute(
            "INSERT INTO solar_usine (name, nickname) VALUES (%s, %s)",
            (name, solar_usine),
        )
        cursor.execute("SELECT LAST_INSERT_ID()")
        cache[key] = cursor.fetchone()[0]


def populate_park(
    solar_usine: str, park: str, cursor: pymysql.cursors.Cursor, cache: dict
) -> None:
    id_solar_usine = get_cached_id(
        cache,
        f"solar_usine_{solar_usine}",
        "SELECT id_solar_usine FROM solar_usine WHERE nickname = %s",
        cursor,
        (solar_usine,),
    )
    name = os.environ[park]
    key = f"park_{park}_{id_solar_usine}"

    if (
        get_cached_id(
            cache,
            key,
            "SELECT id_park FROM park WHERE nickname = %s AND id_solar_usine = %s",
            cursor,
            (park, id_solar_usine),
        )
        is None
    ):
        cursor.execute(
            "INSERT INTO park (id_solar_usine, name, nickname) VALUES (%s, %s, %s)",
            (id_solar_usine, name, park),
        )
        cursor.execute("SELECT LAST_INSERT_ID()")
        cache[key] = cursor.fetchone()[0]


def populate_sensor(
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


def populate_sensor_per_park(
    solar_usine: str,
    park: str,
    sensor: str,
    cursor: pymysql.cursors.Cursor,
    cache: dict,
) -> None:
    id_solar_usine = get_cached_id(
        cache,
        f"solar_usine_{solar_usine}",
        "SELECT id_solar_usine FROM solar_usine WHERE nickname = %s",
        cursor,
        (solar_usine,),
    )
    id_park = get_cached_id(
        cache,
        f"park_{park}_{id_solar_usine}",
        "SELECT id_park FROM park WHERE nickname = %s AND id_solar_usine = %s",
        cursor,
        (park, id_solar_usine),
    )
    id_sensor = get_cached_id(
        cache,
        f"sensor_{sensor}_sensor",
        "SELECT id_sensor FROM sensor WHERE name = %s",
        cursor,
        (sensor,),
    )

    key = f"sensor_per_park_{id_sensor}_{id_park}"
    if key not in cache:
        try:
            cursor.execute(
                "INSERT INTO sensor_per_park (id_sensor, id_park) VALUES (%s, %s)",
                (id_sensor, id_park),
            )
            cache[key] = True
        except:
            pass


def populate_status(status: str, cursor: pymysql.cursors.Cursor, cache: dict) -> None:
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


def populate_datetime(datetime: list, cursor: pymysql.cursors.Cursor) -> None:
    try:
        cursor.executemany(
            "INSERT INTO datetime (timestamp) VALUES (%s) ON DUPLICATE KEY UPDATE id_datetime=id_datetime",
            [(ts,) for ts in datetime],
        )
    except Exception as e:
        print(f"Error inserting datetime: {e}")


def get_id_timestamp(datetime: list, cursor: pymysql.cursors.Cursor) -> list:
    cursor.execute(
        "SELECT id_datetime FROM datetime WHERE timestamp BETWEEN %s AND %s",
        (datetime[0], datetime[-1]),
    )
    result = cursor.fetchall()
    return [row[0] for row in result]


def populate_data(
    timestamp: list,
    solar_usine: str,
    park: str,
    sensor: str,
    status: str,
    data_sensor: pd.Series,
    cursor: pymysql.cursors.Cursor,
    cache: dict,
) -> None:
    id_solar_usine = get_cached_id(
        cache,
        f"solar_usine_{solar_usine}",
        "SELECT id_solar_usine FROM solar_usine WHERE nickname = %s",
        cursor,
        (solar_usine,),
    )
    id_park = get_cached_id(
        cache,
        f"park_{park}_{id_solar_usine}",
        "SELECT id_park FROM park WHERE nickname = %s AND id_solar_usine = %s",
        cursor,
        (park, id_solar_usine),
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
    id_timestamp = get_id_timestamp(timestamp, cursor)

    values = [
        (id_time, id_park, id_sensor, id_status, value)
        for id_time, value in zip(id_timestamp, data_sensor.values)
    ]
    cursor.executemany(
        "INSERT IGNORE INTO data (id_datetime, id_park, id_sensor, id_status, value) VALUES (%s, %s, %s, %s, %s)",
        values,
    )


def populate_db(
    data: pd.DataFrame, status: str, cursor: pymysql.cursors.Cursor
) -> None:
    cache = {}
    datetime = data.index.to_list()

    # Popula a tabela datetime e busca todos os ids de timestamp
    populate_datetime(datetime, cursor)

    for sensor in data.columns:
        data_sensor = data[sensor]

        # Divisão do sensor
        split_sensor = sensor.split("\\")
        solar_usine = split_sensor[0]
        park = split_sensor[1]
        type_sensor = split_sensor[2]  # Aqui estamos usando o tipo do sensor
        name = "\\".join(split_sensor[3:]) if len(split_sensor) > 3 else type_sensor

        print(f"Populando o sensor {name} com tipo {type_sensor}...")

        # Popula as outras tabelas
        populate_solar_usine(solar_usine, cursor, cache)
        populate_park(solar_usine, park, cursor, cache)
        populate_sensor(name, type_sensor, cursor, cache)
        populate_sensor_per_park(solar_usine, park, name, cursor, cache)
        populate_status(status, cursor, cache)

        # Popula a tabela data
        populate_data(
            datetime, solar_usine, park, name, status, data_sensor, cursor, cache
        )


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


def read_data(solar_usine: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    path = f"services/resources/datalake/{solar_usine}"

    gti = pd.read_parquet(f"{path}/gti.parquet")
    ghi = pd.read_parquet(f"{path}/ghi.parquet")

    gti = gti.set_index("Timestamp")
    ghi = ghi.set_index("Timestamp")

    gti = gti.apply(pd.to_numeric, errors="coerce")
    ghi = ghi.apply(pd.to_numeric, errors="coerce")

    return gti, ghi


def get_location(solar_usine: str) -> Location:
    match (solar_usine):
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


def get_clear_sky(solar_usine: str, date: pd.Timestamp) -> pd.Series:
    loc = USINES_PARAM[solar_usine]["location"]

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


def calculate_period_limits(solar_usine: str, date: pd.Timestamp) -> dict[str, tuple]:
    clearsky = get_clear_sky(solar_usine, date)

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
    solar_usine: str, date: pd.Timestamp, irradiance: pd.DataFrame
) -> pd.DataFrame:
    limits = calculate_period_limits(solar_usine, date)
    irradiance_day = irradiance[irradiance.index.date == date.date()]

    mask = (limits["morning"][0] <= irradiance_day.index) & (
        irradiance_day.index <= limits["afternoon"][1]
    )
    irradiance_period = irradiance_day.copy()
    irradiance_period.loc[~mask] = 0

    return irradiance_period


def sun_filter(
    solar_usine: str, irradiance: pd.DataFrame, n_jobs: int = -1
) -> pd.DataFrame:
    begin = irradiance.index.min()
    end = irradiance.index.max()

    # Gerar uma lista de datas para o intervalo
    dates = pd.date_range(begin, end, freq="D")

    # Processar os dias em paralelo usando joblib
    irradiance_filtered_list = Parallel(n_jobs=n_jobs)(
        delayed(process_day)(solar_usine, date, irradiance) for date in dates
    )

    # Concatenar os resultados no final
    irradiance_filtered = pd.concat(irradiance_filtered_list)

    return irradiance_filtered


def populate_irradiance_data(solar_usine: str, moving_averange: bool) -> None:
    status = "Tratado" if moving_averange else "Original"

    print("Iniciando o tratamento dos dados...")

    gti, ghi = read_data(solar_usine)

    gti = sun_filter(solar_usine, gti)
    ghi = sun_filter(solar_usine, ghi)

    if moving_averange:
        window = 11

        gti = gti.rolling(window=window).mean()
        ghi = ghi.rolling(window=window).mean()

        gti = gti.shift(-((window - 1) // 2))
        ghi = ghi.shift(-((window - 1) // 2))

    gti = gti.fillna(0)
    ghi = ghi.fillna(0)

    print("Iniciando a população do banco de dados GTI...")
    populate(gti, status)

    print("Iniciando a população do banco de dados GHI...")
    populate(ghi, status)
