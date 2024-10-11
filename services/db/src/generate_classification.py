import json
import math

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from pvlib.location import Location

PLANTS_PARAM = json.load(open("services/resources/solar_plants.json"))


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


def calculate_period_limits(
    solar_plant: str, date: pd.Timestamp
) -> tuple[dict[str, tuple], tuple]:
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

    irradiance_limits = {
        "morning": (
            date + begin_morning,
            date + pd.Timedelta(hours=12, minutes=0),
        ),
        "afternoon": (
            date + pd.Timedelta(hours=12, seconds=1),
            date + end_afternoon,
        ),
    }

    ghi_limits = (begin_irradiance, end_irradiance)

    return irradiance_limits, ghi_limits


def remove_sensors_without_data_and_variance(
    irradiance: pd.DataFrame, ghi_limits: tuple
) -> pd.DataFrame:
    irradiance_limited = irradiance.loc[ghi_limits[0] : ghi_limits[1]]

    # Removendo do DataFrame "irradiance", todos os sensores com valor 0 durante o período de GHI teórico
    irradiance = irradiance.loc[:, irradiance_limited.sum() > 0]

    # Verificação se os dados do sensor são constantes por, pelo menos, 4 amostras consecutivas
    same_value_for_a_time = irradiance_limited.rolling(window=4).apply(
        lambda x: x.nunique() == 1
    )
    irradiance = irradiance.loc[:, ~same_value_for_a_time.any()]

    value = (
        130  # Valor de referência para o desvio padrão ponderado (ajuste se necessário)
    )

    for sensor in irradiance.columns:
        irradiance_sensor = irradiance[[sensor]]

        data_time = irradiance_sensor.index.hour * 60 + irradiance_sensor.index.minute
        data_weights = irradiance_sensor[sensor].copy()

        # Cálculo da média ponderada dos valores
        weighted_mean = (data_time * data_weights).sum() / data_weights.sum()

        # Cálculo do desvio padrão ponderado
        squared_deviations = ((data_time - weighted_mean) ** 2) * data_weights
        weighted_variance = squared_deviations.sum() / data_weights.sum()
        weighted_standard_deviation = np.sqrt(weighted_variance)

        if weighted_standard_deviation > value:
            irradiance = irradiance.drop(columns=[sensor])

    return irradiance


def remove_sensors_different_from_the_reference(
    irradiance: pd.DataFrame,
) -> pd.DataFrame:
    reference = irradiance.mean(axis=1)

    max_distance = (
        5e3  # Valor de referência para a distância máxima (ajuste se necessário)
    )

    for sensor in irradiance.columns:
        irradiance_sensor = irradiance[[sensor]]

        sum_irradiance = irradiance_sensor.sum()
        sum_reference = reference.sum()

        distance = sum_irradiance - sum_reference

        if distance.min() < -max_distance:
            irradiance = irradiance.drop(columns=[sensor])

    return irradiance


def filter_data(
    irradiance: pd.DataFrame, ghi_limits: tuple, period: dict
) -> pd.DataFrame:
    data_period = irradiance.loc[period[0] : period[1]]

    data_with_variance = remove_sensors_without_data_and_variance(
        data_period, ghi_limits
    )

    data_filtered = remove_sensors_different_from_the_reference(data_with_variance)

    return data_filtered


def classify_period_without_irradiance(gti: pd.DataFrame) -> pd.DataFrame:
    classification = pd.DataFrame(columns=gti.columns, index=[gti.index.min()])

    gti_avg = gti.mean()
    max_avg = gti_avg.max()
    mean_max = gti_avg / max_avg

    for sensor in gti.columns:
        sensor_relation = mean_max[sensor]

        if sensor_relation > 0.8:  # Ajuste este valor conforme necessário
            classification[sensor] = "Disponível"
        else:
            classification[sensor] = "Stow"

    return classification


def classify_period_with_irradiance(
    gti: pd.DataFrame, ghi: pd.DataFrame
) -> pd.DataFrame:
    ghi = ghi.iloc[:, 0]
    ghi_sum = ghi.sum()

    # Limiar para identificar sensores próximos ao GHI
    ghi_threshold = 0.25 * ghi_sum

    # Removendo sensores próximos ao GHI do cálculo de máximo e média
    near_ghi_mask = gti.sub(ghi, axis=0).abs().sum() < ghi_threshold
    filtered_gti = gti.loc[:, ~near_ghi_mask]

    gti_max = gti.max(axis=1)
    gti_mean = gti.mean(axis=1) if filtered_gti.empty else filtered_gti.mean(axis=1)

    classification = pd.DataFrame(columns=gti.columns, index=[gti.index.min()])

    for sensor in gti.columns:
        irradiance_sensor = gti[sensor]

        # Cálculo do erro médio quadrático
        mse1 = ((irradiance_sensor - ghi) ** 2).sum()
        mse2 = ((irradiance_sensor - gti_max) ** 2).sum()
        mse3 = ((irradiance_sensor - gti_mean) ** 2).sum()

        # Em casos onde apenas um sensor está disponível, deve-se observar se o valor da curva GTI está muito próximo ao GHI
        diff_ghi = abs(irradiance_sensor - ghi).sum()
        diff_mean = abs(irradiance_sensor - gti_mean).sum()

        condition_1 = (mse1 - mse3) ** 2 < (mse2 - mse3) ** 2 or mse1 > mse2
        condition_2 = diff_ghi < 0.025 * ghi_sum and diff_mean < 0.025 * gti_mean.sum()

        if condition_1 and not condition_2:
            classification[sensor] = "Disponível"
        else:
            classification[sensor] = "Stow"

    return classification


def process_day(
    solar_plant: str, gti: pd.DataFrame, ghi: pd.DataFrame, date: pd.Timestamp
) -> pd.DataFrame:
    gti_day = gti.loc[gti.index.date == date.date()]
    ghi_day = ghi.loc[ghi.index.date == date.date()]

    irradiance_limits, ghi_limits = calculate_period_limits(solar_plant, date)

    classification = pd.DataFrame()

    for period in irradiance_limits.values():
        gti_filtered = filter_data(gti_day, ghi_limits, period)
        ghi_filtered = filter_data(ghi_day, ghi_limits, period)

        if ghi_filtered.empty:
            period_classification = classify_period_without_irradiance(gti_filtered)

            period_classification[ghi.columns] = "Indisponível"
        else:
            period_classification = classify_period_with_irradiance(
                gti_filtered, ghi_filtered
            )

            period_classification[ghi.columns] = "Disponível"

        if classification.empty:
            classification = period_classification
        else:
            classification = pd.concat([classification, period_classification])

    return classification


def generate_classification(solar_plant: str) -> None:
    gti = pd.read_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"]["gti_avg"])
    ghi = pd.read_parquet(PLANTS_PARAM[solar_plant]["datawarehouse"]["ghi_avg"])

    begin = gti.index.min()
    end = gti.index.max()

    date_range = pd.date_range(begin, end, freq="D")

    classification_list = Parallel(n_jobs=-1)(
        delayed(process_day)(solar_plant, gti, ghi, date) for date in date_range
    )

    classification = pd.concat(classification_list)

    classification = pd.DataFrame(
        classification, columns=(gti.columns.to_list() + ghi.columns.to_list())
    )

    classification = classification.fillna("Indisponível")

    classification.to_parquet(
        PLANTS_PARAM[solar_plant]["datawarehouse"]["classification"]
    )
