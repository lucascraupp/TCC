import os

from peewee import *

# Conexão com o banco de dados MySQL já existente
db = MySQLDatabase(
    os.environ["DB_DATABASE"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    host=os.environ["DB_HOST"],
    port=3306,
)


class BaseModel(Model):
    class Meta:
        database = db


class SolarPlant(BaseModel):
    id_solar_plant = AutoField(primary_key=True)
    name = CharField(unique=True, max_length=45)

    class Meta:
        table_name = "solar_plant"


class Sensor(BaseModel):
    id_sensor = AutoField(primary_key=True)
    name = CharField(max_length=45)
    type = CharField(max_length=45)

    class Meta:
        table_name = "sensor"


class Status(BaseModel):
    id_status = AutoField(primary_key=True)
    name = CharField(unique=True, max_length=11)

    class Meta:
        table_name = "status"


class SolarPlantHasSensor(BaseModel):
    id_solar_plant = ForeignKeyField(
        SolarPlant,
        backref="solar_plant",
        on_delete="RESTRICT",
        on_update="RESTRICT",
        column_name="id_solar_plant",
    )
    id_sensor = ForeignKeyField(
        Sensor,
        backref="sensor",
        on_delete="RESTRICT",
        on_update="RESTRICT",
        column_name="id_sensor",
    )

    class Meta:
        primary_key = CompositeKey("id_solar_plant", "id_sensor")
        table_name = "solar_plant_has_sensor"


class SolarPlantData(BaseModel):
    id_solar_plant = ForeignKeyField(
        SolarPlantHasSensor,
        backref="solar_plant_has_sensor",
        on_delete="RESTRICT",
        on_update="RESTRICT",
        column_name="id_solar_plant",
    )
    id_sensor = ForeignKeyField(
        SolarPlantHasSensor,
        backref="solar_plant_has_sensor",
        on_delete="RESTRICT",
        on_update="RESTRICT",
        column_name="id_sensor",
    )
    id_status = ForeignKeyField(
        Status,
        backref="status",
        on_delete="RESTRICT",
        on_update="RESTRICT",
        column_name="id_status",
    )
    timestamp = DateTimeField()
    value = FloatField()

    class Meta:
        primary_key = CompositeKey(
            "id_solar_plant", "id_sensor", "id_status", "timestamp"
        )
        table_name = "solar_plant_data"


class ExternalData(BaseModel):
    id_solar_plant = ForeignKeyField(
        SolarPlantHasSensor,
        backref="external_data",
        on_delete="RESTRICT",
        on_update="RESTRICT",
        column_name="id_solar_plant",
    )
    id_sensor = ForeignKeyField(
        SolarPlantHasSensor,
        backref="external_data",
        on_delete="RESTRICT",
        on_update="RESTRICT",
        column_name="id_sensor",
    )
    timestamp = TimestampField()
    value = FloatField()

    class Meta:
        primary_key = CompositeKey("id_solar_plant", "id_sensor", "timestamp")
        table_name = "external_data"


class Classification(BaseModel):
    id_solar_plant = ForeignKeyField(
        SolarPlantHasSensor,
        backref="classifications",
        on_delete="RESTRICT",
        on_update="RESTRICT",
        column_name="id_solar_plant",
    )
    id_sensor = ForeignKeyField(
        SolarPlantHasSensor,
        backref="classifications",
        on_delete="RESTRICT",
        on_update="RESTRICT",
        column_name="id_sensor",
    )
    timestamp = TimestampField()
    status = CharField(max_length=45)

    class Meta:
        primary_key = CompositeKey("id_solar_plant", "id_sensor", "timestamp")
        table_name = "classification"


# Conectando ao banco de dados
db.connect()

if __name__ == "__main__":
    for data in SolarPlantData.select():
        print(data)
