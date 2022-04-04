#!/usr/bin/env python3

import os
import json
import argparse
import requests
import logging

from datetime import datetime
from pytz import timezone
from dotenv import load_dotenv
from mysql import connector as DB
from typing import Optional
from pydantic import BaseModel, Field

logging.basicConfig(level=os.environ.get("LINKA_PROXY_LOG_LEVEL", "INFO").upper())

LAST_PATH = os.environ.get("LINKA_PROXY_FIUNA_LAST_PATH", ".last")

SENSORS = {
    "Estacion1": {
        "source": "fiuna-01",
        "sensor": "OPC_N2",
        "description": "FIUNA 01, Campus",
        "ignore": True,
    },
    "Estacion2": {
        "source": "fiuna-02",
        "sensor": "OPC_N2",
        "description": "FIUNA 02, Fernando",
    },
    "Estacion3": {
        "source": "fiuna-03",
        "sensor": "OPC_N2",
        "description": "FIUNA 03, Acceso Sur",
        "ignore": True,
    },
    "Estacion4": {
        "source": "fiuna-04",
        "sensor": "OPC_N2",
        "description": "FIUNA 04, San Vicente",
    },
    "Estacion5": {
        "source": "fiuna-05",
        "sensor": "OPC_N2",
        "description": "FIUNA 05, Villa Morra",
    },
    "Estacion6": {
        "source": "fiuna-06",
        "sensor": "OPC_N2",
        "description": "FIUNA 06, Mariscal López",
        "ignore": True,
    },
    "Estacion7": {
        "source": "fiuna-07",
        "sensor": "OPC_N2",
        "description": "FIUNA 07, San Roque",
    },
    "Estacion8": {
        "source": "fiuna-08",
        "sensor": "OPC_N2",
        "description": "FIUNA 08, Centro",
    },
    "Estacion9": {
        "source": "fiuna-09",
        "sensor": "OPC_N2",
        "description": "FIUNA 08, Mbocayaty",
    },
    "Estacion10": {
        "source": "fiuna-10",
        "sensor": "OPC_N2",
        "description": "FIUNA 08, Residenta",
    },
    "Estacion11": {
        "source": "fiuna-11",
        "sensor": "OPC_N2",
        "description": "FIUNA 11, Cerrito",
        "ignore": True,
    },
}

ALLOWED_FIELDS = [
    "pm10",
    "pm1dot0",
    "pm2dot5",
    "humidity",
    "temperature",
    "pressure",
    "longitude",
    "latitude",
    "recorded",
    "source",
    "sensor",
    "description",
]


class Measurement(BaseModel):

    sensor: str = Field(
        ...,
        title="Sensor",
        description="Model of the device",
    )
    source: str = Field(
        ...,
        title="Source",
        description="Name used to identify the device",
    )
    description: Optional[str] = Field(
        None,
        title="Description",
        description="User friendly name to identify the device",
    )
    version: Optional[str] = Field(
        None,
        title="Version",
        description="Firmware version of the device",
    )
    pm1dot0: Optional[float] = Field(
        None,
        title="PM1.0",
        description="Concentration of PM1.0 inhalable particles per ug/m3",
        ge=0,
        le=500,
    )
    pm2dot5: Optional[float] = Field(
        None,
        title="PM2.5",
        description="Concentration of PM2.5 inhalable particles per ug/m3",
        ge=0,
        le=500,
    )
    pm10: Optional[float] = Field(
        None,
        title="PM10",
        description="Concentration of PM10 inhalable particles per ug/m3",
        ge=0,
        le=500,
    )
    humidity: Optional[float] = Field(
        None,
        title="Humidity",
        description="Concentration of water vapor present in the air",
        ge=1.0,  # Coober Pedy, South Australia
        le=100.0,  # Sea ?
    )
    temperature: Optional[float] = Field(
        None,
        title="Temperature",
        description="Temperature in celsius degrees",
        ge=-89.2,  # Vostok, Antarctica
        le=134.0,  # Death Valley, California
    )
    pressure: Optional[float] = Field(
        None,
        title="Pressure",
        description="Pressure within the atmosphere of Earth in hPa",
        ge=870.0,  # Typhoon Tip, Pacific Ocean
        le=1084.0,  # Agata, Siberia
    )
    co2: Optional[float] = Field(
        None,
        title="CO2",
        description="Carbon dioxide concentration in ppm",
        ge=50.0,  # Closed system with plants
        le=80000.0,  # Twice the IDLH
    )
    longitude: float = Field(
        ...,
        title="Longitud",
        description="Physical longitude coordinate of the device",
        ge=-180,
        le=180,
    )
    latitude: float = Field(
        ...,
        title="Latitude",
        description="Physical latitude coordinate of the device",
        ge=-90,
        le=90,
    )
    recorded: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        title="Recorded",
        description="Date and time for when these values were measured",
    )


def cleanup(measurement):
    # remove unsupported fields
    fields = list(measurement.keys())
    for field in fields:
        if field not in ALLOWED_FIELDS:
            del measurement[field]

    # remove supported fields
    for field in ALLOWED_FIELDS:
        if not measurement[field]:
            del measurement[field]

    # standarize fields
    source = measurement.get("source")
    overrides = SENSORS.get(source)

    if not overrides:
        raise ValueError(f"Can't find {source}")
    if overrides.get("ignore"):
        raise ValueError(f"Ignore {source}")

    measurement.update(overrides)

    recorded = datetime.fromtimestamp(measurement["recorded"])
    recorded = recorded.astimezone(timezone("UTC"))
    measurement["recorded"] = recorded.isoformat()

    # validate all fields
    Measurement(**measurement)

    return measurement


def pull(host, user, password, database, table, last):
    logging.info("pulling...")

    measurements = []

    db = DB.connect(
        host=host,
        user=user,
        password=password,
        database=database,
    )

    query = f"SELECT * FROM {table} WHERE id>{last} ORDER BY id ASC"
    logging.info(query)

    cursor = db.cursor(dictionary=True)
    cursor.execute(query)
    results = cursor.fetchall()

    for result in results:
        last = result["id"]

        try:
            measurement = cleanup(result)
        except ValueError as e:
            logging.debug(e)
            continue
        else:
            measurements.append(measurement)

    db.close()

    return last, measurements


def restore_last():
    logging.info("restoring...")

    if not os.path.exists(LAST_PATH):
        return 0
    with open(LAST_PATH) as file:
        return file.read()


def save_last(last):
    logging.info(f"saving to {last}...")

    with open(LAST_PATH, "w") as file:
        file.write(f"{last}")


def push(measurements, endpoint, api_key):
    logging.info(f"pushing {len(measurements)} measurements...")

    if not measurements:
        return

    headers = {"X-API-Key": api_key}
    data = json.dumps(measurements)
    request = requests.post(endpoint, data=data, headers=headers)
    request.raise_for_status()


def run(host, user, password, database, table, endpoint, api_key):
    logging.info("running...")

    last, measurements = pull(host, user, password, database, table, restore_last())
    push(measurements, endpoint, api_key)
    save_last(last)


def main():
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        default=os.environ.get("LINKA_PROXY_FIUNA_HOST"),
    )
    parser.add_argument(
        "--user",
        default=os.environ.get("LINKA_PROXY_FIUNA_USER"),
    )
    parser.add_argument(
        "--password",
        default=os.environ.get("LINKA_PROXY_FIUNA_PASSWORD"),
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("LINKA_PROXY_FIUNA_DATABASE"),
    )
    parser.add_argument(
        "--table",
        default=os.environ.get("LINKA_PROXY_FIUNA_TABLE"),
    )
    parser.add_argument(
        "--endpoint",
        default=os.environ.get("LINKA_PROXY_SERVER_ENDPOINT"),
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("LINKA_PROXY_SERVER_API_kEY"),
    )
    args = parser.parse_args()

    run(
        args.host,
        args.user,
        args.password,
        args.database,
        args.table,
        args.endpoint,
        args.api_key,
    )


if __name__ == "__main__":
    main()
