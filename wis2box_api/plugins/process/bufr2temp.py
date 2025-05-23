###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

import base64
import math
import logging
import requests

from pygeoapi.process.base import BaseProcessor
from bufr2geojson import transform as as_geojson
from wis2box_api.wis2box.env import STORAGE_PUBLIC_URL, STORAGE_SOURCE

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    "name": "bufr2temp",
    "id": "bufr2temp",
    "version": "0.1.0",
    "title": "Extract Temperature–Pressure Pairs",
    "description": (
        "Download or decode a BUFR file and extract "
        "(pressure, temperature, ln(pressure)) for T–lnP plotting"
    ),
    "keywords": ["bufr", "temperature", "pressure", "T-lnP"],
    "links": [],
    "jobControlOptions": ["async-execute", "sync-execute"],
    "inputs": {
        "data_url": {
            "title": "data_url",
            "description": "URL to the BUFR file",
            "schema": {"type": "string"},
            "minOccurs": 1,
            "maxOccurs": 1,
        },
        "data": {
            "title": "data",
            "description": "Base64-encoded BUFR content",
            "schema": {"type": "string"},
            "minOccurs": 0,
            "maxOccurs": 1,
        },
    },
    "outputs": {
        "items": {
            "title": "TemperaturePressureArray",
            "description": (
                "Array of {pressure, temperature, ln(pressure), "
                "phenomenonTime}"
            ),
            "schema": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "pressure": {"type": "number"},
                        "temperature": {"type": "number"},
                        "log_pressure": {"type": ["number", "null"]},
                        "phenomenonTime": {"type": "string"},
                    },
                },
            },
        },
        "error": {
            "title": {"en": "Error message"},
            "schema": {"type": "string"},
        },
    },
    "example": {
        "inputs": {
            "data_url": (
                "https://wis2box.kma.go.kr/data/2025-05-22/wis/"
                "urn:wmo:md:kr-kma:core.surface-based-observations.temp/"
                "WIGOS_0-20000-0-47169_20250522T231900.bufr4"
            )
        }
    },
}


class Bufr2TempProcessor(BaseProcessor):
    name = "bufr2temp"

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        LOGGER.debug("Executing bufr2temp process")

        # 1) load BUFR bytes
        try:
            if "data" in data:
                input_bytes = base64.b64decode(data["data"].encode("utf-8"))
            else:
                url = data["data_url"].replace(
                    STORAGE_PUBLIC_URL, f"{STORAGE_SOURCE}/wis2box-public"
                )
                LOGGER.debug(f"Downloading BUFR from: {url}")
                resp = requests.get(url)
                resp.raise_for_status()
                input_bytes = resp.content
        except Exception as e:
            LOGGER.error("Failed to load BUFR bytes: %s", e)
            return "application/json", {"items": [], "error": str(e)}

        # 2) BUFR → GeoJSON
        try:
            features = as_geojson(input_bytes)
        except Exception as e:
            LOGGER.error("Conversion to GeoJSON failed: %s", e)
            return "application/json", {"items": [], "error": str(e)}

        # 3) extract temperature/pressure records
        items = []
        for coll in features:
            for fid, item in coll.items():
                if fid != "geojson":
                    continue

                props = item["properties"]
                obs_prop = props.get("observedProperty", "")
                if not obs_prop.endswith("temperature"):
                    continue

                zc = (
                    props.get("parameter", {})
                         .get("additionalProperties", {})
                         .get("zCoordinate", {})
                )
                p = zc.get("value")
                t = props.get("result", {}).get("value")
                tm = props.get("phenomenonTime")

                if p is None or t is None:
                    continue

                try:
                    lp = math.log(p)
                except Exception:
                    lp = None

                items.append(
                    {
                        "pressure": p,
                        "temperature": t,
                        "log_pressure": lp,
                        "phenomenonTime": tm,
                    }
                )

        error = "" if items else "No temperature–pressure pairs extracted"
        return "application/json", {"items": items, "error": error}
