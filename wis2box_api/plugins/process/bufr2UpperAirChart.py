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
import io
import logging
import tempfile

import numpy as np
import matplotlib.pyplot as plt

import requests

from pygeoapi.process.base import BaseProcessor

from eccodes import (
    codes_bufr_new_from_file,
    codes_get,
    codes_get_array,
    codes_release,
)

from wis2box_api.wis2box.env import STORAGE_PUBLIC_URL, STORAGE_SOURCE

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'bufr2UpperAirChart',
    'title': 'Convert BUFR to UpperAir Chart',  # noqa
    'description': 'Download bufr from URL and create Temperature Log-P chart',  # noqa
    'keywords': [],
    'links': [],
    'inputs': {
        'data_url': {
            'title': 'data_url',
            'description': 'URL to the BUFR file',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': [],
            'default': None
        },
        'data': {
            'title': 'data',
            'description': 'UTF-8 string of base64 encoded bytes containing upper-air observations in BUFR format',  # noqa
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': [],
            'default': None
        },
    },
    'outputs': {
        'path': {
            'title': {'en': 'Upper-Air Chart'},
            'description': {
                'en': 'JSON object with base64-encoded PNG image of the upper-air chart' # noqa
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'data_url': 'https://wis.nms.gov.bz/data/2025-09-08/wis/urn:wmo:md:bz-nms:belize-upper-air-messages/WIGOS_0-20000-0-78583_20250908T232600.bufr4',  # noqa
        }
    }
}


class Bufr2UpperAirChartProcessor(BaseProcessor):
    """BUFR to Upper-Air Sounding Chart Processor"""

    def __init__(self, processor_def):
        """
        Initialize object
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def bufr_to_table(self, bufr_data):
        """
        Parse BUFR sounding/profile into a table (list of dicts).
        Each row = one level.

        :param bufr_data: bytes of BUFR data
        """

        variables = [
            "pressure",
            "airTemperature",
            "dewpointTemperature",
            "windSpeed",
            "windDirection"
        ]
        table = []
        datetime_str = ''

        # Write BUFR to a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(bufr_data)
            tmp_path = tmp.name

        # Open with eccodes
        with open(tmp_path, "rb") as f:
            bufr = codes_bufr_new_from_file(f)
            if bufr is None:
                raise Exception("Could not read BUFR file")
            else:
                codes_get(bufr, "unpack")
                # fetch arrays for each variable
                arrays = {}
                maxlen = 0
                for var in variables:
                    try:
                        vals = codes_get_array(bufr, var)
                        arrays[var] = vals
                        if len(vals) > maxlen:
                            maxlen = len(vals)
                    except Exception:
                        LOGGER.info(f"Variable {var} not found in BUFR")
                        arrays[var] = []
                # align arrays by index
                for i in range(maxlen):
                    row = {}
                    for var in variables:
                        if i < len(arrays[var]):
                            row[var] = arrays[var][i]
                        else:
                            row[var] = None  # missing if shorter
                    table.append(row)
                # get date/time
                year = codes_get(bufr, "year")
                month = codes_get(bufr, "month")
                day = codes_get(bufr, "day")
                hour = codes_get(bufr, "hour")
                minute = codes_get(bufr, "minute")
                # create datetime string
                datetime_str = f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d} UTC" # noqa
                codes_release(bufr)
        return table, datetime_str

    def plot_sounding(self, rows, datetime_str):
        """
        Plot upper-air sounding (Temperature and Dewpoint vs Pressure)
        with wind barbs.
        :param rows: list of dicts with sounding data
        :param datetime_str: string with date/time for title
        :returns: matplotlib Figure object
        """

        p = []
        T = []
        Td = []
        wspd = []
        wdir = []

        for r in rows:
            if (r.get("pressure") is None or r.get("airTemperature") is None or r.get("dewpointTemperature") is None): # noqa
                continue
            p.append(r["pressure"] / 100.0)  # Pa → hPa
            T.append(r["airTemperature"] - 273.15)   # K → °C
            Td.append(r["dewpointTemperature"] - 273.15)
            wspd.append(r.get("windSpeed", np.nan))
            wdir.append(r.get("windDirection", np.nan))

        p = np.array(p)
        T = np.array(T)
        Td = np.array(Td)
        wspd = np.array(wspd)
        wdir = np.array(wdir)

        # Compute wind components for barbs
        u = -wspd * np.sin(np.deg2rad(wdir))
        v = -wspd * np.cos(np.deg2rad(wdir))

        # Create figure
        fig, ax = plt.subplots(figsize=(6, 9))

        # Plot Temperature and Dewpoint vs Pressure
        ax.plot(T, p, 'r', label="Temperature")
        ax.plot(Td, p, 'g', label="Dewpoint")

        # Add wind barbs every Nth level
        skip = max(1, len(p)//30)  # thin out if too many levels
        x_barb = np.full_like(p[::skip], fill_value=35.0)  # fixed x position for barbs # noqa
        ax.barbs(x_barb, p[::skip], u[::skip], v[::skip], length=6, pivot='middle') # noqa

        # Pressure axis (log scale, inverted)
        ax.set_yscale("log")
        ax.set_ylim(1050, 100)
        ax.set_yticks([1000, 850, 700, 500, 300, 200, 100])
        ax.get_yaxis().set_major_formatter(plt.ScalarFormatter())

        ax.set_xlabel("Temperature (°C)")
        ax.set_ylabel("Pressure (hPa)")
        ax.set_title(f"{datetime_str}")
        ax.legend(loc="upper center")

        return fig

    def handle_error(self, err):
        """
        Handle error

        :param err: error message

        :returns: json error response
        """

        mimetype = 'application/json'
        outputs = {
            'error': err
        }
        return mimetype, outputs

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        LOGGER.info('Executing process {}'.format(self.name))

        bufr_data = None

        url = data.get('data_url')
        if url:
            # Convert public URL to internal storage path
            if url.startswith(STORAGE_PUBLIC_URL):
                url= url.replace(STORAGE_PUBLIC_URL, f'{STORAGE_SOURCE}/wis2box-public') # noqa
            # get the BUFR data from the URL
            response = requests.get(url)
            response.raise_for_status()
            bufr_data = response.content
        else:
            return self.handle_error('data_url must be provided')

        # parse BUFR to table
        rows, datetime_str = self.bufr_to_table(bufr_data)
        if not rows:
            return self.handle_error('No valid sounding data found in BUFR')
        # plot the sounding
        fig = self.plot_sounding(rows, datetime_str)

        # save figure to bytes buffer
        buf = io.BytesIO()
        plt.savefig(
            buf,
            format="png",
            bbox_inches="tight",
            dpi=90,
            transparent=False
        )
        plt.close(fig)
        buf.seek(0)

        # Encode as base64 string
        b64_png = base64.b64encode(buf.read()).decode("utf-8")

        mimetype = 'application/json'
        outputs = {
            'base64_png': b64_png
        }

        # Return binary PNG with correct content type
        return mimetype, outputs

    def __repr__(self):
        return f"<Bufr2UpperAirChartProcessor> {self.name}"
