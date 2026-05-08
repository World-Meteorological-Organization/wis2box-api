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

import logging
import requests

from pygeoapi.process.base import BaseProcessor
from cap2geojson import transform as as_geojson

from wis2box_api.wis2box.env import STORAGE_PUBLIC_URL, STORAGE_SOURCE

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'cap2geojson',
    'title': 'Convert CAP XML to geoJSON',  # noqa
    'description': 'Download CAP XML from URL and convert file-content to geoJSON',  # noqa
    'keywords': [],
    'links': [],
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'inputs': {
        'data_url': {
            'title': 'data_url',
            'description': 'URL to the CAP XML file',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': [],
            'default': None
        },
        'data': {
            'title': 'data',
            'description': 'UTF-8 string of base64 encoded bytes to be converted to geoJSON',  # noqa
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
            'title': {'en': 'FeatureCollection'},
            'description': {
                'en': 'A GeoJSON FeatureCollection of the converted data'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'data_url': 'https://www.meteo.sc/api/cap/505e142a-3c8b-46de-808f-f117fc6f985f.xml', # noqa
        }
    }
}


class Cap2geojsonProcessor(BaseProcessor):

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition
        :returns: pygeoapi.process.synop-form.submit
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        LOGGER.debug('Execute process')

        items = []
        input_string = ''
        error = ''
        try:
            if 'data_url' in data:
                data_url = data['data_url']
                # replace the public URL with the internal minio URL
                LOGGER.debug('Replacing STORAGE_PUBLIC_URL with internal storage URL')  # noqa
                # replace the public URL with the internal storage URL
                data_url = data_url.replace(STORAGE_PUBLIC_URL, f'{STORAGE_SOURCE}/wis2box-public')  # noqa
                LOGGER.debug(f'Executing cap2geojson on: {data_url}')
                # read the data from the URL
                result = requests.get(data_url)
                # raise an exception if the status code is not 200
                result.raise_for_status()
                # get the string from the response
                input_string = result.text
            elif 'data' in data:
                input_string = data['data']
                LOGGER.debug(f'Executing cap2geojson on: {input_string}')
            else:
                raise ValueError('No data or data_url provided')
            LOGGER.debug('Generating GeoJSON features')
            items.append(as_geojson(input_string))
        except Exception as e:
            LOGGER.error(e)
            error = str(e)

        mimetype = 'application/json'
        outputs = {
            'items': items,
            'error': error
        }
        return mimetype, outputs
