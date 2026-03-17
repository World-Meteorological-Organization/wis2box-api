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

from pyoscar import OSCARClient

from pygeoapi.process.base import BaseProcessor

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'oscar2feature',
    'title': 'Query OSCAR and return station metadata for wis2box',
    'description': 'Query OSCAR and return station metadata for wis2box',
    'keywords': [],
    'links': [],
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'inputs': {
        'wigos_station_identifier': {
            'title': {'en': 'WIGOS Station Identifier'},
            'description': {'en': 'WIGOS Station Identifier'},
            'schema': {'type': 'string', 'default': None},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': []
        }
    },
    'outputs': {
        'path': {
            'title': {'en': 'StationFeature'},
            'description': {
                'en': 'StationFeature for wis2box'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'wigos_station_identifier': '0-20000-0-10393',	# noqa
        },
    },
}

OSCAR_STATION_URL = 'https://oscar.wmo.int/surface/#/search/station/stationReportDetails/' # noqa


class Oscar2FeatureProcessor(BaseProcessor):

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition
        :returns: pygeoapi.process.synop-form.submit
        """

        super().__init__(processor_def, PROCESS_METADATA)

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

        LOGGER.debug('Execute process')

        wsi = data.get('wigos_station_identifier')
        client = OSCARClient(env='prod')

        try:
            station = client.get_station_report(wsi, format_='XML', summary=True) # noqa
        except Exception as err:
            return self.handle_error(f'{err}') # noqa

        if 'wigos_station_identifier' not in station:
            return self.handle_error(f'No station found in OSCAR/Surface for {wsi}') # noqa

        # take the first wigos_station_identifier if there are more than one
        if ',' in station['wigos_station_identifier']:
            station['wigos_station_identifier'] = station['wigos_station_identifier'].split(',')[0] # noqa

        tsi = ''
        if station['wigos_station_identifier'].startswith('0-20000'):
            tsi = station['wigos_station_identifier'].split('-')[-1]  # noqa

        wis2box_station = {
            'id': station['wigos_station_identifier'],
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [
                    station.get('longitude', ''),
                    station.get('latitude', ''),
                    station.get('elevation', '')
                ]
            },
            'properties': {
                'name': station.get('station_name', ''),
                'wigos_station_identifier': wsi,
                'traditional_station_identifier': tsi,
                'facility_type': station.get('facility_type', ''),
                'territory_name': station.get('territory_name', ''),
                'barometer_height': station.get('barometer_height', ''),
                'wmo_region': station.get('wmo_region', ''),
                'url': OSCAR_STATION_URL+station.get('wigos_station_identifier', ''), # noqa
                'topics': [],
                'status': 'operational',
                'id': wsi
            }
        }

        mimetype = 'application/json'
        outputs = {
            'feature': wis2box_station
        }
        return mimetype, outputs
