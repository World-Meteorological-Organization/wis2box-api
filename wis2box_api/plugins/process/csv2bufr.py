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

import json
import logging
import os

from pygeoapi.process.base import BaseProcessor

from wis2box_api.wis2box.handle import handle_error
from wis2box_api.wis2box.handle import DataHandler
from wis2box_api.wis2box.station import Stations

import csv2bufr.templates as c2bt

from csv2bufr import transform as transform_csv

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wis2box-csv2bufr',
    'title': 'Process and publish CSV from Automatic Weather Stations',
    'description': 'Converts the posted data to BUFR and publishes to specified topic',  # noqa
    'keywords': [],
    'links': [],
    'jobControlOptions': ['async-execute'],
    'inputs': {
        'channel': {
            'title': {'en': 'Channel'},
            'description': {'en': 'Channel / topic to publish on'},
            'schema': {'type': 'string', 'default': None},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': []
        },
        'data': {
            'title': 'CSV Data',
            'description': 'Input CSV data',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': [],
        },
        'template': {
            'title': 'Mapping',
            'description': 'Mapping-template for CSV to BUFR conversion',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': [],
            'default': 'aws-template'
        },
        'notify': {
            'title': 'Notify',
            'description': 'Enable WIS2 notifications',
            'schema': {'type': 'boolean'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'default': True
        }
    },
    'outputs': {
        'path': {
            'title': {'en': 'ConvertPublishResult'},
            'description': {
                'en': 'Conversion and publish result in JSON format'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'data': 'wsi_series,wsi_issuer,wsi_issue_number,wsi_local,wmo_block_number,wmo_station_number,station_type,year,month,day,hour,minute,latitude,longitude,station_height_above_msl,barometer_height_above_msl,station_pressure,msl_pressure,geopotential_height,thermometer_height,air_temperature,dewpoint_temperature,relative_humidity,method_of_ground_state_measurement,ground_state,method_of_snow_depth_measurement,snow_depth,precipitation_intensity,anemometer_height,time_period_of_wind,wind_direction,wind_speed,maximum_wind_gust_direction_10_minutes,maximum_wind_gust_speed_10_minutes,maximum_wind_gust_direction_1_hour,maximum_wind_gust_speed_1_hour,maximum_wind_gust_direction_3_hours,maximum_wind_gust_speed_3_hours,rain_sensor_height,total_precipitation_1_hour,total_precipitation_3_hours,total_precipitation_6_hours,total_precipitation_12_hours,total_precipitation_24_hours\n0,20000,0,15015,15,15,1,2022,3,31,0,0,47.77706163,23.94046026,503,504.43,100940,10104,1448,5,298.15,294.55,80.4,3,1,1,0,0.004,10,-10,30,3,30,5,40,9,20,11,2,4.7,5.3,7.9,9.5,11.4', # noqa
            'channel': 'csv/test',
            'notify': False,
            'template': 'aws-template'
        },
    },
}


class CSVPublishProcessor(BaseProcessor):

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

        LOGGER.info('Executing process {}'.format(self.name))

        try:
            channel = data['channel']
            notify = data['notify']
            metadata_id = data.get('metadata_id', None)
            # initialize the DataHandler
            data_handler = DataHandler(channel, notify, metadata_id=metadata_id)
            # get stations
            stations = Stations()
        except Exception as err:
            return handle_error({err})

        # Now call csv to BUFR
        try:
            csv_data = data['data']
            template = data['template']

            if not os.path.isfile(template):
                lt = c2bt.list_templates()
                tn = [x['name'] for x in lt.values()]
                if template not in tn:
                    raise Exception(f"Unknown template: {template}, options are: {', '.join(tn)}") # noqa
                mappings = c2bt.load_template(template)
            else:
                with open(template) as fh:
                    mappings = json.load(fh)
            LOGGER.debug(f'Using mappings: {mappings}')
            # run the transform
            bufr_generator = transform_csv(data=csv_data,
                                           mappings=mappings)
        except Exception as err:
            return handle_error(f'csv2bufr raised Exception: {err}') # noqa

        output_items = []
        try:
            for item in bufr_generator:
                LOGGER.debug(f'Processing item: {item}')
                warnings = []
                errors = []

                wsi = item['_meta']['properties']['wigos_station_identifier']

                if 'result' in item['_meta']:
                    if 'errors' in item['_meta']['result']:
                        for error in item['_meta']['result']['errors']:
                            errors.append(error)
                    if 'warnings' in item['_meta']['result']:
                        for warning in item['_meta']['result']['warnings']:
                            warnings.append(warning)

                if stations.check_valid_wsi(wsi) is False:
                    warning = f'Station {wsi} not in station list; skipping'
                    warnings.append(warning)
                    # remove bufr4 from item
                    if 'bufr4' in item:
                        del item['bufr4']

                item['warnings'] = warnings
                item['errors'] = errors

                output_items.append(item)
        except Exception as err:
            # create a dummy item with error
            item = {
                'warnings': [],
                'errors': [f'Error processing item: {err}']
            }
            output_items.append(item)

        return data_handler.process_items(output_items)
