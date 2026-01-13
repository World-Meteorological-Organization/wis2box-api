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
from synop2bufr import transform

from wis2box_api.wis2box.handle import DataHandler
from wis2box_api.wis2box.handle import handle_error

from wis2box_api.wis2box.station import Stations

from wis2box_api.wis2box.env import WIS2BOX_DOCKER_API_URL

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wis2box-synop2bufr',
    'title': 'Process and publish FM-12 SYNOP',
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
            'title': 'FM 12-SYNOP',
            'description': 'Input FM 12-SYNOP bulletin to convert to BUFR.',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': [],
        },
        'notify': {
            'title': 'Notify',
            'description': 'Enable WIS2 notifications',
            'schema': {'type': 'boolean'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'default': True
        },
        'year': {
            'title': 'Year',
            'description': 'Year (UTC) corresponding to FM 12-SYNOP bulletin',
            'schema': {'type': 'integer'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': []
        },
        'month': {
            'title': 'Month',
            'description': 'Month (UTC) corresponding to FM 12-SYNOP bulletin',
            'schema': {'type': 'integer'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': []
        }
    },
    'outputs': {
        'result': {
            'title': 'WIS2Publish result',
            'description': 'WIS2Publish result',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'channel': 'synop/test',
            'year': 2023,
            'month': 1,
            'notify': True,
            'data': 'AAXX 19064 64400 36/// /0000 10102 20072 30068 40182 53001 333 20056 91003 555 10302 91018=' # noqa
        }
    }
}


class SynopPublishProcessor(BaseProcessor):

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
            notify = data['notify']
            metadata_id = data.get('metadata_id', None)
            channel = data.get('channel', None)
            if metadata_id is None and notify:
                raise Exception('metadata_id must be provided if notify is True') # noqa
        except Exception as err:
            return handle_error({err})

        # get the channel from the metadata
        if channel is None:
            try:
                url = f'{WIS2BOX_DOCKER_API_URL}/collections/discovery-metadata/items?f=json' # noqa
                response = requests.get(url)
                if response.status_code == 200:
                    for item in response.json()['features']:
                        if metadata_id == item['properties']['identifier']:
                            channel = item['properties']['wmo:topicHierarchy']
                            break
            except Exception as err:
                return handle_error(f'Failed to load metadata: {err}')
        if channel is None:
            return handle_error(f'No metadata found for {metadata_id}')

        # initialize the DataHandler
        data_handler = DataHandler(channel,
                                   notify,
                                   metadata_id=metadata_id)
        # get the station metadata for the channel
        stations = Stations(channel=channel)
        # get the station metadata as a CSV string
        metadata = stations.get_csv_string(require_tsi=True)
        if metadata is None:
            return handle_error('No stations found')

        # Now call synop to BUFR
        try:
            fm12 = data['data']
            year = data['year']
            month = data['month']
            # run the transform
            bufr_generator = transform(data=fm12,
                                       metadata=metadata,
                                       year=year,
                                       month=month)
        except Exception as err:
            return handle_error(f'synop2bufr raised Exception: {err}') # noqa

        output_items = []
        try:
            for item in bufr_generator:
                LOGGER.debug(f'Processing item: {item}')
                warnings = []
                errors = []

                if 'result' in item['_meta']:
                    if 'errors' in item['_meta']['result']:
                        for error in item['_meta']['result']['errors']:
                            errors.append(error)
                    if 'warnings' in item['_meta']['result']:
                        for warning in item['_meta']['result']['warnings']:
                            warning.replace('station list file','station list') # noqa
                            warning.replace('not found in station file','not in station list; skipping') # noqa
                            warnings.append(warning)
                item['warnings'] = warnings
                item['errors'] = errors

                output_items.append(item)
        except Exception as err:
            # create a dummy item with error
            item = {
                'warnings': [],
                'errors': [f'Error in iterator: {err}']
            }
            output_items.append(item)

        LOGGER.debug(f'synop2bufr-transform returned {len(output_items)} items') # noqa

        return data_handler.process_items(output_items)

    def __repr__(self):
        return '<submit> {}'.format(self.name)
