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

from dateutil.parser import parse

from pygeoapi.process.base import BaseProcessor

from wis2box_api.wis2box.handle import DataHandler
from wis2box_api.wis2box.handle import handle_error


from wis2box_api.wis2box.env import WIS2BOX_DOCKER_API_URL

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wis2box-data_publish',
    'title': 'Process and publish input data',
    'description': 'Process input data attributes to publish data',  # noqa
    'keywords': [],
    'links': [],
    'jobControlOptions': ['async-execute'],
    'inputs': {
        'metadata_id': {
            'title': {'en': 'Metadata ID'},
            'description': {'en': 'Metadata ID to publish on'},
            'schema': {'type': 'string', 'default': None},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': []
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
        'data': {
            'title': 'raw data',
            'description': 'raw data to publish',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': [],
        },
        'datetime': {
            'title': 'datetime',
            'description': 'ISO 8601 datetime (UTC) corresponding to the data',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': []
        },
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
            'metadata_id': 'urn:md:wmo:example',
            'notify': True,
            'filename': 'SALT31_EYVI_201020.txt',
            'data': 'SALT31 EYVI 201020\nMETAR EYVI 201020Z 28008KT 230V310 CAVOK 18/06 Q1008 NOSIG=',
            'datetime': '2025-05-20T10:20:00Z',
            'geometry': {
                'type': 'Point',
                'coordinates': [24.9384, 60.1695]
            },
        }
    }
}


class DataPublishProcessor(BaseProcessor):

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition
        :returns: pygeoapi.process
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

        try:
            filename = data['filename']
            file_type = filename.split('.')[-1]
            # everything before the last dot
            file_id = filename.rsplit('.', 1)[0]
            geometry = data.get('geometry', None)
            # check if data['data'] is a string	or bytes
            the_data = None
            if isinstance(data['data'], str):
                # if the data is a string, convert it to bytes
                the_data = data['data'].encode('utf-8')
            elif isinstance(data['data'], bytes):
                the_data = data['data']
            else:
                raise Exception('data must be a string or bytes')
            
            
            output_item = {
                file_type : the_data,
                '_meta': {
                    'id': file_id,
                    'properties': {
                        'datetime': parse(data['datetime']),
                        'geometry': geometry
                    }
                },
                'errors': [],
                'warnings': []
            }
        except Exception as err:
            LOGGER.error(f'Failed to process data: {err}')
            return handle_error({err})

        LOGGER.info(f'Processing item: {output_item}')

        return data_handler.process_items([output_item])

    def __repr__(self):
        return '<submit> {}'.format(self.name)
