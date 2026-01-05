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
import logging

from pygeoapi.process.base import BaseProcessor

from wis2box_api.wis2box.handle import handle_error
from wis2box_api.wis2box.handle import DataHandler
from wis2box_api.wis2box.bufr4 import ObservationDataBUFR
from wis2box_api.wis2box.bufr4 import get_bufr_overrides

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wis2box-bufr2bufr',
    'title': 'Process and publish bufr data',
    'description': 'Converts the posted data to BUFR and publishes to specified topic',  # noqa
    'keywords': [],
    'links': [],
    'jobControlOptions': ['async-execute'],
    'inputs': {
        'data': {
            'title': 'data',
            'description': 'UTF-8 string of base64 encoded bytes',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': [],
        },
        'channel': {
            'title': {'en': 'Channel'},
            'description': {'en': 'Channel / topic to publish on'},
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
        }
    },
    'outputs': {
        'path': {
            'title': {'en': 'ConverPublishResult'},
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
            'data': 'SVNNRDAyIExJSUIgMjEwMDAwIFJSQQ0NCkJVRlIAAOwEAAAWAABQAAAAAAACABAAB+YDFQAAAAAACQAAAYDHVgAAwQAgrCanpyoiqaGqqSeQEBAQEBAQEBAQL8xqgAYqvgJXWq5Q0iiRQXP/+98PuhNAUBAGQ0X7QO2ADIH0AGQAA//+mHMFz6hQCCZALgH9BxQD////////////////////////////////////8OP9HI/+AB+gAABkP9AAP///+AZD9EADVev0QANFqB9GCf2JoGf39v//+6YCATv//////3/////////4AAAAf//////7/6P////8P/wCye///8A3Nzc3DQ0K', # noqa
            'channel': 'bufr/test',
            'notify': False
        },
    },
}


class BufrPublishProcessor(BaseProcessor):

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
            if metadata_id is None and notify:
                raise Exception('metadata_id must be provided if notify is True') # noqa
            # initialize the DataHandler
            data_handler = DataHandler(channel,
                                       notify,
                                       metadata_id=metadata_id)
        except Exception as err:
            return handle_error({err})

        # Now call bufr to BUFR
        try:
            base64_encoded_data = data['data']
            LOGGER.debug(f'Executing bufr2bufr on: {base64_encoded_data}')  # noqa
            # Convert the encoded data string to bytes
            encoded_data_bytes = base64_encoded_data.encode('utf-8')
            # Decode base64 encoded data
            input_bytes = base64.b64decode(encoded_data_bytes)
            overrides = get_bufr_overrides(metadata_id) if metadata_id else None # noqa
            obs_bufr = ObservationDataBUFR(input_bytes, channel, overrides)
            LOGGER.info(f'Size of input_bytes: {len(input_bytes)}')
        except Exception as err:
            return handle_error(f'bufr2bufr raised Exception: {err}') # noqa

        try:
            output_items = obs_bufr.process_data()
        except Exception as err:
            msg = f'ObservationDataBUFR.process_data raised Exception: {err}'
            LOGGER.error(msg)
            return handle_error(msg)

        return data_handler.process_items(output_items)
