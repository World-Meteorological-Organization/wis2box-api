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

import paho.mqtt.publish as publish

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from wis2box_api.wis2box.env import BROKER_HOST
from wis2box_api.wis2box.env import BROKER_PORT
from wis2box_api.wis2box.env import BROKER_USERNAME
from wis2box_api.wis2box.env import BROKER_PASSWORD

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wis2box-storage-event',
    'title': 'POST storage-event',
    'description': 'POST a storage-event', # noqa
    'keywords': [],
    'links': [],
    'inputs': {
        'storage_event': {
            'title': {'en': 'Storage Event'},
            'description': {'en': 'Storage Event to process'},
            'schema': {'type': 'object', 'default': None},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': []
        },
    },
    'outputs': {
        'path': {
            'title': {'en': 'status'},
            'description': {
                'en': 'status of update'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    }
}


class StorageEventProcessor(BaseProcessor):

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition
        :returns: this object
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        LOGGER.debug('Execute process')

        status = 'unknown'

        try:
            storage_event = data['storage_event']
        except KeyError:
            msg = 'Missing required parameter: storage_event'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        try:
            # publish notification on internal broker
            private_auth = {
                'username': BROKER_USERNAME,
                'password': BROKER_PASSWORD
            }
            msg = storage_event
            topic = 'wis2box/storage'
            publish.single(topic=topic, # noqa
                           payload=json.dumps(msg),
                           qos=1,
                           retain=False,
                           hostname=BROKER_HOST,
                           port=int(BROKER_PORT),
                           auth=private_auth)
            LOGGER.debug(f'Published storage event on topic={topic}')
        except Exception as e:
            status = f'Error publishing on topic={topic}, error={e}'

        mimetype = 'application/json'
        outputs = {
            'status': status
        }
        return mimetype, outputs
