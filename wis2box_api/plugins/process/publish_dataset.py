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
import time

import paho.mqtt.publish as publish

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from wis2box_api.wis2box.env import BROKER_HOST
from wis2box_api.wis2box.env import BROKER_PORT
from wis2box_api.wis2box.env import BROKER_USERNAME
from wis2box_api.wis2box.env import BROKER_PASSWORD


LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wis2box-publish_dataset',
    'title': 'Publish dataset',
    'description': 'Update metadata and data-mappings in backend and publish metadata-notification', # noqa
    'keywords': [],
    'links': [],
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'inputs': {
        'metadata': {
            'title': {'en': 'metadata'},
            'description': {'en': 'discovery metadata record'},
            'schema': {'type': 'json', 'default': None},
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,
            'keywords': []
        }
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
    },
    'example': {
        'inputs': {
            'metadata': {
                "id": "urn:wmo:md:test-wis-node2:surface-based-observations.synop", # noqa
                "conformsTo": [
                    "http://wis.wmo.int/spec/wcmp/2/conf/core"
                ],
                "type": "Feature",
                "wis2box": {
                    "topic_hierarchy": "test-wis-node2.data.core.weather.surface-based-observations.synop", # noqa
                    "country": "FRA",
                    "centre_id": "test-wis-node2",
                    "data_mappings": {"plugins": {
                        "bin": [{
                            "plugin": "wis2box.data.bufr4.ObservationDataBUFR",
                            "notify": True,
                            "buckets": ["wis2box-incoming"],
                            "file-pattern": "^.*\\.bin$"
                        }],
                        "txt": [{
                            "plugin": "wis2box.data.synop2bufr.ObservationDataSYNOP2BUFR", # noqa
                            "notify": True,
                            "file-pattern": "^A_SMR.*EDZW_(\\d{4})(\\d{2}).*.txt$" # noqa
                        }],
                        "csv": [{
                            "plugin": "wis2box.data.csv2bufr.ObservationDataCSV2BUFR", # noqa
                            "template": "aws-template",
                            "notify": True,
                            "buckets": ["wis2box-incoming"],
                            "file-pattern": "^.*\\.csv$"
                        }],
                        "bufr4": [{
                            "plugin": "wis2box.data.bufr2geojson.ObservationDataBUFR2GeoJSON", # noqa
                            "buckets": ["wis2box-public"],
                            "file-pattern": "^WIGOS_(\\d-\\d+-\\d+-\\w+)_.*\\.bufr4$" # noqa
                        }]
                    }}
                },
                "time": {
                    "interval": ["2024-03-14", ".."],
                    "resolution": "P1H"
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [-180, 90],
                        [180, 90],
                        [180, -90],
                        [-180, -90],
                        [-180, 90]
                    ]]
                },
                "properties": {
                    "type": "dataset",
                    "identifier": "urn:wmo:md:test-wis-node2:surface-based-observations.synop", # noqa
                    "title": "Hourly synoptic observations from fixed-land stations (SYNOP) (test-wis-node2)", # noqa
                    "description": "this is a test dataset",
                    "language": {
                        "code": None
                    },
                    "keywords": ["surface", "land", "observations"],
                    "themes": [{
                        "concepts": [{"id": "weather", "title": "Weather"}],
                        "scheme": [ "https://codes.wmo.int/wis/topic-hierarchy/earth-system-discipline"] # noqa
                    }],
                    "contacts": [{
                        "organization": "Maaike Limper",
                        "phones": [{"value": "+33"}],
                        "emails": [{"value": "me@gmail.com"}],
                        "addresses": [{"country": "FRA"}],
                        "links": [{"rel": "about", "href": "https://me.net", "type": "text/html"}], # noqa
                        "hoursOfService": "Hours: Mo-Fr 9am-5pm Sa 10am-5pm Su 10am-4pm", # noqa
                        "contactInstructions": "Email",
                        "roles": ["host"]
                    }],
                    "created": "2024-03-14T12:37:33Z",
                    "updated": "2024-03-15T09:43:34Z",
                    "wmo:dataPolicy": "core",
                    "wmo:topicHierarchy": "origin/a/wis2/test-wis-node2/data/core/weather/surface-based-observations/synop", # noqa
                    "id": "urn:wmo:md:test-wis-node2:surface-based-observations.synop" # noqa
                }
            }
        }
    }
}


class PublishDatasetProcessor(BaseProcessor):

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition
        :returns: wis2box_api.plugins.process.publish_dataset
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        LOGGER.debug('Execute process')

        status = 'success'

        try:
            metadata = data['metadata']
        except KeyError:
            msg = 'Missing required parameter: metadata'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        # check that metadata is a dict
        if not isinstance(metadata, dict):
            msg = 'metadata must be a json object'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        # check that metadata has an id
        if 'id' not in metadata:
            msg = 'metadata must have an id'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        try:
            # create the message out of the metadata
            msg = metadata
            # dump the message to a string and sanitize html
            msg = json.dumps(msg).replace('<', '&lt;').replace('>', '&gt;')
            # publish notification on internal broker
            private_auth = {
                'username': BROKER_USERNAME,
                'password': BROKER_PASSWORD
            }
            topic = 'wis2box/dataset/publication'
            publish.single(topic=topic,
                           payload=msg,
                           qos=1,
                           retain=False,
                           hostname=BROKER_HOST,
                           port=int(BROKER_PORT),
                           auth=private_auth)
            LOGGER.debug('dataset publish message sent')
        except Exception as e:
            status = f'Error publishing on topic={topic}, error={e}'
        # sleep for a 1 second to allow the backend to process the message
        time.sleep(1)

        try:
            # send a message to refresh the data mappings
            private_auth = {
                'username': BROKER_USERNAME,
                'password': BROKER_PASSWORD
            }
            msg = {}
            topic = 'wis2box/data_mappings/refresh'
            publish.single(topic=topic,
                           payload=json.dumps(msg),
                           qos=1,
                           retain=False,
                           hostname=BROKER_HOST,
                           port=int(BROKER_PORT),
                           auth=private_auth)
            LOGGER.debug('refresh data mappings message sent')
        except Exception as e:
            msg = f'Error publishing on topic={topic}, error={e}' # noqa
            LOGGER.error(msg)
            status = msg

        mimetype = 'application/json'
        outputs = {
            'status': status
        }
        return mimetype, outputs
