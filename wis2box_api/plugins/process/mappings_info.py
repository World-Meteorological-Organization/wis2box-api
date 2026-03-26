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

import csv2bufr.templates as c2bt

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

LOGGER = logging.getLogger(__name__)

PROCESS_DEF = {
    'version': '0.1.0',
    'id': 'wmo-get-ra',
    'title': {
        'en': 'Get available mapping templates in the system'
    },
    'description': {
        'en': 'Get available mapping templates in the system'
    },
    'keywords': [],
    'links': [],
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'inputs': {
        'plugin': {
            'title': 'Plugin',
            'description': 'Plugin ID',
            'schema': {
                'type': 'string'
            }
        }
    },
    'outputs': {
        'result': {
            'title': 'Mapping templates',
            'description': 'Mapping templates',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'plugin': 'wis2box.data.csv2bufr.ObservationDataCSV2BUFR'
        }
    }
}


class MappingsInfoProcessor(BaseProcessor):
    """WMO RA Processor"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: wis2box_api.plugins.process.mappings_info.MappingsInfoProcessor # noqa
        """

        super().__init__(processor_def, PROCESS_DEF)

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        mimetype = 'application/json'

        valid_plugins = [
            'wis2box.data.csv2bufr.ObservationDataCSV2BUFR'
        ]

        plugin_id = data.get('plugin')

        if plugin_id not in valid_plugins:
            raise ProcessorExecuteError('Invalid plugin ID')

        templates = []
        if plugin_id == 'wis2box.data.csv2bufr.ObservationDataCSV2BUFR':
            for template in c2bt.list_templates().values():
                LOGGER.info(template)
                id_ = template['path']
                title = template['path']
                if '/opt/csv2bufr/templates/' in id_:
                    id_ = id_.replace('/opt/csv2bufr/templates/', '').replace('.json', '') # noqa
                # to ensure backward compatibility with existing titles
                if title.find('aws-template') != -1:
                    title = 'AWS'
                elif title.find('daycli-template') != -1:
                    title = 'DayCLI'
                elif title.find('CampbellAfrica-v1-template') != -1:
                    title = 'WIS2-pilot-template-2021'
                else:
                    # extract title from filename
                    title = id_.split('/')[-1].replace('.json', '')
                templates.append({
                    'id': id_,
                    'title': title
                })
        outputs = {
            'templates': templates
        }

        return mimetype, outputs

    def __repr__(self):
        return '<TemplatesInfoProcessor> {}'.format(self.name)
