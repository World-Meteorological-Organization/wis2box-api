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

from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch

import os
import minio
import logging
import requests


from pygeoapi.util import yaml_load, get_path_basename
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from wis2box_api.wis2box.env import WIS2BOX_DOCKER_API_URL

LOGGER = logging.getLogger(__name__)

STORAGE_PUBLIC = os.getenv('WIS2BOX_STORAGE_PUBLIC')
STORAGE_INCOMING = os.getenv('WIS2BOX_STORAGE_INCOMING')

STORAGE_URL = os.getenv('WIS2BOX_STORAGE_SOURCE')
ACCESS_KEY = os.getenv('WIS2BOX_STORAGE_USERNAME')
SECRET_KEY = os.getenv('WIS2BOX_STORAGE_PASSWORD')

IS_SECURE = False
S3_ENDPOINT = STORAGE_URL.replace('http://', '')
if STORAGE_URL.startswith('https://'):
    IS_SECURE = True
    S3_ENDPOINT = STORAGE_URL.replace('https://', '')


PROCESS_DEF = {
    'version': '0.1.0',
    'id': 'dataset-info',
    'title': 'Dataset Information',
    'description': 'Retrieve information about datasets contained in the WIS2BOX', # noqa
    'keywords': [],
    'links': [],
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'inputs': {
        'collection': {
            'title': {'en': 'Collection identifier'},
            'description': {'en': 'Collection identifier'},
            'keywords': {'en': ['collection', 'topic', 'dataset']},
            'schema': {
                'type': 'string',
                'default': None
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None  # TODO how to use?
        }
    },
    'outputs': {
        'dataset_info': {
            'title': {'en': 'Dataset Info'},
            'description': {
                'en': 'Dataset info in JSON format'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'collection': 'urn:wmo:md:mw-mw_met_centre:surface-weather-observations'  # noqa
        }
    }
}


class DatasetInfoProcessor(BaseProcessor):
    """Station Info Processor"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: wis2box_api.plugins.process.dataset_info.DatasetInfoProcessor
        """

        super().__init__(processor_def, PROCESS_DEF)

        es_host = os.environ['WIS2BOX_API_BACKEND_URL']
        self.es = Elasticsearch(es_host)
        if not self.es.ping():
            msg = 'Cannot connect to Elasticsearch'
            LOGGER.error(msg)
            self.es = None

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        LOGGER.info('dataset-info processor execute')

        mimetype = 'application/json'

        # load the api_config at execution time (in case it has changed)
        api_config = None
        with open(os.getenv('PYGEOAPI_CONFIG'), encoding='utf8') as fh:
            api_config = yaml_load(fh)
        if api_config is None:
            msg = 'Error loading pygeoapi config'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        dataset_info = {}

        collection_id = data['collection'] if 'collection' in data else None

        # loop over all metadata items and optionally filter by collection
        try:
            url = f'{WIS2BOX_DOCKER_API_URL}/collections/discovery-metadata/items?f=json' # noqa
            response = requests.get(url)
            if response.status_code == 200:
                for item in response.json()['features']:
                    key = item['properties']['identifier']
                    if collection_id is None or collection_id == key:
                        # find index in api_config
                        index = 'notfound'
                        if key in api_config['resources']:
                            collection_config = api_config['resources'][key]
                            index_url = collection_config['providers'][0]['data'] # noqa
                            index = get_path_basename(index_url)
                        # fill dataset_info dict
                        dataset_info[key] = {
                            'topic': item['properties']['wmo:topicHierarchy'],
                            'files_incoming_24hrs': 0,
                            'files_public_24hrs': 0,
                            'timestamp_last_incoming': None,
                            'timestamp_last_public': None,
                            'index': index,
                            'index_status': None
                        }
            else:
                LOGGER.error(f'Error getting collection list: {response.text}')
                raise ProcessorExecuteError('Error getting collection list')
        except Exception as err:
            LOGGER.error(f'Error getting collection list: {err}')
            raise ProcessorExecuteError('Error getting collection list')

        # define date offset
        now_minus_24hrs = datetime.now(timezone.utc) - timedelta(hours=24)
        incoming_bucket_info = self._get_bucket_info(STORAGE_INCOMING, now_minus_24hrs) # noqa
        public_bucket_info = self._get_bucket_info(STORAGE_PUBLIC, now_minus_24hrs) # noqa

        for c_id in dataset_info:
            topic = (dataset_info[c_id]['topic']).replace('origin/a/wis2/', '')
            es_index = dataset_info[c_id]['index']
            if c_id in incoming_bucket_info or topic in incoming_bucket_info:
                key = c_id if c_id in incoming_bucket_info else topic
                dataset_info[c_id]['files_incoming_24hrs'] = incoming_bucket_info[key]['files_last24hrs'] # noqa
                dataset_info[c_id]['timestamp_last_incoming'] = incoming_bucket_info[key]['last_timestamp'] # noqa
            if c_id in public_bucket_info or topic in public_bucket_info:
                key = c_id if c_id in public_bucket_info else topic
                dataset_info[c_id]['files_public_24hrs'] = public_bucket_info[key]['files_last24hrs'] # noqa
                dataset_info[c_id]['timestamp_last_public'] = public_bucket_info[key]['last_timestamp'] # noqa
            if dataset_info[c_id]['timestamp_last_incoming'] is not None:
                dataset_info[c_id]['timestamp_last_incoming'] = dataset_info[c_id]['timestamp_last_incoming'].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ') # noqa
            if dataset_info[c_id]['timestamp_last_public'] is not None:
                dataset_info[c_id]['timestamp_last_public'] = dataset_info[c_id]['timestamp_last_public'].astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ') # noqa
            if self.es is not None and es_index != 'notfound':
                dataset_info[c_id]['index_status'] = self._get_es_index_info(es_index) # noqa
            continue
        outputs = {
            'dataset_info': dataset_info
        }
        return mimetype, outputs

    def _get_es_index_info(self, index):
        """
        Get information about an Elasticsearch index

        :param index: index

        :returns: dict with info
        """

        my_dict = {}

        if self.es is None:
            return my_dict
        try:
            # Retrieve the index settings
            settings = self.es.indices.get_settings(index=index)
            # Extract the 'read_only_allow_delete' setting
            read_only_allow_delete = settings[index]['settings']['index'].get('blocks.read_only_allow_delete', False) # noqa
            # Retrieve the index stats
            stats = self.es.indices.stats(index=index)
            # Extract the 'total' document counts
            total_docs = None
            index_failed = None
            total_size = None
            if '_all' in stats and 'primaries' in stats['_all']:
                stats = stats['_all']['primaries']
                if 'docs' in stats and 'count' in stats['docs']:
                    total_docs = stats['docs']['count']
                # extract failed index count
                if 'indexing' in stats and 'index_failed' in stats['indexing']:
                    index_failed = stats['indexing']['index_failed']
                # extract the total size of the index
                if 'store' in stats and 'size_in_bytes' in stats['store']:
                    total_size = stats['store']['size_in_bytes']
            # fill the dictionary
            my_dict = {
                'total_docs': total_docs,
                'total_size': total_size,
                'index_failed': index_failed,
                'read_only_allow_delete': read_only_allow_delete
            }
        except Exception:
            return my_dict

        return my_dict

    def _get_bucket_info(self, bucket_name, now_minus_24hrs):
        """"
        Analyze the content of the wis2box-public bucket in MinIO

        :param collection_id: collection_id

        :returns: dict with info
        """

        my_dict = {}
        try:
            minio_client = minio.Minio(
                S3_ENDPOINT,
                access_key=ACCESS_KEY,
                secret_key=SECRET_KEY,
                secure=IS_SECURE
            )
            LOGGER.info('Connected to MinIO')
        except Exception as err:
            LOGGER.error(f'Error connecting to MinIO: {err}')
            return my_dict

        for object in minio_client.list_objects(bucket_name, '', True):
            obj_name = object.object_name
            dataset_id = ''
            if bucket_name == STORAGE_PUBLIC:
                if 'wis/' not in obj_name:
                    continue
                dataset_id = obj_name.split('wis/')[1]
                dataset_id = dataset_id.replace(dataset_id.split('/')[-1],'')[:-1] # noqa
            else:
                dataset_id = obj_name.replace(obj_name.split('/')[-1], '')[:-1]
            if dataset_id not in my_dict:
                nfiles = 0
                if object.last_modified > now_minus_24hrs:
                    nfiles = 1
                my_dict[dataset_id] = {
                    'files_last24hrs': nfiles,
                    'last_timestamp': object.last_modified
                }
            else:
                if object.last_modified > now_minus_24hrs:
                    my_dict[dataset_id]['files_last24hrs'] += 1
                if object.last_modified > my_dict[dataset_id]['last_timestamp']: # noqa
                    my_dict[dataset_id]['last_timestamp'] = object.last_modified # noqa
        # return the dictionary
        return my_dict

    def __repr__(self):
        return '<DatasetInfoProcessor> {}'.format(self.name) # noqa
