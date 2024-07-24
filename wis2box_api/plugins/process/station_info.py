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

from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import os
import logging
import requests

from pygeoapi.util import yaml_load, get_path_basename
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from wis2box_api.wis2box.env import WIS2BOX_API_URL, WIS2BOX_DOCKER_API_URL

LOGGER = logging.getLogger(__name__)

with open(os.getenv('PYGEOAPI_CONFIG'), encoding='utf8') as fh:
    CONFIG = yaml_load(fh)


PROCESS_DEF = {
    'version': '0.1.0',
    'id': 'station-info',
    'title': 'Station Information',
    'description': 'Returns the station feature collection with'
    ' counted observations by station',
    'keywords': [],
    'links': [],
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
        },
        'wigos_station_identifier': {
            'title': {'en': 'WIGOS Station Identifier'},
            'schema': {
                'type': 'array',
                'minItems': 1,
                'items': {'type': 'string'}
            },
            'minOccurs': 0,
            'maxOccurs': 1,
            'metadata': None  # TODO how to use?
        },
        'days': {
            'title': {'en': 'Days'},
            'schema': {'type': 'number', 'default': 1},
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'years': {
            'title': {'en': 'Years'},
            'schema': {'type': 'number', 'default': 0},
            'minOccurs': 0,
            'maxOccurs': 1
        },
    },
    'outputs': {
        'path': {
            'title': {'en': 'FeatureCollection'},
            'description': {
                'en': 'A GeoJSON FeatureCollection of the '
                'stations with their status'
            },
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'collection': 'urn:x-wmo:md:mw-mw_met_centre:surface-weather-observations'  # noqa
        }
    }
}


class StationInfoProcessor(BaseProcessor):
    """Station Info Processor"""

    def __init__(self, processor_def):
        """
        Initialize object

        :param processor_def: provider definition

        :returns: wis2box_api.plugins.process.station_info.StationInfoProcessor
        """

        super().__init__(processor_def, PROCESS_DEF)

        host = os.environ['WIS2BOX_API_BACKEND_URL']
        self.es = Elasticsearch(host)

        if not self.es.ping():
            msg = 'Cannot connect to Elasticsearch'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

    def execute(self, data):
        """
        Execute Process

        :param data: processor arguments

        :returns: 'application/json'
        """

        mimetype = 'application/json'
        outputs = {
            'id': 'path',
            'code': 'success',
            'value': None,
        }

        try:
            collection_id = data['collection']
            topic = 'notfound'
            # get the topic from the collection
            url = f'{WIS2BOX_DOCKER_API_URL}/collections/discovery-metadata/items/{collection_id}?f=json' # noqa
            response = requests.get(url)
            if response.status_code == 200 and 'wmo:topicHierarchy' in response.json()['properties']:  # noqa
                topic = response.json()['properties']['wmo:topicHierarchy']
            else:
                LOGGER.error(f'Error getting topic for collection {collection_id}') # noqa
                raise ProcessorExecuteError('Error getting topic for collection') # noqa
        except KeyError:
            msg = 'Collection id required'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        wigos_station_identifiers = data.get('wigos_station_identifier', [])
        if isinstance(wigos_station_identifiers, list) is False:
            msg = 'wigos_station_identifier must be an array'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        # determine the index to query from pygeoapi config
        index = 'notfound'
        if collection_id in CONFIG['resources']:
            collection_config = CONFIG['resources'][collection_id]
            index_url = collection_config['providers'][0]['data']
            index = get_path_basename(index_url)

        if index == 'notfound':
            msg = 'Error determining index to query'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        days = data.get('days', 1) + (data.get('years', 0) * 365)
        _time_delta = timedelta(days=days, minutes=59, seconds=59)
        date_offset = (datetime.utcnow() - _time_delta).isoformat()

        if CONFIG['resources'].get('stations') is None:
            msg = 'stations collection does not exist'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)

        fc = self._load_stations(wigos_station_identifiers, topic, collection_id) # noqa
        if None in fc['features']:
            msg = 'Invalid WIGOS station identifier provided'
            LOGGER.error(msg)
            raise ProcessorExecuteError(msg)
        else:
            outputs['value'] = fc

        query_core = {
            'bool': {
                'filter': [
                    {
                        'range': {
                            'properties.resultTime.raw': {'gte': date_offset}
                        }
                    }
                ]
            }
        }
        query_agg = {
            'each': {
                'terms': {
                    'field': 'properties.wigos_station_identifier.raw',
                    'size': 64000
                },
                'aggs': {
                    'count': {
                        'terms': {'field': 'reportId.raw', 'size': 64000}
                    }
                }
            }
        }
        query = {'size': 0, 'query': query_core, 'aggs': query_agg}

        try:
            response = self.es.search(index=index, **query)
        except Exception as err:
            msg = f'Error querying Elasticsearch with index={index}: {err}'
            raise ProcessorExecuteError(err)

        response_buckets = response['aggregations']['each']['buckets']

        hits = {b['key']: len(b['count']['buckets']) for b in response_buckets} # noqa

        for station in outputs['value']['features']:
            station['properties']['num_obs'] = hits.get(station['id'], 0)

        return mimetype, outputs

    def _load_stations(self, wigos_station_identifiers: list = [],
                       topic: str = '', collection_id: str = ''):
        fc = {'type': 'FeatureCollection', 'features': []}

        # load stations from backend
        LOGGER.info("Loading stations from backend")
        es = Elasticsearch(os.getenv('WIS2BOX_API_BACKEND_URL'))
        nbatch = 50
        res = es.search(index="stations", query={"match_all": {}}, size=nbatch)
        if len(res['hits']['hits']) == 0:
            LOGGER.error('No stations found')
            return fc
        for hit in res['hits']['hits']:
            fc['features'].append(hit['_source'])
        while len(res['hits']['hits']) == nbatch:
            res = es.search(index="stations",
                            query={"match_all": {}},
                            size=nbatch,
                            from_=len(fc['features']))
            for hit in res['hits']['hits']:
                fc['features'].append(hit['_source'])
        LOGGER.info(f"Found {len(fc['features'])} stations")

        dm_link = {
            "rel": "canonical",
            "href": f"{WIS2BOX_API_URL}/collections/discovery-metadata/items/{collection_id}",  # noqa
            "type": "application/json",
            "title": collection_id  # noqa
        }

        # filter by topic
        ff = []
        try:
            for f in fc['features']:
                if topic.replace('origin/a/wis2/', '') in f['properties']['topics']:  # noqa
                    f['properties']['topic'] = collection_id
                    f['links'] = [dm_link]
                    ff.append(f)
                elif topic in f['properties']['topics']:
                    f['properties']['topic'] = collection_id
                    f['links'] = [dm_link]
                    ff.append(f)
            fc['features'] = ff
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error('Error filtering stations by topic')
            LOGGER.error('Returning empty feature collection')
            fc['features'] = []
        # after filter
        LOGGER.info(f"Found {len(fc['features'])} stations for topic {topic}")

        return fc

    def __repr__(self):
        return '<StationInfoProcessor> {}'.format(self.name)
