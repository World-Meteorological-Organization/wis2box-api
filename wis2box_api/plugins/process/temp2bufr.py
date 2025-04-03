import base64
import logging
import requests

from pygeoapi.process.base import BaseProcessor
from wis2box_api.wis2box.bufr import as_geojson
from wis2box_api.wis2box.env import STORAGE_PUBLIC_URL, STORAGE_SOURCE

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'temp2bufr',
    'title': 'Extract TEMP data from BUFR',
    'description': 'Download BUFR file and extract TEMP-related data',
    'inputs': {
        'data_url': {
            'title': 'data_url',
            'description': 'URL to the BUFR file',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
        },
        'data': {
            'title': 'data',
            'description': 'Base64 encoded BUFR file content',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1,
        },
    },
    'outputs': {
        'temp_data': {
            'title': 'Extracted TEMP data',
            'description': 'List of extracted temperature-related data',
            'schema': {'type': 'array'},
        }
    }
}


class Temp2BufrProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        LOGGER.debug('Executing TEMP data extraction process')

        input_bytes = None
        error = ''
        temp_data = []

        try:
            if 'data_url' in data:
                data_url = data['data_url']
                data_url = data_url.replace(STORAGE_PUBLIC_URL, f'{STORAGE_SOURCE}/wis2box-public')
                LOGGER.debug(f'Fetching BUFR file from: {data_url}')
                result = requests.get(data_url)
                result.raise_for_status()
                input_bytes = result.content
            elif 'data' in data:
                encoded_data_bytes = data['data'].encode('utf-8')
                input_bytes = base64.b64decode(encoded_data_bytes)
            else:
                raise Exception('No valid data or data_url provided')

            LOGGER.debug('Parsing TEMP data from BUFR')
            result = as_geojson(input_bytes)
            for item in result:
                temp_data.append({
                    'phenomenonTime': item.get('geojson', {}).get('properties', {}).get('phenomenonTime'),
                    'longitude': item.get('geojson', {}).get('geometry', {}).get('coordinates', [None, None])[0],
                    'latitude': item.get('geojson', {}).get('geometry', {}).get('coordinates', [None, None])[1],
                    'zCoordinate': item.get('geojson', {}).get('properties', {}).get('parameter', {}).get('additionalProperties', {}).get('zCoordinate', {}).get('value'),
                    'zCoordinate_units': item.get('geojson', {}).get('properties', {}).get('parameter', {}).get('additionalProperties', {}).get('zCoordinate', {}).get('units'),
                    'observedProperty': item.get('geojson', {}).get('properties', {}).get('observedProperty'),
                    'value': item.get('geojson', {}).get('properties', {}).get('result', {}).get('value'),
                    'value_units': item.get('geojson', {}).get('properties', {}).get('result', {}).get('units')
                })

        except Exception as e:
            LOGGER.error(e)
            error = str(e)

        outputs = {'temp_data': temp_data, 'error': error}
        return 'application/json', outputs
