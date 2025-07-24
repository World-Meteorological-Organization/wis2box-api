###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# 'License'); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

# integration tests assume that the workflow in
# .github/workflows/wis2box_test.yml has been executed

import json
import os
import time

import requests

import paho.mqtt.client as mqtt

URL = 'http://localhost:4343'
API_URL = f'{URL}/oapi'


def store_message(message, channel):
    """store message in current directory with filename

    :param message: received message
    :param filename: filename
    """
    # decode message payload as json
    message = json.loads(message.payload.decode())
    # if message matches channel store it
    if message['channel'] == channel:
        filename = message['channel'].replace('/', '_') + '.json'
        # store message in current directory with filename
        with open(filename, 'w') as f:
            json.dump(message, f, indent=4)


def transform_to_string(process_name: str, data: dict) -> dict:
    """Transform data to bufr or geojson

    :param process_name: name of the process
    :param data: data to be transformed

    :returns: response_json
    """

    url = f'{API_URL}/processes/{process_name}/execution'

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }
    headers['Prefer'] = 'respond-async'
    response = requests.post(url, headers=headers, json=data)
    headers_json = dict(response.headers)
    assert response.status_code == 201, f'Aysnc response status code: {response.status_code}'  # noqa
    # print(headers_json)
    if 'Location' not in headers_json:
        assert False, f'Location not in headers: {headers_json}'
    # get the job_location_url from the response
    job_location_url = headers_json['Location']
    # print(job_location_url)
    status = "accepted"
    while status == "accepted" or status == "running":
        # get the job status
        response = requests.get(job_location_url, headers=headers)
        response_json = response.json()
        status = response_json['status']
        time.sleep(0.1)
    assert status == "successful"
    # get result from job_location_url/results?f=json
    response = requests.get(f'{job_location_url}/results?f=json', headers=headers) # noqa
    response_json = response.json()
    return response_json


def validate_data_transform_process(
        process_name: str,
        data: dict,
        expected_response: dict
        ):
    """Run the data transform process and validate the response

    :param process_name: name of the process
    :param data: data to be transformed
    :param expected_response: expected response
    :param message_published: whether a message should be published or not

    """

    url = f'{API_URL}/processes/{process_name}/execution'

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }
    headers['Prefer'] = 'respond-async'
    response = requests.post(url, headers=headers, json=data)
    headers_json = dict(response.headers)
    assert response.status_code == 201, f'Aysnc response status code: {response.status_code}'  # noqa
    # print(headers_json)
    if 'Location' not in headers_json:
        assert False, f'Location not in headers: {headers_json}'
    # get the job_location_url from the response
    job_location_url = headers_json['Location']
    # print(job_location_url)
    status = "accepted"
    while status == "accepted" or status == "running":
        # get the job status
        response = requests.get(job_location_url, headers=headers)
        response_json = response.json()
        status = response_json['status']
        time.sleep(0.1)
    assert status == "successful"
    # get result from job_location_url/results?f=json
    response = requests.get(f'{job_location_url}/results?f=json', headers=headers) # noqa
    response_json = response.json()

    print(response_json)
    for key in ['result', 'messages transformed', 'messages published', 'errors', 'warnings']:  # noqa
        assert response_json[key] == expected_response[key]

    # if no data_items are returned, no message is expected to be published
    if expected_response['data_items'] == []:
        return

    filename = data['inputs']['channel'].replace('/', '_') + '.json'

    # check that the file has been created
    try:
        # open the received message
        with open(filename) as f:
            message = json.load(f)
    except Exception as e:
        assert False, f'Error opening file: {e}'

    # rm the file
    os.remove(filename)

    # create the expected message
    expected_message = {
        'channel': expected_response['data_items'][0]['channel'],
        'filename': expected_response['data_items'][0]['filename'],
        'data': expected_response['data_items'][0]['data'],
        '_meta': expected_response['data_items'][0]['_meta']
    }

    # compare the received message with the expected message
    for key in ['channel', 'filename', 'data', '_meta']:
        if message[key] != expected_message[key]:
            print(f"Issue found for message['{key}']")
            print(f' found: {message[key]}')
            print(f' expected: {expected_message[key]}')
        assert message[key] == expected_message[key]


def test_universal():
    """Test universal data process"""
    process_name = 'wis2box-publish_data'
    data = {
        "inputs": {
            "metadata_id": "urn:wmo:md:universal:test",
            "channel": "universal-test/data/recommended/weather/aviation/metar", # noqa
            "notify": True,
            "filename": "SALT31_EYVI_201020.txt",
            "data": "SALT31 EYVI 201020\nMETAR EYVI 201020Z 28008KT 230V310 CAVOK 18/06 Q1008 NOSIG=", # noqa
            "datetime": "2025-05-20T10:20:00Z",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    24.9384,
                    60.1695
                ]
            }
        }
    }
    expected_response = {
        "result": "success",
        "messages transformed": 1,
        "messages published": 1,
        "data_items": [{
            "data": "U0FMVDMxIEVZVkkgMjAxMDIwCk1FVEFSIEVZVkkgMjAxMDIwWiAyODAwOEtUIDIzMFYzMTAgQ0FWT0sgMTgvMDYgUTEwMDggTk9TSUc9", # noqa
            "filename": data['inputs']['filename'],
            "_meta": {
                "id": str(data['inputs']['filename']).split('.')[0],
                "data_date": '2025-05-20T10:20:00+00:00',
                "geometry": data['inputs']['geometry'],
            },
            'channel': data['inputs']['channel']
        }],
        "errors": [],
        "warnings": []
    }
    # start mqtt client
    client = mqtt.Client('wis2box-universal')
    # user credentials wis2box:wis2box
    client.username_pw_set('wis2box', 'wis2box')
    # connect to the broker
    client.connect('localhost', 5883, 60)
    # subscribe to the topic
    client.subscribe('wis2box/data/publication')
    # define callback function for received messages
    client.on_message = lambda client, userdata, message: store_message(message, channel=data['inputs']['channel']) # noqa
    # start the loop
    client.loop_start()
    validate_data_transform_process(process_name, data, expected_response)
    # stop the loop
    client.loop_stop()
    # disconnect from the broker
    client.disconnect()


def test_universal_binary():
    """Test universal data process with large binary data"""
    process_name = 'wis2box-publish_data'
    data = {
        "inputs": {
            "data": "QlVGUgABAgQAABYAAFAAAAAAAAIAHAAH5gMVAAAAAAALAAABgMGWx1YAANUABOIAAAMTYzNDQAAAAAAAAAAAAAACCsJqenKiKpoaqpJ4AAAAAAAAAAAAAvzGqABiq+AldarlDSKJFBc//73w+6E0BQEAZDRftA7YAMgfQAZAAD//6YcwXPqFAIJkAuAf0HFAP////////////////////////////////////w4/0cj/4AH6AAAGQ/0AA////4BkP0QANV6/RAA0WoH0YJ/YmgZ/f2///7pgIBO///////f/////////gAAAB///////v/o/////w//ALJ7///w3Nzc3", # noqa
            "channel": "universal-test-binary/data/core/weather/experimental",
            "metadata_id": "urn:wmo:md:universal-test-binary",
            "notify": True,
            "filename": "my_data.bin",
            "datetime": "2025-06-23T15:03:00.000Z",
            "wigos_station_identifier": "0-20000-0-notreal",
            "is_binary": True
        }
    }
    expected_response = {
        "result": "success",
        "messages transformed": 1,
        "messages published": 1,
        "data_items": [{
            "data": data['inputs']['data'],
            "filename": data['inputs']['filename'],
            "_meta": {
                "id": str(data['inputs']['filename']).split('.')[0],
                "data_date": '2025-06-23T15:03:00+00:00',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [11.8817, -4.8045, 18.0]
                },
                'wigos_station_identifier': '0-20000-0-notreal'
            },
            'channel': data['inputs']['channel']
        }],
        "errors": [],
        "warnings": []
    }
    # start mqtt client
    client = mqtt.Client('wis2box-universal-binary')
    # user credentials wis2box:wis2box
    client.username_pw_set('wis2box', 'wis2box')
    # connect to the broker
    client.connect('localhost', 5883, 60)
    # subscribe to the topic
    client.subscribe('wis2box/data/publication')
    # define callback function for received messages
    client.on_message = lambda client, userdata, message: store_message(message, channel=data['inputs']['channel']) # noqa
    # start the loop
    client.loop_start()
    validate_data_transform_process(process_name, data, expected_response)
    # stop the loop
    client.loop_stop()
    # disconnect from the broker
    client.disconnect()


def test_synop2bufr():
    """Test synop2bufr"""

    process_name = 'wis2box-synop2bufr'
    data = {
        'inputs': {
            'channel': 'synop-test/data/core/weather/surface-based-observations/synop', # noqa
            'metadata_id': 'urn:wmo:md:synop:test',
            'year': 2023,
            'month': 1,
            'notify': True,
            'data': 'AAXX 19064 64400 36/// /0000 10102 20072 30068 40182 53001 333 20056 91003 555 10302 91018=' # noqa
        }
    }
    expected_response = {
        'result': 'success',
        'messages transformed': 1,
        'messages published': 1,
        'data_items': [
            {
                'data': 'QlVGUgABeAQAABYAAAAAAAAAAAJuHgAH5wETBgAAAAALAAABgMGWx2AAAUsABOIAAANjQ0MDAAAAAAAAAAAAAAAIDIKCekpyoilqcnpKkigAAAAAAAAAAPzimYBA/78kmTlBBUCDB///////////////////////////+dUnxn1P///////////26vbYOl////////////////////////////////////////////////////////////////AACP/yP+T///////////////7///v9f/////////////////////////////////+J/b/gAff2/4Dz/X/////////////////////////////////////7+kAH//v6QANnH//////AAf/wAF+j//////////////v0f//////f//+/R/+////////////////////fo//////////////////3+oAP///////////////////8Nzc3Nw==', # noqa
                'filename': 'WIGOS_0-20000-0-64400_20230119T060000.bufr4',
                '_meta': {
                    'id': 'WIGOS_0-20000-0-64400_20230119T060000',
                    'wigos_station_identifier': '0-20000-0-64400',
                    'data_date': '2023-01-19T06:00:00',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [11.8817, -4.8045]
                    }
                },
                'channel': data['inputs']['channel']
            }
        ],
        'errors': [],
        'warnings': []
    }
    # start mqtt client
    client = mqtt.Client('wis2box-synop2bufr')
    # user credentials wis2box:wis2box
    client.username_pw_set('wis2box', 'wis2box')
    # connect to the broker
    client.connect('localhost', 5883, 60)
    # subscribe to the topic
    client.subscribe('wis2box/data/publication')
    # define callback function for received messages
    client.on_message = lambda client, userdata, message: store_message(message, channel=data['inputs']['channel']) # noqa
    # start the loop
    client.loop_start()
    validate_data_transform_process(process_name, data, expected_response)
    # stop the loop
    client.loop_stop()
    # disconnect from the broker
    client.disconnect()


def test_csv2bufr():
    """Test csv2bufr"""

    process_name = 'wis2box-csv2bufr'
    data = {
        'inputs': {
            'data': 'wsi_series,wsi_issuer,wsi_issue_number,wsi_local,wmo_block_number,wmo_station_number,station_type,year,month,day,hour,minute,latitude,longitude,station_height_above_msl,barometer_height_above_msl,station_pressure,msl_pressure,geopotential_height,thermometer_height,air_temperature,dewpoint_temperature,relative_humidity,method_of_ground_state_measurement,ground_state,method_of_snow_depth_measurement,snow_depth,precipitation_intensity,anemometer_height,time_period_of_wind,wind_direction,wind_speed,maximum_wind_gust_direction_10_minutes,maximum_wind_gust_speed_10_minutes,maximum_wind_gust_direction_1_hour,maximum_wind_gust_speed_1_hour,maximum_wind_gust_direction_3_hours,maximum_wind_gust_speed_3_hours,rain_sensor_height,total_precipitation_1_hour,total_precipitation_3_hours,total_precipitation_6_hours,total_precipitation_12_hours,total_precipitation_24_hours\n0,20000,0,15015,15,15,1,2022,3,31,0,0,47.77706163,23.94046026,503,504.43,100940,20104,1448,5,298.15,294.55,80.4,3,1,1,0,0.004,10,-10,30,3,30,5,40,9,20,11,2,4.7,5.3,7.9,9.5,11.4', # noqa
            'metadata_id': 'urn:wmo:md:csv:test',
            'channel': 'csv-test/data/core/weather/surface-based-observations/synop', # noqa
            'notify': True,
            'template': 'aws-template'
        }
    }
    expected_response = {
        'result': 'partial success',
        'messages transformed': 1,
        'messages published': 1,
        'data_items': [
            {
                'data': 'QlVGUgABgAQAABYAAAAAAAAAAAJuHgAH5gMfAAAAAAALAAABgMGWx2AAAVMABOIAAAMTUwMTUAAAAAAAAAAAAAAB4H//////////////////////////+vzG+ABpHZUm5gfCNGEap///////////////////////////+du////////+CZAB9P/3R3cw+h////////////////////////////////////////////////8wiAAX//////////Af////gP////////////////////////////////////+AyP//////////////////////////+J/YPAPff2DwGT4goBadMCgN3//////////////////////////////////////////A+j/f/AMH/QDZ/oBQf0AYH6AHP////////////////////////////////////////////////////////////////////////////////////8A3Nzc3', # noqa
                'filename': 'WIGOS_0-20000-0-15015_20220331T000000.bufr4',
                '_meta': {
                    'id': 'WIGOS_0-20000-0-15015_20220331T000000',
                    'wigos_station_identifier': '0-20000-0-15015',
                    'data_date': '2022-03-31T00:00:00',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [23.94046026, 47.77706163]
                    }
                },
                'channel': data['inputs']['channel']
            }
        ],
        'errors': [],
        'warnings': [
            '#1#pressureReducedToMeanSeaLevel: Value (20104.0) out of valid range (50000 - 150000).; Element set to missing' # noqa
        ]
    }

    # start mqtt client
    client = mqtt.Client('wis2box-csv2bufr')
    # user credentials wis2box:wis2box
    client.username_pw_set('wis2box', 'wis2box')
    # connect to the broker
    client.connect('localhost', 5883, 60)
    # subscribe to the topic
    client.subscribe('wis2box/data/publication')
    # define callback function for received messages
    client.on_message = lambda client, userdata, message: store_message(message, channel=data['inputs']['channel']) # noqa
    # start the loop
    client.loop_start()
    # transform bufr message
    validate_data_transform_process(process_name, data, expected_response)
    # stop the loop
    client.loop_stop()
    # disconnect from the broker
    client.disconnect()


def test_csv2bufr_wrong_location():
    """Test csv2bufr"""

    process_name = 'wis2box-csv2bufr'
    data = {
        'inputs': {
            'data': 'wsi_series,wsi_issuer,wsi_issue_number,wsi_local,wmo_block_number,wmo_station_number,station_type,year,month,day,hour,minute,latitude,longitude,station_height_above_msl,barometer_height_above_msl,station_pressure,msl_pressure,geopotential_height,thermometer_height,air_temperature,dewpoint_temperature,relative_humidity,method_of_ground_state_measurement,ground_state,method_of_snow_depth_measurement,snow_depth,precipitation_intensity,anemometer_height,time_period_of_wind,wind_direction,wind_speed,maximum_wind_gust_direction_10_minutes,maximum_wind_gust_speed_10_minutes,maximum_wind_gust_direction_1_hour,maximum_wind_gust_speed_1_hour,maximum_wind_gust_direction_3_hours,maximum_wind_gust_speed_3_hours,rain_sensor_height,total_precipitation_1_hour,total_precipitation_3_hours,total_precipitation_6_hours,total_precipitation_12_hours,total_precipitation_24_hours\n0,20000,0,15015,15,15,1,2022,3,31,0,0,47.77706163,33.94046026,503,504.43,100940,50104,1448,5,298.15,294.55,80.4,3,1,1,0,0.004,10,-10,30,3,30,5,40,9,20,11,2,4.7,5.3,7.9,9.5,11.4', # noqa
            'metadata_id': 'urn:wmo:md:csv:test',
            'channel': 'csv-test/data/core/weather/surface-based-observations/synop', # noqa
            'notify': True,
            'template': 'aws-template'
        }
    }
    expected_response = {
        'result': 'partial success',
        'messages transformed': 1,
        'messages published': 0,
        'data_items': [],
        'errors': [],
        'warnings': [
            'Station 0-20000-0-15015: location reported in data is 748940.72 meters from station-location; skipping' # noqa
        ]
    }

    # start mqtt client
    client = mqtt.Client('wis2box-csv2bufr')
    # user credentials wis2box:wis2box
    client.username_pw_set('wis2box', 'wis2box')
    # connect to the broker
    client.connect('localhost', 5883, 60)
    # subscribe to the topic
    client.subscribe('wis2box/data/publication')
    # define callback function for received messages
    client.on_message = lambda client, userdata, message: store_message(message, channel=data['inputs']['channel']) # noqa
    # start the loop
    client.loop_start()
    # transform bufr message
    validate_data_transform_process(process_name, data, expected_response)
    # stop the loop
    client.loop_stop()
    # disconnect from the broker
    client.disconnect()


def test_bufr2bufr():
    """Test bufr2bufr"""

    process_name = 'wis2box-bufr2bufr'
    data = {
        'inputs': {
            'data': 'SVNNRDAyIExJSUIgMjEwMDAwIFJSQQ0NCkJVRlIAAOwEAAAWAABQAAAAAAACABAAB+YDFQAAAAAACQAAAYDHVgAAwQAgrCanpyoiqaGqqSeQEBAQEBAQEBAQL8xqgAYqvgJXWq5Q0iiRQXP/+98PuhNAUBAGQ0X7QO2ADIH0AGQAA//+mHMFz6hQCCZALgH9BxQD////////////////////////////////////8OP9HI/+AB+gAABkP9AAP///+AZD9EADVev0QANFqB9GCf2JoGf39v//+6YCATv//////3/////////4AAAAf//////7/6P////8P/wCye///8A3Nzc3DQ0K', # noqa
            'metadata_id': 'urn:wmo:md:bufr:test',
            'channel': 'bufr-test/data/core/weather/surface-based-observations/synop', # noqa
            'notify': True
        },
    }
    expected_response = {
        'result': 'success',
        'messages transformed': 1,
        'messages published': 1,
        'data_items': [
            {
                'data': 'QlVGUgABAgQAABYAAFAAAAAAAAIAHAAH5gMVAAAAAAALAAABgMGWx1YAANUABOIAAAMTYzNDQAAAAAAAAAAAAAACCsJqenKiKpoaqpJ4AAAAAAAAAAAAAvzGqABiq+AldarlDSKJFBc//73w+6E0BQEAZDRftA7YAMgfQAZAAD//6YcwXPqFAIJkAuAf0HFAP////////////////////////////////////w4/0cj/4AH6AAAGQ/0AA////4BkP0QANV6/RAA0WoH0YJ/YmgZ/f2///7pgIBO///////f/////////gAAAB///////v/o/////w//ALJ7///w3Nzc3', # noqa
                'filename': 'WIGOS_0-20000-0-16344_20220321T000000.bufr4',
                '_meta': {
                    'id': 'WIGOS_0-20000-0-16344_20220321T000000',
                    'wigos_station_identifier': '0-20000-0-16344',
                    'data_date': '2022-03-21T00:00:00',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': [16.39639, 39.33056, 1669.0]
                    }
                },
                'channel': data['inputs']['channel']
            }
        ],
        'errors': [],
        'warnings': []
    }

    # start mqtt client
    client = mqtt.Client('wis2box-bufr2bufr')
    # user credentials wis2box:wis2box
    client.username_pw_set('wis2box', 'wis2box')
    # connect to the broker
    client.connect('localhost', 5883, 60)
    # subscribe to the topic
    client.subscribe('wis2box/data/publication')
    # define callback function for received messages
    client.on_message = lambda client, userdata, message: store_message(message, channel=data['inputs']['channel']) # noqa
    # start the loop
    client.loop_start()
    # transform bufr message
    validate_data_transform_process(process_name, data, expected_response)
    # stop the loop
    client.loop_stop()
    # disconnect from the broker
    client.disconnect()


def test_cap2geojson():
    """Test cap2geojson"""

    process_name = 'cap2geojson'

    script_dir = os.path.dirname(__file__)
    cap_xml_path = os.path.join(script_dir, './data/sc.xml')
    cap_geojson_path = os.path.join(script_dir, './data/sc.geojson')

    with open(cap_xml_path, 'r') as f:
        cap_xml = f.read()

    data = {
        'inputs': {
            'data': cap_xml
        }
    }
    output = transform_to_string(process_name, data)

    with open(cap_geojson_path, 'r') as f: # noqa
        cap_geojson = json.load(f)

    assert 'items' in output
    assert len(output['items']) == 1
    assert output['items'][0]['features'][0]['properties'] == cap_geojson['features'][0]['properties'] # noqa


def test_mappings_info():
    """Test mappings_info process"""

    process_name = 'mappings-info'
    data = {
        'inputs': {
            'plugin': 'wis2box.data.csv2bufr.ObservationDataCSV2BUFR'
        }
    }

    # execute the process and get the result
    result = requests.post(f'{API_URL}/processes/{process_name}/execution', json=data) # noqa

    # check the status code
    assert result.status_code == 200

    response = result.json()

    expected_response = {
        "templates": [
            {
                "id": "/data/wis2box/mappings/my_csv2bufr_mappings.json",
                "title": "my_csv2bufr_mappings"
            },
            {
                "id": "daycli-template",
                "title": "DayCLI"
            },
            {
                "id": "CampbellAfrica-v1-template",
                "title": "WIS2-pilot-template-2021"
            },
            {
                "id": "aws-template",
                "title": "AWS"
            }
        ]
    }

    # compare that all expected templates are present, regardless of the order
    assert len(response['templates']) == len(expected_response['templates'])
    templates = response['templates']
    for template in expected_response['templates']:
        assert template['id'] in [t['id'] for t in templates]
        assert template['title'] in [t['title'] for t in templates]
