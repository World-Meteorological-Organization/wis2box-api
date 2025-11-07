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
import requests

import paho.mqtt.publish as publish

from flask import Blueprint, request

from wis2box_api.wis2box.env import BROKER_HOST
from wis2box_api.wis2box.env import BROKER_PORT
from wis2box_api.wis2box.env import BROKER_USERNAME
from wis2box_api.wis2box.env import BROKER_PASSWORD

LOGGER = logging.getLogger(__name__)

SNS_BLUEPRINT = Blueprint('sns_listener', __name__)


@SNS_BLUEPRINT.route('/sns', methods=['POST'])
def sns_listener():
    """
    Dedicated endpoint to receive raw AWS SNS notifications.
    This bypasses the pygeoapi OGC API-Processes structure.
    """
    try:
        data_as_text = request.get_data(as_text=True)
        data = json.loads(data_as_text)
    except Exception as e:
        return {"error": f"Error {e} parsing JSON: {data_as_text}"}, 400

    if not data or "Type" not in data:
        return {"error": "Missing SNS Type"}, 400

    msg_type = data["Type"]

    if msg_type == "SubscriptionConfirmation":
        # Confirm subscription
        subscribe_url = data.get("SubscribeURL")
        if subscribe_url:
            requests.get(subscribe_url)
            return {"status": "subscription confirmed"}, 200
        else:
            return {"error": "Missing SubscribeURL"}, 400
    elif msg_type == "Notification":
        # Handle S3 event
        message = json.loads(data.get("Message"))
        LOGGER.info(f'Received S3 event: {message}')
        try:
            # publish notification on internal broker
            private_auth = {
                'username': BROKER_USERNAME,
                'password': BROKER_PASSWORD
            }
            topic = 'wis2box/storage'
            publish.single(topic=topic, # noqa
                           payload=json.dumps(message),
                           qos=1,
                           retain=False,
                           hostname=BROKER_HOST,
                           port=int(BROKER_PORT),
                           auth=private_auth)
            LOGGER.debug(f'Published storage event on topic={topic}')
        except Exception as e:
            LOGGER.error(f'Error publishing on topic={topic}, error={e}')
            return {"error": "Error publishing on internal broker"}, 500
        return {"status": "Published S3 event on internal broker"}, 200
    else:
        return {"error": "Unhandled message type"}, 400
