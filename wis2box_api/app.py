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
import requests

from flask import Flask, redirect, request
from pygeoapi.flask_app import BLUEPRINT as pygeoapi_blueprint

from wis2box_api.flask_admin import ADMIN_BLUEPRINT

app = Flask(__name__, static_url_path='/oapi/static')
app.url_map.strict_slashes = False

app.register_blueprint(ADMIN_BLUEPRINT, url_prefix='/oapi')
app.register_blueprint(pygeoapi_blueprint, url_prefix='/oapi')

try:
    from flask_cors import CORS
    CORS(app)
except ImportError:  # CORS needs to be handled by upstream server
    pass


@app.route('/oapi/sns', methods=['POST'])
def sns_listener():
    """
    Dedicated endpoint to receive raw AWS SNS notifications.
    This bypasses the pygeoapi OGC API-Processes structure.
    """
    try:
        data = request.get_json()
    except Exception:
        return {"error": "Invalid JSON"}, 400

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
        # Here, implement your processing logic
        print("Received S3 event:", message)
        return {"status": "notification received"}, 200
    else:
        return {"error": "Unhandled message type"}, 400


@app.route('/')
def home():
    return redirect('https://docs.wis2box.wis.wmo.int', code=302)
