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

from flask import Flask, redirect
from pygeoapi.flask_app import BLUEPRINT as pygeoapi_blueprint

from wis2box_api.flask_admin import ADMIN_BLUEPRINT

app = Flask(__name__, static_url_path='/static')
app.url_map.strict_slashes = False

app.register_blueprint(ADMIN_BLUEPRINT, url_prefix='/oapi')
app.register_blueprint(pygeoapi_blueprint, url_prefix='/oapi')

try:
    from flask_cors import CORS
    CORS(app)
except ImportError:  # CORS needs to be handled by upstream server
    pass


@app.route('/')
def home():
    return redirect('https://docs.wis2box.wis.wmo.int', code=302)
