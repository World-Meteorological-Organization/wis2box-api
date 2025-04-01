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

import os
import logging

from flask import Blueprint, request

from pygeoapi.flask_app import execute_from_flask
from pygeoapi.util import yaml_load

from wis2box_api.asyncapi import AsyncAPI

LOGGER = logging.getLogger(__name__)

CONFIG = None

if 'PYGEOAPI_CONFIG' not in os.environ:
    raise RuntimeError('PYGEOAPI_CONFIG environment variable not set')

with open(os.environ.get('PYGEOAPI_CONFIG'), encoding='utf8') as fh:
    CONFIG = yaml_load(fh)

asyncapi_ = AsyncAPI(CONFIG)

ASYNCAPI_BLUEPRINT = Blueprint(
    'asyncapi',
    __name__
)


@ASYNCAPI_BLUEPRINT.route('/asyncapi')
def home():
    """
    AsyncAPI endpoint

    :returns: HTTP response
    """

    return execute_from_flask(asyncapi_.get_asyncapi, request)
