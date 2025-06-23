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

WIS2BOX_DOCKER_API_URL = os.environ.get('WIS2BOX_DOCKER_API_URL', 'http://wis2box-api:80/oapi') # noqa
WIS2BOX_URL = os.environ.get('WIS2BOX_URL')
WIS2BOX_API_URL = os.environ.get('WIS2BOX_API_URL')

WIS2BOX_OBSERVATION_DISTANCE_THRESHOLD = os.environ.get('WIS2BOX_OBSERVATION_DISTANCE_THRESHOLD', 1000.0)  # default set to 1000 meters # noqa

API_BACKEND_URL = os.environ.get('WIS2BOX_API_BACKEND_URL')

BROKER_USERNAME = os.environ.get('WIS2BOX_BROKER_USERNAME')
BROKER_PASSWORD = os.environ.get('WIS2BOX_BROKER_PASSWORD')
BROKER_HOST = os.environ.get('WIS2BOX_BROKER_HOST')
BROKER_PORT = os.environ.get('WIS2BOX_BROKER_PORT')
BROKER_PUBLIC = os.environ.get('WIS2BOX_BROKER_PUBLIC')

STORAGE_PUBLIC_URL = f"{WIS2BOX_URL}/data"
STORAGE_SOURCE = os.environ.get('WIS2BOX_STORAGE_SOURCE')
