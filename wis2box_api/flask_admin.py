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

from flask import Blueprint, make_response, request

from pygeoapi.util import yaml_load

from wis2box_api.admin import Admin

LOGGER = logging.getLogger(__name__)

CONFIG = None

if 'PYGEOAPI_CONFIG' not in os.environ:
    raise RuntimeError('PYGEOAPI_CONFIG environment variable not set')

with open(os.environ.get('PYGEOAPI_CONFIG'), encoding='utf8') as fh:
    CONFIG = yaml_load(fh)

admin_ = Admin(CONFIG)
ADMIN_BLUEPRINT = Blueprint(
    'admin',
    __name__,
    template_folder='templates',
    static_folder='/static'
)


def get_response(result: tuple):
    """
    Creates a Flask Response object and updates matching headers.

    :param result: The result of the API call.
                   This should be a tuple of (headers, status, content).

    :returns: A Response instance
    """

    headers, status, content = result
    response = make_response(content, status)

    if headers:
        response.headers = headers
    return response


@ADMIN_BLUEPRINT.route('/admin')
def admin():
    """
    Admin landing page endpoint

    :returns: HTTP response
    """

    return get_response(admin_.admin(request))


@ADMIN_BLUEPRINT.route('/admin/resources', methods=['GET', 'POST'])
def resources():
    """
    Resource landing page endpoint

    :returns: HTTP response
    """

    if request.method == 'GET':
        return get_response(admin_.resources(request))

    elif request.method == 'POST':
        return get_response(admin_.post_resource(request))


@ADMIN_BLUEPRINT.route(
    '/admin/resources/<resource_id>', methods=['GET', 'PUT', 'PATCH', 'DELETE']
)
def resource(resource_id):
    """
    Resource landing page endpoint

    :returns: HTTP response
    """

    if request.method == 'GET':
        return get_response(admin_.get_resource(request, resource_id))

    elif request.method == 'DELETE':
        return get_response(admin_.delete_resource(request, resource_id))

    elif request.method == 'PUT':
        return get_response(admin_.put_resource(request, resource_id))

    elif request.method == 'PATCH':
        return get_response(admin_.patch_resource(request, resource_id))
