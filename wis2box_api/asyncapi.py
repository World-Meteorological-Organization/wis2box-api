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
from typing import Any, Tuple, Union
from urllib.parse import urlparse


from pygeoapi import l10n
from pygeoapi.api import API, APIRequest, F_HTML, pre_process
from pygeoapi.openapi import load_openapi_document
from pygeoapi.util import to_json, render_j2_template

from wis2box_api import __version__


LOGGER = logging.getLogger(__name__)


class AsyncAPI(API):
    """AsyncAPI object"""

    PYGEOAPI_CONFIG = os.environ.get("PYGEOAPI_CONFIG")
    PYGEOAPI_OPENAPI = os.environ.get("PYGEOAPI_OPENAPI")

    def __init__(self, config):
        """
        constructor

        :param config: configuration dict

        :returns: `wis2box_api.AsyncAPI` instance
        """

        openapi = load_openapi_document()
        super().__init__(config, openapi)

    @pre_process
    def get_asyncapi(self, request: Union[APIRequest, Any]) -> Tuple[dict, int, str]:  # noqa
        """
        Provide AsyncAPI document

        :param request: request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        headers = request.get_response_headers()
        headers['Content-Type'] = 'application/json'

        content = to_json(generate_asyncapi(self.config, request.locale),
                          self.pretty_print)

        return headers, 200, content

        if request.format == F_HTML:
            content = render_j2_template(
                self.config, 'admin/index.html', self.config, request.locale
            )
        else:
            content = to_json(generate_asyncapi(), self.pretty_print)

        return headers, 200, content


def generate_asyncapi(config: dict, locale: str) -> dict:
    """
    Generate an AsyncAPI document

    :param config: `dict` of pygeoapi configuration
    :param language: A locale string (e.g. "en-US" or "en") or Babel Locale.

    :returns: `dict` of AsyncAPI document
    """

    LOGGER.debug('Generating AsyncAPI document')

    title = l10n.translate(config['metadata']['identification']['title'], locale)  # noqa
    description = l10n.translate(config['metadata']['identification']['description'], locale)  # noqa
    tags = l10n.translate(config['metadata']['identification']['keywords'], locale)  # noqa

    url = wis2box_mqtt_url = os.environ.get('WIS2BOX_BROKER_PUBLIC')
    u = urlparse(wis2box_mqtt_url)
    auth = f'{u.username}:{u.password}@'
    url = wis2box_mqtt_url.replace(auth, '')

    a = {
        'asyncapi': '2.6.0',
        'id': 'https://docs.wis2box.wis.wmo.int',
        'defaultContentType': 'application/json',
        'info': {
            'version': __version__,
            'title': title,
            'description': description,
            'license': {
                'name': config['metadata']['license']['name'],
                'url': config['metadata']['license']['url']
            },
            'contact': {
                'name': config['metadata']['contact']['name'],
                'url': config['metadata']['contact']['url'],
                'email': config['metadata']['contact']['email']
            }
        },
        'components': {
            'operationTraits': {
                'mqtt': {
                    'bindings': {
                        'mqtt': {
                            'qos': 1
                        }
                    }
                }
            }
        },
        'servers': {
            'production': {
                'url': url,
                'protocol': 'mqtt',
                'protocolVersion': '5.0',
                'description': description
            }
        },
        'channels': {
            'origin/a/wis2': {
                'description': 'Data notifications',
                'subscribe': {
                    'operationId': 'Notify',
                    'traits': [
                        {'$ref': '#/components/operationTraits/mqtt'}
                    ],
                    'message': {
                        '$ref': 'https://raw.githubusercontent.com/wmo-im/wis2-notification-message/main/schemas/notificationMessageGeoJSON.yaml'  # noqa
                    }
                }
            }
        },
        'tags': [{'name': tag} for tag in tags],
        'externalDocs': {
            'url': 'https://docs.wis2box.wis.wmo.int'
        }
    }

    return a
