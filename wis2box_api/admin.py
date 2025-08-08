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

from copy import deepcopy
import os
import json

import json_merge_patch
from jsonschema.exceptions import ValidationError
import logging
import shutil
import tempfile
from typing import Any, Tuple, Union
import yaml


from pygeoapi.api import API, APIRequest, F_HTML
from pygeoapi.config import validate_config
from pygeoapi.openapi import get_oas, load_openapi_document
from pygeoapi.util import to_json, render_j2_template


LOGGER = logging.getLogger(__name__)


def pre_process(func):
    """
    Decorator that transforms an incoming Request instance specific to the
    web framework (i.e. Flask, Starlette or Django) into a generic
    :class:`APIRequest` instance.

    :param func: decorated function

    :returns: `func`
    """

    def inner(*args):
        cls, req_in = args[:2]
        req_out = APIRequest.from_flask(req_in, getattr(cls, 'locales', set()))
        if len(args) > 2:
            return func(cls, req_out, *args[2:])
        else:
            return func(cls, req_out)

    return inner


class Admin(API):
    """Admin object"""

    PYGEOAPI_CONFIG = os.environ.get("PYGEOAPI_CONFIG")
    PYGEOAPI_OPENAPI = os.environ.get("PYGEOAPI_OPENAPI")

    def __init__(self, config):
        """
        constructor

        :param config: configuration dict

        :returns: `wis2box_api.Admin` instance
        """

        openapi = load_openapi_document()
        super().__init__(config, openapi)

    def validate(self, config):
        """
        Validate pygeoapi configuration and OpenAPI to file

        :param config: configuration dict
        """
        # validate pygeoapi configuration
        LOGGER.debug('Validating configuration')
        validate_config(config)
        # validate OpenAPI document
        LOGGER.debug('Validating openapi document')
        # oas = get_oas(config)
        # validate_openapi_document(oas)
        return True

    def write(self, config, action: str):
        """
        Write pygeoapi configuration and OpenAPI to file

        :param config: configuration dict
        :param action: HTTP operation
        """
        self.write_config(config, action)
        self.write_oas(config)

    def write_config(self, config, action: str):
        """
        Write pygeoapi configuration file

        :param config: configuration dict
        :param action: HTTP operation
        """

        # validate pygeoapi configuration
        validate_config(config)

        with open(self.PYGEOAPI_CONFIG, encoding='utf8') as fh:
            conf = yaml.safe_load(fh)

        if action != 'DELETE':
            config = json_merge_patch.merge(config, conf)

        # write pygeoapi configuration
        LOGGER.debug('Writing pygeoapi configutation')
        self.yaml_dump(config, self.PYGEOAPI_CONFIG)
        LOGGER.debug('Finished writing pygeoapiconfigutation')

    def write_oas(self, config):
        """
        Write pygeoapi OpenAPI document

        :param config: configuration dict
        """

        # validate OpenAPI document
        oas = get_oas(config)
        # validate_openapi_document(oas)

        # write OpenAPI document
        LOGGER.debug('Writing OpenAPI document')
        self.yaml_dump(oas, self.PYGEOAPI_OPENAPI)
        LOGGER.debug('Finished writing OpenAPI document')

    def yaml_dump(self, dict_, destfile) -> bool:
        """
        Dump dict to YAML file

        :param dict_: `dict` to dump
        :param destfile: destination filepath

        :returns: `bool`
        """

        temp_filename = None

        LOGGER.debug('Dumping YAML document')
        with tempfile.NamedTemporaryFile(delete=False) as fh:
            temp_filename = fh.name
            yaml.safe_dump(dict_, fh, sort_keys=False, encoding='utf8',
                           default_flow_style=False)

        LOGGER.debug(f'Moving {temp_filename} to {destfile}')
        shutil.move(temp_filename, destfile)

        return True

    @pre_process
    def admin(self, request: Union[APIRequest, Any]) -> Tuple[dict, int, str]:
        """
        Provide admin document

        :param request: request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        headers = request.get_response_headers()

        if request.format == F_HTML:
            content = render_j2_template(
                self.config, 'admin/index.html', self.config, request.locale
            )
        else:
            content = to_json(self.config, self.pretty_print)

        return headers, 200, content

    @pre_process
    def resources(
        self, request: Union[APIRequest, Any]
    ) -> Tuple[dict, int, str]:
        """
        Provide admin document

        :param request: request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        headers = request.get_response_headers()

        if request.format == F_HTML:
            content = render_j2_template(
                self.config,
                'admin/index.html',
                self.config['resources'],
                request.locale,
            )
        else:
            content = to_json(self.config['resources'], self.pretty_print)

        return headers, 200, content

    @pre_process
    def post_resource(
        self, request: Union[APIRequest, Any]
    ) -> Tuple[dict, int, str]:
        """
        Add resource configuration

        :param request: request object

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        config = deepcopy(self.config)
        headers = request.get_response_headers()

        data = request.data
        if not data:
            msg = 'missing request data'
            return self.get_exception(
                400, headers, request.format, 'MissingParameterValue', msg
            )

        try:
            # Parse data
            data = data.decode()
        except (UnicodeDecodeError, AttributeError):
            pass

        try:
            data = json.loads(data)
        except (json.decoder.JSONDecodeError, TypeError) as err:
            # Input is not valid JSON
            LOGGER.error(err)
            msg = 'invalid request data'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg
            )

        resource_id = next(iter(data.keys()))

        if config['resources'].get(resource_id) is not None:
            # Resource already exists
            msg = f'Resource exists: {resource_id}'
            LOGGER.error(msg)
            return self.get_exception(
                400, headers, request.format, 'NoApplicableCode', msg
            )

        LOGGER.debug(f'Adding resource: {resource_id}')
        config['resources'].update(data)

        try:
            self.validate(config)
        except ValidationError as err:
            LOGGER.error(err)
            msg = 'Schema validation error'
            return self.get_exception(
                400, headers, request.format, 'ValidationError', msg
            )

        self.write(config, action='POST')

        content = f'Location: /{request.path_info}/{resource_id}'
        LOGGER.debug(f'Success at {content}')

        return headers, 201, content

    @pre_process
    def get_resource(
        self, request: Union[APIRequest, Any], resource_id: str
    ) -> Tuple[dict, int, str]:
        """
        Get resource configuration

        :param request: request object
        :param resource_id:

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        headers = request.get_response_headers()

        try:
            resource = self.config['resources'][resource_id]
        except KeyError:
            msg = f'Resource not found: {resource_id}'
            return self.get_exception(
                400, headers, request.format, 'ResourceNotFound', msg
            )

        if request.format == F_HTML:
            content = render_j2_template(
                self.config, 'admin/index.html', resource, request.locale
            )
        else:
            content = to_json(resource, self.pretty_print)

        return headers, 200, content

    @pre_process
    def delete_resource(
        self, request: Union[APIRequest, Any], resource_id: str
    ) -> Tuple[dict, int, str]:
        """
        Delete resource configuration

        :param request: request object
        :param resource_id: resource identifier

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        config = deepcopy(self.config)
        headers = request.get_response_headers()

        try:
            LOGGER.debug(f'Removing resource configuration for: {resource_id}')
            config['resources'].pop(resource_id)
        except KeyError:
            msg = f'Resource not found: {resource_id}'
            return self.get_exception(
                400, headers, request.format, 'ResourceNotFound', msg
            )

        LOGGER.debug('Resource removed, validating and saving configuration')
        try:
            self.validate(config)
        except ValidationError as err:
            LOGGER.error(err)
            msg = 'Schema validation error'
            return self.get_exception(
                400, headers, request.format, 'ValidationError', msg
            )

        self.write(config, action='DELETE')

        return headers, 204, {}

    @pre_process
    def put_resource(
        self,
        request: Union[APIRequest, Any],
        resource_id: str,
    ) -> Tuple[dict, int, str]:
        """
        Update complete resource configuration

        :param request: request object
        :param resource_id: resource identifier

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        config = deepcopy(self.config)
        headers = request.get_response_headers()

        try:
            LOGGER.debug('Verifying resource exists')
            config['resources'][resource_id]
        except KeyError:
            msg = f'Resource not found: {resource_id}'
            return self.get_exception(
                400, headers, request.format, 'ResourceNotFound', msg
            )

        data = request.data
        if not data:
            msg = 'missing request data'
            return self.get_exception(
                400, headers, request.format, 'MissingParameterValue', msg
            )

        try:
            # Parse data
            data = data.decode()
        except (UnicodeDecodeError, AttributeError):
            pass

        try:
            data = json.loads(data)
        except (json.decoder.JSONDecodeError, TypeError) as err:
            # Input is not valid JSON
            LOGGER.error(err)
            msg = 'invalid request data'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg
            )

        LOGGER.debug('Updating resource')
        config['resources'].update({resource_id: data})

        try:
            self.validate(config)
        except ValidationError as err:
            LOGGER.error(err)
            msg = 'Schema validation error'
            return self.get_exception(
                400, headers, request.format, 'ValidationError', msg
            )

        self.write(config, action='PUT')

        return headers, 204, {}

    @pre_process
    def patch_resource(
        self, request: Union[APIRequest, Any], resource_id: str
    ) -> Tuple[dict, int, str]:
        """
        Update partial resource configuration

        :param request: request object
        :param resource_id: resource identifier

        :returns: tuple of headers, status code, content
        """

        if not request.is_valid():
            return self.get_format_exception(request)

        config = deepcopy(self.config)
        headers = request.get_response_headers()

        try:
            LOGGER.debug('Verifying resource exists')
            resource = config['resources'][resource_id]
        except KeyError:
            msg = f'Resource not found: {resource_id}'
            return self.get_exception(
                400, headers, request.format, 'ResourceNotFound', msg
            )

        data = request.data
        if not data:
            msg = 'missing request data'
            return self.get_exception(
                400, headers, request.format, 'MissingParameterValue', msg
            )

        try:
            # Parse data
            data = data.decode()
        except (UnicodeDecodeError, AttributeError):
            pass

        try:
            data = json.loads(data)
        except (json.decoder.JSONDecodeError, TypeError) as err:
            # Input is not valid JSON
            LOGGER.error(err)
            msg = 'invalid request data'
            return self.get_exception(
                400, headers, request.format, 'InvalidParameterValue', msg
            )

        LOGGER.debug('Merging resource block')
        data = json_merge_patch.merge(resource, data)
        LOGGER.debug('Updating resource')
        config['resources'].update({resource_id: data})

        try:
            self.validate(config)
        except ValidationError as err:
            LOGGER.error(err)
            msg = 'Schema validation error'
            return self.get_exception(
                400, headers, request.format, 'ValidationError', msg
            )

        self.write(config, action='PATCH')

        content = to_json(resource, self.pretty_print)

        return headers, 200, content
