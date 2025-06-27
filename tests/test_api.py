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

import time

from pathlib import Path
import unittest

import requests

THISDIR = Path(__file__).resolve().parent


class Wis2BoxAPITest(unittest.TestCase):
    def setUp(self):
        """setup test fixtures, etc."""

        # TODO: abstract when wis2box-api is merged into wis2box
        self.endpoint = 'http://localhost:8999/oapi'
        self.admin_endpoint = f'{self.endpoint}/admin'
        self.headers = {
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }

    def tearDown(self):
        """return to pristine state"""

        pass

    def test_admin(self):

        url = f'{self.admin_endpoint}'
        content = requests.get(url).json()

        keys = ['logging', 'metadata', 'resources', 'server']
        self.assertEqual(sorted(content.keys()), keys)

    def test_resources_crud(self):

        url = f'{self.admin_endpoint}/resources'
        content = requests.get(url).json()

        self.assertEqual(len(content.keys()), 1)

        # POST a new resource
        with get_abspath('resource-post.json').open() as fh:
            post_data = fh.read()

        response = requests.post(url, headers=self.headers, data=post_data)
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.text,
                         'Location: /oapi/admin/resources/data2')

        # NOTE: we sleep 5 between CRUD requests so as to let gunicorn
        # restart with the refreshed configuration
        time.sleep(5)

        content = requests.get(url).json()
        self.assertEqual(len(content.keys()), 2)

        # PUT an existing resource
        url = f'{self.admin_endpoint}/resources/data2'
        with get_abspath('resource-put.json').open() as fh:
            post_data = fh.read()

        response = requests.put(url, headers=self.headers, data=post_data)
        self.assertEqual(response.status_code, 204)

        time.sleep(5)

        content = requests.get(url).json()
        self.assertEqual(content['title']['en'],
                         'Data assets, updated by HTTP PUT')

        # PATCH an existing resource
        url = f'{self.admin_endpoint}/resources/data2'
        with get_abspath('resource-patch.json').open() as fh:
            post_data = fh.read()

        response = requests.patch(url, headers=self.headers, data=post_data)
        self.assertEqual(response.status_code, 200)

        time.sleep(5)

        content = requests.get(url).json()
        self.assertEqual(content['title']['en'],
                         'Data assets, updated by HTTP PATCH')

        # DELETE an existing new resource
        response = requests.delete(url, headers=self.headers)
        self.assertEqual(response.status_code, 204)

        time.sleep(5)

        url = f'{self.admin_endpoint}/resources'
        content = requests.get(url).json()
        self.assertEqual(len(content.keys()), 1)


def get_abspath(filepath):
    """helper function absolute file access"""

    return Path(THISDIR) / filepath


if __name__ == '__main__':
    unittest.main()
