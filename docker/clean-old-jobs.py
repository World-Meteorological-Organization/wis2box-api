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

# a simple script to clean old jobs from the api

import logging
import os

from datetime import datetime

import requests

logging.basicConfig(level=logging.INFO)

LOGGER = logging.getLogger('clean-old-jobs')
JOB_RETENTION_MINUTES = int(os.environ.get('JOB_RETENTION_MINUTES', 60))
LOCAL_API_URL = os.environ.get('LOCAL_API_URL', 'http://localhost/oapi')


def clean_jobs():
    # get jobs
    url = f'{LOCAL_API_URL}/jobs'

    headers = {
        'Accept': 'application/json'
    }

    try:
        response = requests.get(url, headers=headers)
        result = response.json()
        if len(result['jobs']) == 0:
            LOGGER.info('No jobs to clean')
            return
        jobs = result['jobs']
    except Exception as err:
        LOGGER.error(f'Error getting jobs: {err}')
        return

    job_ids = []
    for job in jobs:
        if job.get('finished', None) is None:
            continue
        if job.get('status', 'unknown') != 'successful':
            continue
        endtime_str = job['finished']
        endtime = datetime.strptime(endtime_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        minutes_since_end = (datetime.utcnow() - endtime).total_seconds() / 60
        logging.debug(f'Job {job["jobID"]} ended {minutes_since_end} min. ago')
        if minutes_since_end > JOB_RETENTION_MINUTES:
            job_ids.append(job['jobID'])

    LOGGER.info(f'Found {len(job_ids)} jobs to clean')
    for id in job_ids:
        LOGGER.info(f'Cleaning job {id}')
        url = f'{LOCAL_API_URL}/jobs/{id}'
        requests.delete(url, headers=headers)


if __name__ == '__main__':
    clean_jobs()
