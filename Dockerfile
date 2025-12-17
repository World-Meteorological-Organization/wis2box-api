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
FROM ghcr.io/world-meteorological-organization/dim_eccodes_baseimage:noble_eccodes-2.44

ENV PYGEOAPI_CONFIG=/data/wis2box/config/pygeoapi/local.config.yml
ENV PYGEOAPI_OPENAPI=/data/wis2box/config/pygeoapi/local.openapi.yml

ENV CSV2BUFR_TEMPLATES=/data/wis2box/mappings

WORKDIR /root

RUN apt-get update -y && apt-get install cron curl python3-pip git unzip -y
# install gdal
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    python3-gdal libudunits2-dev dos2unix wget && \
    rm -rf /var/lib/apt/lists/*

RUN /venv/bin/python3 -m pip install --no-cache-dir gunicorn gevent

# install pygeoapi==0.22.0 from GitHub
RUN pip3 install --no-cache-dir https://github.com/geopython/pygeoapi/archive/refs/tags/0.22.0.zip

# install WMO software
RUN pip3 install --no-cache-dir \
    https://github.com/World-Meteorological-Organization/pywis-topics/archive/refs/tags/0.3.5.zip \
    https://github.com/World-Meteorological-Organization/pywcmp/archive/refs/tags/0.13.1.zip \
    https://github.com/wmo-cop/pyoscar/archive/refs/tags/0.9.0.zip

RUN pywcmp bundle sync

# get latest version of csv2bufr templates and install
RUN c2bt=$(git -c 'versionsort.suffix=-' ls-remote --tags --sort='v:refname' https://github.com/World-Meteorological-Organization/csv2bufr-templates.git | tail -1 | cut -d '/' -f 3 | sed 's/v//') && \
    mkdir /opt/csv2bufr && \
    cd /opt/csv2bufr && \
    wget https://github.com/World-Meteorological-Organization/csv2bufr-templates/archive/refs/tags/v${c2bt}.tar.gz && \
    tar -zxf v${c2bt}.tar.gz --strip-components=1 csv2bufr-templates-${c2bt}/templates

# install wis2box-api
COPY . /app
COPY wis2box_api/templates/admin /pygeoapi/pygeoapi/templates/admin
COPY ./docker/pygeoapi-config.yml $PYGEOAPI_CONFIG

RUN cd /app \
    && pip3 install -e . \
    && chmod +x /app/docker/es-entrypoint.sh /app/docker/wait-for-elasticsearch.sh

# Install Supercronic for job management
RUN curl -fsSLO "https://github.com/aptible/supercronic/releases/download/v0.2.39/supercronic-linux-amd64" && \
    chmod +x supercronic-linux-amd64 && \
    mv supercronic-linux-amd64 /usr/local/bin/supercronic

# create the wis2box-api user and give ownership of relevant folders
RUN useradd -m -d /wis2box-api -s /bin/bash wis2box-api && \
    chown -R wis2box-api:wis2box-api /data /wis2box-api /app

USER wis2box-api
WORKDIR /wis2box-api

ENTRYPOINT [ "/app/docker/es-entrypoint.sh" ]
#ENTRYPOINT [ "/bin/bash" ]
