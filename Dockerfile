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
FROM ghcr.io/World-Meteorological-Organization/dim_eccodes_baseimage:latest

ENV PYGEOAPI_CONFIG=/data/wis2box/config/pygeoapi/local.config.yml
ENV PYGEOAPI_OPENAPI=/data/wis2box/config/pygeoapi/local.openapi.yml

ENV CSV2BUFR_TEMPLATES=/data/wis2box/mappings

WORKDIR /root

RUN apt-get update -y && apt-get install cron curl python3-pip git unzip -y
# install gunicorn, gevent, gdal, elasticsearch
RUN apt-get install -y --no-install-recommends \
    libgdal-dev gunicorn python3-gevent python3-gdal python3-elasticsearch libudunits2-dev dos2unix wget \
    && rm -rf /var/lib/apt/lists/*

# install pygeoapi==0.18.0 from GitHub
RUN pip3 install --no-cache-dir https://github.com/geopython/pygeoapi/archive/refs/tags/0.18.0.zip

# install WMO software
RUN pip3 install --no-cache-dir \
    https://github.com/World-Meteorological-Organization/pywis-topics/archive/refs/tags/0.3.2.zip \
    https://github.com/World-Meteorological-Organization/pywcmp/archive/refs/tags/0.8.5.zip \
    https://github.com/wmo-cop/pyoscar/archive/refs/tags/0.9.0.zip

RUN pywcmp bundle sync

RUN mkdir -p /data && \
    cd /data && \
    curl -f -L -o /data/wmo-ra.geojson https://raw.githubusercontent.com/OGCMetOceanDWG/wmo-ra/master/wmo-ra.geojson

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

#COPY ./docker/pygeoapi-openapi.yml $PYGEOAPI_OPENAPI

RUN cd /app \
    && pip3 install -e . \
    && chmod +x /app/docker/es-entrypoint.sh /app/docker/wait-for-elasticsearch.sh

# add wis2box.cron to crontab
COPY ./docker/wis2box-api.cron /etc/cron.d/wis2box-api.cron

RUN chmod 0644 /etc/cron.d/wis2box-api.cron && crontab /etc/cron.d/wis2box-api.cron

ENTRYPOINT [ "/app/docker/es-entrypoint.sh" ]
#ENTRYPOINT [ "/bin/bash" ]
