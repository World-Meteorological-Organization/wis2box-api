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

import logging
import tempfile

from datetime import datetime

from eccodes import (
    codes_bufr_copy_data,
    codes_bufr_new_from_samples,
    codes_bufr_new_from_file,
    codes_get_message,
    codes_clone,
    codes_set,
    codes_set_array,
    codes_release,
    codes_get,
    codes_get_array,
    CODES_MISSING_DOUBLE
)

from wis2box_api.wis2box.station import Stations

LOGGER = logging.getLogger(__name__)
TEMPLATE = codes_bufr_new_from_samples("BUFR4")
TIME_PATTERNS = ['%Y', '%m', '%d', '%H', '%M', '%S']
TIME_NAMES = [
    'typicalYear',
    'typicalMonth',
    'typicalDay',
    'typicalHour',
    'typicalMinute',
    'typicalSecond'
]

HEADERS = ["edition", "masterTableNumber", "bufrHeaderCentre",
           "bufrHeaderSubCentre", "updateSequenceNumber", "dataCategory",
           "internationalDataSubCategory", "dataSubCategory",
           "masterTablesVersionNumber", "localTablesVersionNumber",
           "typicalYear", "typicalMonth", "typicalDay", "typicalHour",
           "typicalMinute", "typicalSecond",
           "numberOfSubsets", "observedData", "compressedData"]


def get_bufr_overrides(metadata_id: str):
    """
    BufrOverrides initializer

    :param metadata_id: `str` of metadata identifier

    :returns: `dict`, of bufr overrides
    """
    import requests
    url = f'http://localhost/oapi/collections/discovery-metadata/items/{metadata_id}' # noqa
    overrides_stations = {}
    overrides_all = {}
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        item = resp.json()
        wis2box_config = item.get('wis2box', {})
        for key, value in wis2box_config.get('bufr_overrides', {}).items():
            if key != 'all' and len(key.split('-')) == 4:
                overrides_stations[key] = value
            elif key == 'all':
                overrides_all = value
            else:
                LOGGER.warning(f'Invalid bufr_overrides key={key}, must be "all" or a WSI') # noqa
    except Exception as err:
        LOGGER.error(f'Error fetching bufr_overrides, url={url}: {err}')
    return {'stations': overrides_stations, 'all': overrides_all}


class ObservationDataBUFR():
    """Observation data in bufr format"""

    def __init__(self, input_bytes: bytes,
                 channel: str = None,
                 overrides: dict = None):
        """
        ObservationDataBufr initializer

        :param input_data: `bytes` of input data

        :returns: `None`
        """

        self.input_bytes = input_bytes
        self.stations = Stations(channel)
        self.output_items = []
        self.overrides = {
            'stations': overrides.get('stations', {}) if overrides else {},
            'all': overrides.get('all', {}) if overrides else {}
        }

    # return an array of output data
    def process_data(
        self
    ) -> list:
        """
        Transform input data to BUFR

        :returns: `list` of output data
        """

        LOGGER.debug('Proccessing BUFR data')

        # FIXME: figure out how to pass a bytestring to ecCodes BUFR reader
        tmp = tempfile.NamedTemporaryFile()
        with open(tmp.name, 'wb') as f:
            f.write(self.input_bytes)

        # workflow
        # check for multiple messages
        # split messages and process
        data = {}
        with open(tmp.name, 'rb') as fh:
            while data is not None:
                data = codes_bufr_new_from_file(fh)
                if data is not None:
                    try:
                        self.transform_message(data)
                    except Exception as err:
                        msg = f'Error in transform_message: {err}'
                        LOGGER.error(msg)
                        self.output_items.append({
                            'errors': [msg],
                            'warnings': []
                        })
                    codes_release(data)
        return self.output_items

    def transform_message(self, bufr_in: int) -> None:
        """
        Parse single BUFR message
        :param bufr_in: `int` of ecCodes pointer to BUFR message
        :returns: `None`
        """
        # workflow
        # check for multiple subsets
        # add necessary components for WSI in BUFR
        # split subsets into individual messages and process
        try:
            codes_set(bufr_in, 'unpack', True)
        except Exception as err:
            msg = f'Error unpacking message: {err}'
            LOGGER.error(msg)
            self.output_items.append({
                'errors': [msg],
                'warnings': []
            })
            return

        # get descriptors present in the file
        descriptors = codes_get_array(bufr_in, "expandedDescriptors").tolist()

        # prepare the headers for the new messages
        headers = {}
        for header in HEADERS:
            headers[header] = codes_get(bufr_in, header)
        # original to be split by subset, so set the number of subsets to 1
        headers['numberOfSubsets'] = 1
        # set the master table version number
        table_version = max(
            28, codes_get(bufr_in, 'masterTablesVersionNumber')
        )
        headers['masterTablesVersionNumber'] = table_version
        # set the unexpanded descriptors
        out_ue = codes_get_array(bufr_in, 'unexpandedDescriptors').tolist()
        if 301150 not in out_ue:
            out_ue.insert(0, 301150)
        headers['unexpandedDescriptors'] = out_ue

        # loop over the subsets, create a new message for each
        num_subsets = codes_get(bufr_in, 'numberOfSubsets')
        LOGGER.debug(f'Found {num_subsets} subsets')

        for i in range(num_subsets):
            idx = i + 1
            LOGGER.debug(f'Processing subset {idx}')
            LOGGER.debug('Extracting subset')
            codes_set(bufr_in, 'extractSubset', idx)
            codes_set(bufr_in, 'doExtractSubsets', 1)
            # copy the replication factors
            if 31000 in descriptors:
                try:
                    short_replication_factors = codes_get_array(bufr_in, "shortDelayedDescriptorReplicationFactor").tolist()  # noqa
                except Exception as e:
                    short_replication_factors = []
                    LOGGER.error(e.__class__.__name__)
            if 31001 in descriptors:
                try:
                    replication_factors = codes_get_array(bufr_in, "delayedDescriptorReplicationFactor").tolist()  # noqa
                except Exception as e:
                    replication_factors = []
                    LOGGER.error(e.__class__.__name__)
            if 31002 in descriptors:
                try:
                    extended_replication_factors = codes_get_array(bufr_in, "extendedDelayedDescriptorReplicationFactor").tolist()  # noqa
                except Exception as e:
                    extended_replication_factors = []
                    LOGGER.error(e.__class__.__name__)

            LOGGER.debug('Copying template BUFR')
            subset_out = codes_clone(TEMPLATE)

            # set the replication factors, this needs to be done before
            # setting the unexpanded descriptors
            if (31000 in descriptors) and (len(short_replication_factors) > 0):  # noqa
                codes_set_array(subset_out, "inputShortDelayedDescriptorReplicationFactor", short_replication_factors)  # noqa
            if (31001 in descriptors) and (len(replication_factors) > 0):
                codes_set_array(subset_out, "inputDelayedDescriptorReplicationFactor", replication_factors)  # noqa
            if (31002 in descriptors) and (len(extended_replication_factors) > 0):  # noqa
                codes_set_array(subset_out, "inputExtendedDelayedDescriptorReplicationFactor", extended_replication_factors)  # noqa

            # we need to copy all the headers, not just the
            # unexpandedDescriptors and MT number

            for k, v in headers.items():
                if isinstance(v, list):
                    codes_set_array(subset_out, k, v)
                else:
                    codes_set(subset_out, k, v)

            LOGGER.debug('Cloning subset to new message')
            subset = codes_clone(bufr_in)
            self.transform_subset(subset, subset_out)
            codes_release(subset)
            codes_release(subset_out)

    def transform_subset(self, subset: int, subset_out: int) -> None:
        """
        Parse single BUFR message subset
        :param subset: `int` of ecCodes pointer to input BUFR
        :param subset_out: `int` of ecCodes pointer to output BUFR
        :returns: `None`
        """
        # workflow
        # - check for WSI,
        #   - if None, lookup using tsi
        # - check for location,
        #   - if None, use geometry from station report
        # - check for time,
        #   - if temporal extent, use end time
        #   - set times in header
        # - write a separate BUFR message for each subset
        # keep track of errors and warnings
        warnings = []
        errors = []

        # get expanded sequence
        descriptors = codes_get_array(subset, "expandedDescriptors")

        # unpack
        codes_set(subset, "unpack", True)

        temp_wsi = None
        temp_tsi = None
        try:
            # get WSI
            if 1125 in descriptors:
                wsi_series = codes_get(subset, "#1#wigosIdentifierSeries")
                wsi_issuer = codes_get(subset, "#1#wigosIssuerOfIdentifier")
                wsi_issue_number = codes_get(subset, "#1#wigosIssueNumber")
                wsi_local_identifier = codes_get(subset, "#1#wigosLocalIdentifierCharacter")  # noqa
                temp_wsi = f"{wsi_series}-{wsi_issuer}-{wsi_issue_number}-{wsi_local_identifier}"  # noqa

            # now TSI
            if all(x in descriptors for x in (1001, 1002)):  # noqa we have block and station
                block_number = codes_get(subset, "#1#blockNumber")
                station_number = codes_get(subset, "#1#stationNumber")
                temp_tsi = f"{block_number:02d}{station_number:03d}"
            elif all(x in descriptors for x in (1011,)):  # noqa we have ship callsign
                callsign = codes_get(subset,"#1#shipOrMobileLandStationIdentifier")  # noqa
                temp_tsi = callsign
            elif all(x in descriptors for x in (1003, 1020, 1005)):  # noqa wmo region, sub area and buoy number
                region = codes_get(subset, "#1#regionNumber")
                sub_area = codes_get(subset, "#1#wmoRegionSubArea")
                buoy_number = codes_get(subset, "#1#buoyOrPlatformIdentifier")
                temp_tsi = f"{region:01d}{sub_area:01d}{buoy_number:03d}"
            elif all(x in descriptors for x in (1010,)):  # noqa we have moored buoy, CMAN or other fixed sea station
                callsign = codes_get(subset, "#1#stationaryBuoyPlatformIdentifierEGCManBuoys")  # noqa
                temp_tsi = callsign
            elif all(x in descriptors for x in (1087,)):  # noqa we have 7 digit buoy number
                buoy_number = codes_get(subset, "#1#marineObservingPlatformIdentifier")  # noqa
                temp_tsi = f"{buoy_number:07d}"

        except Exception as err:
            LOGGER.warning(err)
            warnings.append(err)

        try:
            # the following should always be present
            yyyy = codes_get(subset, "#1#year")
            mm = codes_get(subset, "#1#month")
            dd = codes_get(subset, "#1#day")
            # for daily data the following may be missing, default to 0
            if 4004 in descriptors:
                HH = codes_get(subset, "#1#hour")
            else:
                HH = 0
            if 4005 in descriptors:
                MM = codes_get(subset, "#1#minute")
            else:
                MM = 0
            data_date = f"{yyyy:04d}-{mm:02d}-{dd:02d}T{HH:02d}:{MM:02d}:00Z"
        except Exception:
            msg = f"Error parsing time from subset with wsi={temp_wsi} (tsi={temp_tsi}), skip this subset" # noqa
            errors.append(msg)
            self.output_items.append({
                'errors': errors,
                'warnings': warnings
            })
            return

        # now repack
        codes_set(subset, "pack", True)

        LOGGER.debug(f'Processing temp_wsi: {temp_wsi}, temp_tsi: {temp_tsi}')
        wsi = self.stations.get_valid_wsi(wsi=temp_wsi, tsi=temp_tsi)
        if wsi is None:
            msg = f'Station {temp_wsi} (tsi={temp_tsi}) not in station list: '  # noqa
            errors.append(msg)
            self.output_items.append({
                'errors': errors,
                'warnings': warnings
            })
            return

        try:
            LOGGER.debug('Copying wsi to BUFR')
            [series, issuer, number, tsi] = wsi.split('-')
            codes_set(subset_out, '#1#wigosIdentifierSeries', int(series))
            codes_set(subset_out, '#1#wigosIssuerOfIdentifier', int(issuer))
            codes_set(subset_out, '#1#wigosIssueNumber', int(number))
            codes_set(subset_out, '#1#wigosLocalIdentifierCharacter', tsi)
            codes_bufr_copy_data(subset, subset_out)

            if '/' in data_date:
                data_date = data_date.split('/')[1]

            isodate = datetime.strptime(data_date, '%Y-%m-%dT%H:%M:%SZ')

            for (name, p) in zip(TIME_NAMES, TIME_PATTERNS):
                codes_set(subset_out, name, int(isodate.strftime(p)))

            # add bufr_overrides
            for key, value in self.overrides['stations'].get(wsi, {}).items():
                try:
                    if key == '#1#heightOfStationGroundAboveMeanSeaLevel':
                        LOGGER.warning(f'Overriding key={key}')
                    LOGGER.debug(f'Applying station override {key}={value} for wsi={wsi}')  # noqa
                    codes_set(subset_out, key, value)
                except Exception as err:
                    msg = f'Error applying station override {key}:{value} for wsi={wsi}: {err}'  # noqa
                    errors.append(msg)
            for key, value in self.overrides['all'].items():
                try:
                    LOGGER.debug(f'Applying any override {key}={value} for wsi={wsi}')  # noqa
                    codes_set(subset_out, key, value)
                except Exception as err:
                    msg = f'Error applying global override {key}:{value} for wsi={wsi}: {err}'  # noqa
                    errors.append(msg)

            isodate_str = isodate.strftime('%Y%m%dT%H%M%S')

            rmk = f"WIGOS_{wsi}_{isodate_str}"
            LOGGER.info(f'Publishing with identifier: {rmk}')

            # set location AFTER overrides have been applied
            try:
                if any(x in descriptors for x in (6001, 6002)):
                    longitude = codes_get(subset_out, "#1#longitude")
                else:
                    longitude = CODES_MISSING_DOUBLE
                if any(x in descriptors for x in (5001, 5002)):
                    latitude = codes_get(subset_out, "#1#latitude")
                else:
                    latitude = CODES_MISSING_DOUBLE
                if 7030 in descriptors:
                    elevation = codes_get(subset_out,"#1#heightOfStationGroundAboveMeanSeaLevel")  # noqa
                else:
                    elevation = CODES_MISSING_DOUBLE

                if CODES_MISSING_DOUBLE in (longitude, latitude):
                    location = None
                else:
                    location = {
                        "type": "Point",
                        "coordinates": [round(longitude, 5),
                                        round(latitude, 5)]
                    }
                    if elevation != CODES_MISSING_DOUBLE:
                        location['coordinates'].append(round(elevation))
                if location is None or None in location['coordinates']:
                    msg = f'Missing coordinates in BUFR for wsi={wsi} (tsi={temp_tsi}, using coordinates from station metadata'  # noqa
                    warnings.append(msg)
                    location = self.stations.get_geometry(wsi)
                    long, lat, elev = location.get('coordinates')
                    codes_set(subset_out, '#1#longitude', long)
                    codes_set(subset_out, '#1#latitude', lat)
                    codes_set(subset_out, '#1#heightOfStationGroundAboveMeanSeaLevel', elev)  # noqa
            except Exception as err:
                msg = f'Can not parse location from subset with wsi={wsi} (tsi={temp_tsi}): {err}' # noqa
                LOGGER.info(msg)

            LOGGER.debug('Writing bufr4')
            try:
                bufr4 = codes_get_message(subset_out)
            except Exception as err:
                errors.append(err)
                self.output_items.append({
                    'errors': errors,
                    'warnings': warnings
                })
            self.output_items.append({
                'bufr4': bufr4,
                '_meta': {
                    'id': rmk,
                    'properties': {
                        'wigos_station_identifier': wsi,
                        'datetime': isodate,
                        'geometry': location
                    }
                },
                'errors': errors,
                'warnings': warnings
            })
        except Exception as err:
            msg = f'Error processing subset: {err}'
            errors.append(msg)
            self.output_items.append({
                'errors': errors,
                'warnings': warnings
            })
