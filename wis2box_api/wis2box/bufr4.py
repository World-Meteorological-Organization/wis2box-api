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


class ObservationDataBUFR():
    """Oservation data in bufr format"""

    def __init__(self, input_bytes: bytes) -> None:
        """
        ObservationDataBufr initializer

        :param input_data: `bytes` of input data

        :returns: `None`
        """

        self.input_bytes = input_bytes
        self.stations = Stations()
        self.output_items = []

    # return an array of output data
    def process_data(
        self
    ) -> list:
        """
        Transform input data to BUFR

        :returns: `list` of output data
        """

        LOGGER.info('Proccessing BUFR data')

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
                wsi_series = codes_get(subset, "wigosIdentifierSeries")
                wsi_issuer = codes_get(subset, "wigosIssuerOfIdentifier")
                wsi_issue_number = codes_get(subset, "wigosIssueNumber")
                wsi_local_identifier = codes_get(subset, "wigosLocalIdentifierCharacter")  # noqa
                temp_wsi = f"{wsi_series}-{wsi_issuer}-{wsi_issue_number}-{wsi_local_identifier}"  # noqa

            # now TSI
            if all(x in descriptors for x in (1001, 1002)):  # noqa we have block and station
                block_number = codes_get(subset, "blockNumber")
                station_number = codes_get(subset, "stationNumber")
                temp_tsi = f"{block_number:02d}{station_number:03d}"
            elif all(x in descriptors for x in (1011)):  # noqa we have ship callsign
                callsign = codes_get(subset,"shipOrMobileLandStationIdentifier")  # noqa
                temp_tsi = callsign
            elif all(x in descriptors for x in (1003, 1020, 1005)):  # noqa wmo region, sub area and buoy number
                region = codes_get(subset, "regionNumber")
                sub_area = codes_get(subset, "wmoRegionSubArea")
                buoy_number = codes_get(subset, "buoyOrPlatformIdentifier")
                temp_tsi = f"{region:01d}{sub_area:01d}{buoy_number:03d}"
            elif all(x in descriptors for x in (1010)):  # noqa we have moored buoy, CMAN or other fixed sea station
                callsign = codes_get(subset, "stationaryBuoyPlatformIdentifierEGCManBuoys")  # noqa
                temp_tsi = callsign
            elif all(x in descriptors for x in (1087)):  # noqa we have 7 digit buoy number
                buoy_number = codes_get(subset, "marineObservingPlatformIdentifier")  # noqa
                temp_tsi = f"{buoy_number:07d}"

        except Exception as err:
            LOGGER.warning(err)
            warnings.append(err)

        try:
            if any(x in descriptors for x in (6001, 6002)):
                longitude = codes_get(subset, "longitude")
            else:
                longitude = CODES_MISSING_DOUBLE
            if any(x in descriptors for x in (5001, 5002)):
                latitude = codes_get(subset, "latitude")
            else:
                latitude = CODES_MISSING_DOUBLE
            if 7030 in descriptors:
                elevation = codes_get(subset,"heightOfStationGroundAboveMeanSeaLevel")  # noqa
            else:
                elevation = CODES_MISSING_DOUBLE

            if CODES_MISSING_DOUBLE in (longitude, latitude, elevation):
                location = None
            else:
                location = {
                    "type": "Point",
                    "coordinates": [round(longitude, 5),
                                    round(latitude, 5),
                                    round(elevation)]
                }
            if location is None or None in location['coordinates']:
                msg = 'Missing location in BUFR'
                LOGGER.debug(msg)
                raise Exception(msg)
        except Exception as err:
            msg = f'Can not parse location from subset with wsi={temp_wsi}: {err}' # noqa
            LOGGER.info(msg)

        try:
            # the following should always be present
            yyyy = codes_get(subset, "year")
            mm = codes_get(subset, "month")
            dd = codes_get(subset, "day")
            # for daily data the following may be missing, default to 0
            if 4004 in descriptors:
                HH = codes_get(subset, "hour")
            else:
                HH = 0
            if 4005 in descriptors:
                MM = codes_get(subset, "minute")
            else:
                MM = 0
            data_date = f"{yyyy:04d}-{mm:02d}-{dd:02d}T{HH:02d}:{MM:02d}:00Z"
        except Exception:
            msg = f"Error parsing time from subset with wsi={temp_wsi}, skip this subset" # noqa
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
            msg = 'Station not in station list: '
            msg += f'wsi={temp_wsi} tsi={temp_tsi}; skipping'
            errors.append(msg)
            self.output_items.append({
                'errors': errors,
                'warnings': warnings
            })
            return

        try:
            LOGGER.debug('Copying wsi to BUFR')
            [series, issuer, number, tsi] = wsi.split('-')
            codes_set(subset_out, 'wigosIdentifierSeries', int(series))
            codes_set(subset_out, 'wigosIssuerOfIdentifier', int(issuer))
            codes_set(subset_out, 'wigosIssueNumber', int(number))
            codes_set(subset_out, 'wigosLocalIdentifierCharacter', tsi)
            codes_bufr_copy_data(subset, subset_out)

            if location is None or None in location['coordinates']:
                msg = 'Missing coordinates in BUFR, using coordinates from station metadata'  # noqa
                warnings.append(msg)
                location = self.stations.get_geometry(wsi)
                long, lat, elev = location.get('coordinates')
                codes_set(subset_out, 'longitude', long)
                codes_set(subset_out, 'latitude', lat)
                codes_set(subset_out, 'heightOfStationGroundAboveMeanSeaLevel', elev)  # noqa

            if '/' in data_date:
                data_date = data_date.split('/')[1]

            isodate = datetime.strptime(data_date, '%Y-%m-%dT%H:%M:%SZ')

            for (name, p) in zip(TIME_NAMES, TIME_PATTERNS):
                codes_set(subset_out, name, int(isodate.strftime(p)))

            isodate_str = isodate.strftime('%Y%m%dT%H%M%S')

            rmk = f"WIGOS_{wsi}_{isodate_str}"
            LOGGER.debug(f'Publishing with identifier: {rmk}')

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
