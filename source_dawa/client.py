# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import json
import pkgutil
from datetime import datetime
from typing import Dict


from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    SyncMode,
    Type,
)
from airbyte_cdk.logger import AirbyteLogger
from dawa import API


class Client:
    _POSTNUMMER = "postnummer"
    _ADRESSE = "adresse"
    _VEJSTYKKE = "vejstykke"
    _ADGANGSADRESSE = "adgangsadresse"
    _VEJPUNKT = "vejpunkt"
    _NAVNGIVENVEJ = "navngivenvej"
    _AFSTEMNINGSOMRODE = "afstemningsområde"
    _AFSTEMNINGSOMRODETILKNYTNING = "afstemningsområdetilknytning"
    _BROFASTHED = "brofasthed"
    _BYGNING = "bygning"
    _BYGNINGTILKNYTNING = "bygningtilknytning"
    _DAGI_POSTNUMMER = "dagi_postnummer"
    _EJERLAV = "ejerlav"
    _HØJDE = "højde"
    _IKKE_BROFAST_HUSNUMMER = "ikke_brofast_husnummer"
    _JORDSTYKKE = "jordstykke"
    _JORDSTYKKETILKNYTNING = "jordstykketilknytning"
    _KOMMUNE = "kommune"
    _KOMMUNETILKNYTNING = "kommunetilknytning"
    _LANDPOSTNUMMER = "landpostnummer"
    _MENIGHEDSRADSAFSTEMNINGSOMRADE = "menighedsrådsafstemningsområde"
    _MENIGHEDSRADSAFSTEMNINGSOMRADETILKNYTNING = "menighedsrådsafstemningsområdetilknytning"
    _OPSTILLINGSKREDS = "opstillingskreds"
    _OPSTILLINGSKREDSTILKNYTNING = "opstillingskredstilknytning"
    _POLITIKREDS = "politikreds"
    _POLITIKREDSTILKNYTNING = "politikredstilknytning"
    _REGION = "region"
    _REGIONSTILKNYTNING = "regionstilknytning"
    _RETSKREDS = "retskreds"
    _RETSKREDSTILKNYTNING = "retskredstilknytning"
    _SOGN = "sogn"
    _SOGNETILKNYTNING = "sognetilknytning"
    _STED = "sted"
    _STEDNAVN = "stednavn"
    _STEDNAVNTILKNYTNING = "stednavntilknytning"
    _STEDTILKNYTNING = "stedtilknytning"
    _STORKREDS = "storkreds"
    _STORKREDSTILKNYTNING = "storkredstilknytning"
    _SUPPLERENDEBYNAVN = "supplerendebynavn"
    _SUPPLERENDEBYNAVNTILKNYTNING = "supplerendebynavntilknytning"
    _VALGLANDSDEL = "valglandsdel"
    _VALGLANDSDELSTILKNYTNING = "valglandsdelstilknytning"
    _VEJMIDTE = "vejmidte"
    _VEJSTYKKEPOSTNUMMERRELATION = "vejstykkepostnummerrelation"
    _ZONE = "zone"
    _ZONETILKNYTNING = "zonetilknytning"

    STREAMS = [
        _POSTNUMMER,
        _ADRESSE,
        _VEJSTYKKE,
        _ADGANGSADRESSE,
        _VEJPUNKT,
        _NAVNGIVENVEJ,
        _AFSTEMNINGSOMRODE,
        _AFSTEMNINGSOMRODETILKNYTNING,
        _BROFASTHED,
        _BYGNING,
        _BYGNINGTILKNYTNING,
        _DAGI_POSTNUMMER,
        _EJERLAV,
        _HØJDE,
        _IKKE_BROFAST_HUSNUMMER,
        _JORDSTYKKE,
        _JORDSTYKKETILKNYTNING,
        _KOMMUNE,
        _KOMMUNETILKNYTNING,
        _LANDPOSTNUMMER,
        _MENIGHEDSRADSAFSTEMNINGSOMRADE,
        _MENIGHEDSRADSAFSTEMNINGSOMRADETILKNYTNING,
        _OPSTILLINGSKREDS,
        _OPSTILLINGSKREDSTILKNYTNING,
        _POLITIKREDS,
        _POLITIKREDSTILKNYTNING,
        _REGION,
        _REGIONSTILKNYTNING,
        _RETSKREDS,
        _RETSKREDSTILKNYTNING,
        _SOGN,
        _SOGNETILKNYTNING,
        _STED,
        _STEDNAVN,
        _STEDNAVNTILKNYTNING,
        _STEDTILKNYTNING,
        _STORKREDS,
        _STORKREDSTILKNYTNING,
        _SUPPLERENDEBYNAVN,
        _SUPPLERENDEBYNAVNTILKNYTNING,
        _VALGLANDSDEL,
        _VALGLANDSDELSTILKNYTNING,
        _VEJMIDTE,
        _VEJSTYKKEPOSTNUMMERRELATION,
        _ZONE,
        _ZONETILKNYTNING,
    ]

    CDC_LSN = "_ab_cdc_lsn"
    CDC_UPDATED_AT = "_ab_cdc_updated_at"
    CDC_DELETED_AT = "_ab_cdc_deleted_at"

    def __init__(self):
        self._client = API()

    def health_check(self):
        try:
            txid = self._client.txid()
            if txid is None:
                raise ValueError("Could not connect to Dawa")
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=str(e))

    def get_streams(self):
        streams = []
        for stream in self.STREAMS:
            raw_schema = json.loads(pkgutil.get_data(self.__class__.__module__.split(".")[0], f"schemas/{stream}.json"))

            stream = AirbyteStream.parse_obj(raw_schema)

            json_schema = stream.json_schema
            properties = json_schema["properties"]

            properties[self.CDC_LSN] = {"type": "number"}
            properties[self.CDC_UPDATED_AT] = {"type": "number"}
            properties[self.CDC_DELETED_AT] = {"type": "number"}

            streams.append(stream)

        return streams

    def _format_columns(self, record) -> Dict[str, any]:

        # CDC columns
        record[self.CDC_LSN] = None
        record[self.CDC_UPDATED_AT] = None
        record[self.CDC_DELETED_AT] = None

        # CDC row
        if "operation" in record:
            ab_cdc_datetime = datetime.strptime(record["tidspunkt"], "%Y-%m-%dT%H:%M:%S.%fZ")
            ab_cdc_datetime_timestamp = ab_cdc_datetime.timestamp()

            record[self.CDC_LSN] = record["txid"]
            record[self.CDC_UPDATED_AT] = ab_cdc_datetime_timestamp
            if record["operation"] == "delete":
                record[self.CDC_DELETED_AT] = ab_cdc_datetime_timestamp

        return record

    def get_records(self, catalog: ConfiguredAirbyteCatalog, logger: AirbyteLogger, state: Dict[str, any]):
        cursor_field = self.CDC_LSN
        txid = self._client.txid()

        for configured_stream in catalog.streams:
            stream = configured_stream.stream

            if stream.name not in self.STREAMS:
                logger.warn(f"Stream '{stream.name}' is not recognized in this source")
                continue

            if configured_stream.sync_mode == SyncMode.incremental and cursor_field in state[stream.name]:

                for record in self._client.replicate(stream.name, txidfra=state[stream.name][cursor_field], txidtil=txid):
                    formatted_record = self._format_columns(record)

                    yield self._record(stream=stream.name, data=formatted_record)
            else:
                for record in self._client.replicate(stream.name):
                    formatted_record = self._format_columns(record)

                    yield self._record(stream=stream.name, data=formatted_record)

            # Set new state
            # Add one cause SDK is inclusive
            state[stream.name][cursor_field] = txid + 1
            yield self._state(state)

    @staticmethod
    def _record(stream: str, data: Dict[str, any]) -> AirbyteMessage:
        now = int(datetime.now().timestamp()) * 1000
        return AirbyteMessage(type=Type.RECORD, record=AirbyteRecordMessage(stream=stream, data=data, emitted_at=now))

    @staticmethod
    def _state(data: Dict[str, any]):
        return AirbyteMessage(type=Type.STATE, state=AirbyteStateMessage(data=data))
