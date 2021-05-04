"""
MIT License

Copyright (c) 2020 Airbyte

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import pytest
from source_dawa.client import Client



@pytest.fixture()
def get_client():
    client = Client()
    return client

@pytest.fixture()
def get_api(get_client):
    return get_client._client

def test_client_connection(get_client):
    status, error = get_client.health_check()
    assert status

def test_client_getting_streams(get_client):
    streams = get_client.get_streams()
    assert streams


def test_client_landpostnummer(get_api):
    landpostnummer = list(get_api.replicate("landpostnummer", txidfra="3789366", txidtil="3789366"))
    assert isinstance(landpostnummer, list)


def test_client_stedtilknytning(get_api):
    stedtilknytning = list(get_api.replicate("stedtilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(stedtilknytning, list)


def test_client_ejerlav(get_api):
    ejerlav = list(get_api.replicate("ejerlav", txidfra="3789366", txidtil="3789366"))
    assert isinstance(ejerlav, list)


def test_client_sted(get_api):
    sted = list(get_api.replicate("sted", txidfra="3789366", txidtil="3789366"))
    assert isinstance(sted, list)


def test_client_postnummertilknytning(get_api):
    postnummertilknytning = list(get_api.replicate("postnummertilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(postnummertilknytning, list)


def test_client_zone(get_api):
    zone = list(get_api.replicate("zone", txidfra="3789366", txidtil="3789366"))
    assert isinstance(zone, list)


def test_client_bygningtilknytning(get_api):
    bygningtilknytning = list(get_api.replicate("bygningtilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(bygningtilknytning, list)


def test_client_valglandsdelstilknytning(get_api):
    valglandsdelstilknytning = list(get_api.replicate("valglandsdelstilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(valglandsdelstilknytning, list)


def test_client_supplerendebynavn(get_api):
    supplerendebynavn = list(get_api.replicate("supplerendebynavn", txidfra="3789366", txidtil="3789366"))
    assert isinstance(supplerendebynavn, list)


def test_client_vejmidte(get_api):
    vejmidte = list(get_api.replicate("vejmidte", txidfra="3789366", txidtil="3789366"))
    assert isinstance(vejmidte, list)


def test_client_navngivenvej(get_api):
    navngivenvej = list(get_api.replicate("navngivenvej", txidfra="3789366", txidtil="3789366"))
    assert isinstance(navngivenvej, list)


def test_client_jordstykketilknytning(get_api):
    jordstykketilknytning = list(get_api.replicate("jordstykketilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(jordstykketilknytning, list)


def test_client_bygning(get_api):
    bygning = list(get_api.replicate("bygning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(bygning, list)


def test_client_sognetilknytning(get_api):
    sognetilknytning = list(get_api.replicate("sognetilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(sognetilknytning, list)


def test_client_storkreds(get_api):
    storkreds = list(get_api.replicate("storkreds", txidfra="3789366", txidtil="3789366"))
    assert isinstance(storkreds, list)


def test_client_stednavntilknytning(get_api):
    stednavntilknytning = list(get_api.replicate("stednavntilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(stednavntilknytning, list)


def test_client_højde(get_api):
    højde = list(get_api.replicate("højde", txidfra="3789366", txidtil="3789366"))
    assert isinstance(højde, list)


def test_client_valglandsdel(get_api):
    valglandsdel = list(get_api.replicate("valglandsdel", txidfra="3789366", txidtil="3789366"))
    assert isinstance(valglandsdel, list)


def test_client_adgangsadresse(get_api):
    adgangsadresse = list(get_api.replicate("adgangsadresse", txidfra="3789366", txidtil="3789366"))
    assert isinstance(adgangsadresse, list)


def test_client_stednavn(get_api):
    stednavn = list(get_api.replicate("stednavn", txidfra="3789366", txidtil="3789366"))
    assert isinstance(stednavn, list)


def test_client_opstillingskredstilknytning(get_api):
    opstillingskredstilknytning = list(get_api.replicate("opstillingskredstilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(opstillingskredstilknytning, list)


def test_client_retskredstilknytning(get_api):
    retskredstilknytning = list(get_api.replicate("retskredstilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(retskredstilknytning, list)


def test_client_storkredstilknytning(get_api):
    storkredstilknytning = list(get_api.replicate("storkredstilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(storkredstilknytning, list)


def test_client_ikke_brofast_husnummer(get_api):
    ikke_brofast_husnummer = list(get_api.replicate("ikke_brofast_husnummer", txidfra="3789366", txidtil="3789366"))
    assert isinstance(ikke_brofast_husnummer, list)


def test_client_menighedsrådsafstemningsområde(get_api):
    menighedsrådsafstemningsområde = list(get_api.replicate("menighedsrådsafstemningsområde", txidfra="3789366", txidtil="3789366"))
    assert isinstance(menighedsrådsafstemningsområde, list)


def test_client_politikredstilknytning(get_api):
    politikredstilknytning = list(get_api.replicate("politikredstilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(politikredstilknytning, list)


def test_client_vejpunkt(get_api):
    vejpunkt = list(get_api.replicate("vejpunkt", txidfra="3789366", txidtil="3789366"))
    assert isinstance(vejpunkt, list)


def test_client_vejstykke(get_api):
    vejstykke = list(get_api.replicate("vejstykke", txidfra="3789366", txidtil="3789366"))
    assert isinstance(vejstykke, list)


def test_client_regionstilknytning(get_api):
    regionstilknytning = list(get_api.replicate("regionstilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(regionstilknytning, list)


def test_client_retskreds(get_api):
    retskreds = list(get_api.replicate("retskreds", txidfra="3789366", txidtil="3789366"))
    assert isinstance(retskreds, list)


def test_client_kommune(get_api):
    kommune = list(get_api.replicate("kommune", txidfra="3789366", txidtil="3789366"))
    assert isinstance(kommune, list)


def test_client_postnummer(get_api):
    postnummer = list(get_api.replicate("postnummer", txidfra="3789366", txidtil="3789366"))
    assert isinstance(postnummer, list)


def test_client_brofasthed(get_api):
    brofasthed = list(get_api.replicate("brofasthed", txidfra="3789366", txidtil="3789366"))
    assert isinstance(brofasthed, list)


def test_client_adresse(get_api):
    adresse = list(get_api.replicate("adresse", txidfra="3789366", txidtil="3789366"))
    assert isinstance(adresse, list)


def test_client_vejstykkepostnummerrelation(get_api):
    vejstykkepostnummerrelation = list(get_api.replicate("vejstykkepostnummerrelation", txidfra="3789366", txidtil="3789366"))
    assert isinstance(vejstykkepostnummerrelation, list)


def test_client_afstemningsområde(get_api):
    afstemningsområde = list(get_api.replicate("afstemningsområde", txidfra="3789366", txidtil="3789366"))
    assert isinstance(afstemningsområde, list)


def test_client_zonetilknytning(get_api):
    zonetilknytning = list(get_api.replicate("zonetilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(zonetilknytning, list)


def test_client_dagi_postnummer(get_api):
    dagi_postnummer = list(get_api.replicate("dagi_postnummer", txidfra="3789366", txidtil="3789366"))
    assert isinstance(dagi_postnummer, list)


def test_client_menighedsrådsafstemningsområdetilknytning(get_api):
    menighedsrådsafstemningsområdetilknytning = list(get_api.replicate("menighedsrådsafstemningsområdetilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(menighedsrådsafstemningsområdetilknytning, list)


def test_client_region(get_api):
    region = list(get_api.replicate("region", txidfra="3789366", txidtil="3789366"))
    assert isinstance(region, list)


def test_client_politikreds(get_api):
    politikreds = list(get_api.replicate("politikreds", txidfra="3789366", txidtil="3789366"))
    assert isinstance(politikreds, list)


def test_client_supplerendebynavntilknytning(get_api):
    supplerendebynavntilknytning = list(get_api.replicate("supplerendebynavntilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(supplerendebynavntilknytning, list)


def test_client_jordstykke(get_api):
    jordstykke = list(get_api.replicate("jordstykke", txidfra="3789366", txidtil="3789366"))
    assert isinstance(jordstykke, list)


def test_client_afstemningsområdetilknytning(get_api):
    afstemningsområdetilknytning = list(get_api.replicate("afstemningsområdetilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(afstemningsområdetilknytning, list)


def test_client_opstillingskreds(get_api):
    opstillingskreds = list(get_api.replicate("opstillingskreds", txidfra="3789366", txidtil="3789366"))
    assert isinstance(opstillingskreds, list)


def test_client_sogn(get_api):
    sogn = list(get_api.replicate("sogn", txidfra="3789366", txidtil="3789366"))
    assert isinstance(sogn, list)


def test_client_kommunetilknytning(get_api):
    kommunetilknytning = list(get_api.replicate("kommunetilknytning", txidfra="3789366", txidtil="3789366"))
    assert isinstance(kommunetilknytning, list)
