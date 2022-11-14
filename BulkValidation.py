"""
This module is a sample use case of an 2 API
to validate and standardize the address. Functions from this
file can be used in Azure triggers to transform the data.
"""
from typing import List

import numpy as np
import pandas as pd
import requests
from placekey.api import PlacekeyAPI
from azure.identity import DefaultAzureCredential
from azure.mgmt.billing import BillingManagementClient

import os
from dotenv import load_dotenv

load_dotenv()

placekey_api_key = os.getenv("PLACE_KEY_API")
pk_api = PlacekeyAPI(placekey_api_key)

client = BillingManagementClient(
    credential=DefaultAzureCredential(),
    subscription_id=os.getenv("SUBSCRIPTION_ID"),
)

"""
Before running this script, please add these 3 Variables in your environment.
If your running Mac, just copy past these 3 lines and past them into terminal and hit enter.

AZURE_CLIENT_SECRET=os.getenv("AZURE_CLIENT_SECRET")

AZURE_CLIENT_ID=os.getenv("AZURE_CLIENT_ID")

AZURE_TENANT_ID=os.getenv("AZURE_TENANT_ID")


"""
pd.set_option("display.max_colwidth", None)
pd.set_option("display.max_columns", 500)

street_name = [
    "1333 S park st",
    "1333 South park street, unit 1008",
    "unit 1008, 1333 South park street",
    "1333, South park street, 1008",
    "unit 1008, 1333 South park street",
    "unit 1008, 1030 South park street",
    "unit 1008, 1030 South park street",
    "unit 1008, 1030 South park street",
    "unit 1008, 1030 Sout park street",
    "unit 1008, 1030 South park street",
    "unit 1008, 1030 Sout park street",
]
city = [
    "Halifax",
    "Halifax",
    "halifax",
    "halifax",
    "halifax",
    "Halifax",
    "Halifa",
    "Halifax",
    "Toronto",
    "Toronto",
    "Toronto",
]
region = [
    "Nova Scotia",
    "NS",
    "ns",
    "ns",
    "ns",
    "ns",
    "ns",
    "BC",
    "BC",
    "ON",
    "ON",
]
postalCode = [
    "B3J2K9",
    "B3J 2K9",
    "B3J 2K9",
    "B3J 2K9",
    "B3J 2K9",
    "B3J 2K9",
    "B3H 2W3",
    "B3H 2W3",
    "B3H 2W3",
    "B3J 2K9",
    "B3J 2K9",
]
country = ["CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA"]

# Sample DF with the synthetic data.
df = pd.DataFrame(
    {
        "street_address": street_name,
        "city": city,
        "region": region,
        "postal_code": postalCode,
        "iso_country_code": country,
    }
)
df["json"] = df.to_json(orient="records", lines=True).splitlines()
my_addresses = df["json"].to_numpy()

# JSON data for Geo Locate API.
data = {
    "batchItems": [
        {"query": "?query=400 Broad St, Seattle, WA 98109&limit=3"},
        {"query": "?query=One, Microsoft Way, Redmond, WA 98052&limit=3"},
        {"query": "?query=350 5th Ave, New York, NY 10118&limit=1"},
        {"query": "?query=1333 S park st, Halifax, NS B3J2K9&limit=3"},
        {
            "query": "?query=1333 South Park Street, Unit 1008, Halifax, NS, B3J2K9&limit=3"
        },
        {
            "query": "?query=Champ de Mars, 5 Avenue Anatole France, 75007 Paris, France&limit=1"
        },
    ]
}


def convert(x):
    return eval(x)


my_addresses = np.vectorize(convert)(my_addresses)


def bulk_PlaceKey(bulk_addresses):
    """
    This function takes a list of JSON addresses and returns the corresponding hash values of the addresses.
    :param bulk_addresses: List of JSON address that must contain keys as follows: street_address, city, region,
    postal_code, iso_country_code
    """
    try:
        response = pk_api.lookup_placekeys(bulk_addresses)
    except Exception as exception:
        print("API failed for the call" + str(bulk_addresses))
        raise SystemExit from exception
    return pd.DataFrame(response)


df3 = bulk_PlaceKey(my_addresses.tolist())
df = pd.concat([df, df3], axis=1)
print(df)


def bulk_geoCode(data):
    """
    This function takes JSON input of list of addresses and generates standardize address.
    :param data:
    :return: It returns a JSON response with the address standardize.
    """
    try:
        url = (
            "https://atlas.microsoft.com/search/address/batch/sync/json?api-version=1.0&subscription-key="
            + os.getenv("GEO_API_KEY")
        )
        response = requests.post(url, json=data)
    except Exception as exception:
        print("Check API response for value" + str(data))
        raise SystemExit from exception
    return eval(response.text)


print(bulk_geoCode(data))
