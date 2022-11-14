"""
This module is a sample use case of an 3 different API
to validate and standardize the address. Functions from this
file can be used in Azure triggers to transform the data.
"""

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


# This function is for GEO_LOCATION API
def single_address_validator(
    street_name: str, city: str, region: str, postalCode: str, country: str
):
    """
    This function uses the Geo-Location API that generates coordinates for the address in query
    and a standardized version of the address if it's available.
    :param street_name:
    :param city:
    :param region:
    :param postalCode:
    :param country:
    :return: address dictionary.
    """

    parameters = {
        "api-version": "1.0",
        "subscription-key": os.getenv("GEO_API_KEY"),
        "countryCode": country,
        "streetName": street_name,
        "postalCode": postalCode,
        "municipality": city,
        "countrySubdivision": region,
    }
    try:
        response = requests.get(
            "https://atlas.microsoft.com/search/address/structured/json",
            params=parameters,
        )
        address = eval(response.text)["results"][0]["address"]
    except Exception as exception:
        # Failed to get response from the API. API might be down or error
        # in API call syntax. Evaluation of the response might also fail.
        print(
            "Call to API failed. Make sure the obtain response has "
            "appropriate keys. failing on Parameters" + str(parameters)
        )
        raise SystemExit from exception

    return address


# This function is for PlaceKey API
def placeKeyApi(
    street_address: str,
    city: str,
    region: str,
    postal_code: str,
    iso_country_code: str,
):
    """
    This API generates a hash for the addresses after validating them and standardizing it.
    :param street_address:
    :param city:
    :param region:
    :param postal_code:
    :param iso_country_code:
    :return: JSON response of the PlaceKey hash.
    """
    address = {
        "street_address": street_address,
        "city": city,
        "region": region,
        "postal_code": postal_code,
        "iso_country_code": iso_country_code,
    }
    try:
        response = eval(
            str(pk_api.lookup_placekey(**address, strict_address_match=False))
        )
    except Exception as exception:
        # Evaluation of the response failed or API call failed.
        print(
            "Evaluation of the response failed or API call failed. evaluating address"
            + str(address)
        )
        raise SystemExit from exception

    return response


# This function is for address validation API.
def azure_validation(
    street_name: str, city: str, region: str, postalCode: str, country: str
):
    """
    This function calls the AZURE address validation api that validates an input address.
    :param street_name:
    :param city:
    :param region:
    :param postalCode:
    :param country:
    :return: It returns the standardized address or the fields where address is failing.
    """
    address_validate = {
        "addressLine1": street_name,
        "city": city,
        "region": region,
        "postalCode": postalCode,
        "country": country,
    }
    try:
        response = client.address.validate(
            address=address_validate,
        )
        address = response.suggested_addresses
    except Exception as exception:
        print("Check API response for value" + str(address_validate))
        raise SystemExit from exception

    if address:
        return eval(str(address.pop()))
    else:
        return {"error": str(response.validation_message)}


# Sample Data.
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

df = pd.DataFrame(
    {
        "street_name": street_name,
        "city": city,
        "region": region,
        "postalCode": postalCode,
        "country": country,
    }
)
print(df)

# small code snippet to run the Azure Geo-Locate API function call on the data.
df["azure_geo"] = df.apply(
    lambda row: single_address_validator(
        row["street_name"],
        row["city"],
        row["region"],
        row["postalCode"],
        row["country"],
    ),
    axis=1,
)

df2 = pd.json_normalize(df["azure_geo"])
df_azure_geo = pd.concat([df, df2], axis=1)
df_azure_geo.to_excel("output_azure_geo.xlsx")
df.drop("azure_geo", axis=1)

# small code snippet to run the PlaceKey API function call on the data.
df["placeKey"] = df.apply(
    lambda row: placeKeyApi(
        row["street_name"],
        row["city"],
        row["region"],
        row["postalCode"],
        row["country"],
    ),
    axis=1,
)

df2 = pd.json_normalize(df["placeKey"])
df_placekey = pd.concat([df, df2], axis=1)
df_placekey.to_excel("output_placeKey.xlsx")
df.drop("placeKey", axis=1)

# small code snippet to run the Azure address validate API function call on the data.
df["azure_validation"] = df.apply(
    lambda row: azure_validation(
        row["street_name"],
        row["city"],
        row["region"],
        row["postalCode"],
        row["country"],
    ),
    axis=1,
)

df2 = pd.json_normalize(df["azure_validation"])
df_azure_validation = pd.concat([df, df2], axis=1)
df_azure_validation.to_excel("output_validation_azure.xlsx")
