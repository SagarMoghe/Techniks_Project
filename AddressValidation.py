import pandas as pd
import requests
from placekey.api import PlacekeyAPI
from azure.identity import DefaultAzureCredential
from azure.mgmt.billing import BillingManagementClient

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', 500)

placekey_api_key = "Eva3Pg1eAHDlxdoFq38VV5ElXHi0m9ga"
pk_api = PlacekeyAPI(placekey_api_key)

client = BillingManagementClient(
    credential=DefaultAzureCredential(),
    subscription_id="6a5033ca-a0de-4866-bb06-1d8767f83c8c",
)

"""
Before running this script, please add these 3 Variables in your environment.
If your running Mac, just copy past these 3 lines and past them into terminal and hit enter.

AZURE_CLIENT_SECRET=Feq8Q~DqW5HqyLVwvp2ZvlrQoMDi~jXU4RQksax9

AZURE_CLIENT_ID=57797f08-5978-41af-8329-958e51439d40

AZURE_TENANT_ID=5d024880-1af1-4bae-be79-5b9c23101d96


"""


# This function is for GEO_LOCATION API
def single_address_validator(street_name, city, region, postalCode, country):
    parameters = {
        "api-version": "1.0",
        "subscription-key": "iOCE4LmkavtZHc22xrK-H9pm50XUNCA_VOiwnU2SPj0",
        "countryCode": country,
        "streetName": street_name,
        "postalCode": postalCode,
        "municipality": city,
        "countrySubdivision": region
    }
    response = requests.get("https://atlas.microsoft.com/search/address/structured/json", params=parameters)
    return eval(response.text)["results"][0]["address"]


# This function is for PlaceKey API
def placeKeyApi(street_address, city, region, postal_code, iso_country_code):
    address = {

        "street_address": street_address,
        "city": city,
        "region": region,
        "postal_code": postal_code,
        "iso_country_code": iso_country_code

    }
    return eval(str(pk_api.lookup_placekey(**address, strict_address_match=False)))


# This function is for address validation API.
def azure_validation(street_name, city, region, postalCode, country):
    response = client.address.validate(
        address={
            "addressLine1": street_name,
            "city": city,
            "region": region,
            "postalCode": postalCode,
            "country": country
        },
    )
    address = response.suggested_addresses
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
city = ["Halifax",
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
    "ON"
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

df = pd.DataFrame({
    "street_name": street_name,
    "city": city,
    "region": region,
    "postalCode": postalCode,
    "country": country
})
print(df)

df["azure_geo"] = df.apply(
    lambda row: single_address_validator(row["street_name"], row["city"], row["region"], row["postalCode"],
                                         row["country"]), axis=1)

df2 = pd.json_normalize(df['azure_geo'])
df_azure_geo = pd.concat([df, df2], axis=1)
df_azure_geo.to_excel('output_azure_geo.xlsx')
df.drop("azure_geo", axis=1)

df["placeKey"] = df.apply(
    lambda row: placeKeyApi(row["street_name"], row["city"], row["region"], row["postalCode"], row["country"]), axis=1)

df2 = pd.json_normalize(df['placeKey'])
df_placekey = pd.concat([df, df2], axis=1)
df_placekey.to_excel('output_placeKey.xlsx')
df.drop("placeKey", axis=1)

df["azure_validation"] = df.apply(
    lambda row: azure_validation(row["street_name"], row["city"], row["region"], row["postalCode"],
                                 row["country"]), axis=1)

df2 = pd.json_normalize(df['azure_validation'])
df_azure_validation = pd.concat([df, df2], axis=1)
df_azure_validation.to_excel('output_validation_azure.xlsx')
