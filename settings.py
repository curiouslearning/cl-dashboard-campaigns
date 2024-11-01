import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import campaigns
from rich import print
import pandas as pd
import users
import datetime as dt
from google.cloud import secretmanager
import json
import asyncio

default_daterange = [dt.datetime(2021, 1, 1).date(), dt.date.today()]


def get_gcp_credentials():
    # first get credentials to secret manager
    client = secretmanager.SecretManagerServiceClient()

    # get the secret that holds the service account key
    name = "projects/405806232197/secrets/service_account_json/versions/latest"
    response = client.access_secret_version(name=name)
    key = response.payload.data.decode("UTF-8")

    # use the key to get service account credentials
    service_account_info = json.loads(key)
    # Create BigQuery API client.
    gcp_credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ],
    )

    bq_client = bigquery.Client(
        credentials=gcp_credentials, project="dataexploration-193817"
    )
    return bq_client


def initialize():
    pd.options.mode.copy_on_write = True
    pd.set_option("display.max_columns", 20)

    bq_client = get_gcp_credentials()

    if "bq_client" not in st.session_state:
        st.session_state["bq_client"] = bq_client


# Get the campaign data from BigQuery, roll it up per campaign
def init_data():
# Call the combined asynchronous campaign data function
    df_campaign_users, df_google_ads_data, df_facebook_ads_data = cache_marketing_data()

    #Get all campaign data by segment_date
    df_campaigns_all = pd.concat([df_google_ads_data, df_facebook_ads_data])
    df_campaigns_all = campaigns.add_country_and_language(df_campaigns_all)
    df_campaigns_all = df_campaigns_all.reset_index(drop=True)
    df_campaigns_rollup = campaigns.rollup_campaign_data(df_campaigns_all)

    if "df_campaigns_rollup" not in st.session_state:
        st.session_state["df_campaigns_rollup"] = df_campaigns_rollup

    if "df_campaigns_all" not in st.session_state:
        st.session_state["df_campaigns_all"] = df_campaigns_all
    
    if "df_campaign_users" not in st.session_state:
        st.session_state["df_campaign_users"] = df_campaign_users

@st.cache_data(ttl="1d", show_spinner="Loading Data")
def cache_marketing_data():
    # Execute the async function and return its result synchronously
    return asyncio.run(campaigns.get_campaign_data())



