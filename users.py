import streamlit as st
import pandas as pd
from rich import print as print
import numpy as np
from pyinstrument import Profiler

import asyncio

# How far back to obtain user data.  Currently the queries pull back to 01/01/2021
start_date = "2024/09/11"
sources_to_remove = ['testingSource','DSS-Botswana']

# Firebase returns two different formats of user_pseudo_id between
# web app events and android events, so we have to run multiple queries
# instead of a join because we don't have a unique key for both
# This would all be unncessery if dev had included the app user id per the spec.

import streamlit as st

async def get_users_list():
    p = Profiler(async_mode="disabled")
    with p:

        bq_client = st.session_state.bq_client

        # Helper function to run BigQuery in a thread
        async def run_query(query):
            return await asyncio.to_thread(bq_client.query(query).to_dataframe)

        # Define the queries
        sql_campaign_users = f"""
            SELECT *
            FROM `dataexploration-193817.user_data.cr_app_launch_campaign_data`
         """

        # Run all the queries asynchronously
        df_campaign_users, = await asyncio.gather(
            run_query(sql_campaign_users)
        )
        print(type(df_campaign_users))

        # Eliminate duplicate cr users (multiple language combinations) - just keep the first one
        df_campaign_users = df_campaign_users.drop_duplicates(subset='user_pseudo_id', keep="first")

        # Fix data typos
        df_campaign_users["app_language"] = df_campaign_users["app_language"].replace(
            "ukranian", "ukrainian"
        )
        df_campaign_users["app_language"] = df_campaign_users["app_language"].replace(
            "malgache", "malagasy"
        )

    #Remove garbage sources
    df_campaign_users = df_campaign_users[~df_campaign_users['source'].isin(sources_to_remove)]

    p.print(color="red")
    return df_campaign_users




@st.cache_data(ttl="1d", show_spinner=False)
def get_language_list():
    lang_list = ["All"]
    if "bq_client" in st.session_state:
        bq_client = st.session_state.bq_client
        sql_query = f"""
                    SELECT display_language
                    FROM `dataexploration-193817.user_data.language_max_level`
                    ;
                    """
        rows_raw = bq_client.query(sql_query)
        rows = [dict(row) for row in rows_raw]
        if len(rows) == 0:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df.drop_duplicates(inplace=True)
        lang_list = np.array(df.values).flatten().tolist()
        lang_list = [x.strip(" ") for x in lang_list]
    return lang_list


@st.cache_data(ttl="1d", show_spinner=False)
def get_country_list():
    countries_list = []
    if "bq_client" in st.session_state:
        bq_client = st.session_state.bq_client
        sql_query = f"""
                    SELECT *
                    FROM `dataexploration-193817.user_data.active_countries`
                    order by country asc
                    ;
                    """
        rows_raw = bq_client.query(sql_query)
        rows = [dict(row) for row in rows_raw]
        if len(rows) == 0:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        countries_list = np.array(df.values).flatten().tolist()
    return countries_list


@st.cache_data(ttl="1d", show_spinner=False)
def get_app_version_list():
    app_versions = []
    if "bq_client" in st.session_state:
        bq_client = st.session_state.bq_client
        sql_query = f"""
                    SELECT *
                    FROM `dataexploration-193817.user_data.cr_app_versions`
                    """
        rows_raw = bq_client.query(sql_query)
        rows = [dict(row) for row in rows_raw]
        if len(rows) == 0:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        conditions = [
            f"app_version >=  'v1.0.25'",
        ]
        query = " and ".join(conditions)
        df = df.query(query)

        app_versions = np.array(df.values).flatten().tolist()
        app_versions.insert(0, "All")

    return app_versions


@st.cache_data(ttl="1d", show_spinner=False)
def get_funnel_snapshots(daterange,languages):

    if "bq_client" in st.session_state:
        bq_client = st.session_state.bq_client
    else:
        st.write ("No database connection")
        return

    languages_str = ', '.join([f"'{lang}'" for lang in languages])

    sql_query = f"""
            SELECT *
            FROM `dataexploration-193817.user_data.funnel_snapshots`
            WHERE language IN ({languages_str})
            AND
            DATE(date) BETWEEN '{daterange[0].strftime("%Y-%m-%d")}' AND '{daterange[1].strftime("%Y-%m-%d")}' ;

            """

    df = bq_client.query(sql_query).to_dataframe() 
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    return df
