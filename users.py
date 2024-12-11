import streamlit as st
import pandas as pd
from rich import print as print
import numpy as np

sources_to_remove = ['testingSource', 'DSS-Botswana']

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

def cleanup_users(df):
            # Eliminate duplicate cr users (multiple language combinations) - just keep the first one
    df = df.drop_duplicates(
            subset='user_pseudo_id', keep="first")
    df["event_date"] = pd.to_datetime(
        df["event_date"], errors='coerce')
    df["event_date"] = df["event_date"].dt.date

        # Fix data typos
    df["app_language"] = df["app_language"].replace(
            "ukranian", "ukrainian"
        )
    df["app_language"] = df["app_language"].replace(
            "malgache", "malagasy"
        )

    # Remove garbage sources
    df = df[
        ~df['source_id'].isin(sources_to_remove) &
        ~df['source_id'].str.startswith('test', na=False)
]

    return df