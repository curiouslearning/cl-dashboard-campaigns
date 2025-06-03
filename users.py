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
    
    df["app_language"] = df["app_language"].replace(
        "arabictest", "arabic"
    )


    # Remove garbage sources
    df = df[
        ~df['source_id'].isin(sources_to_remove) &
        ~df['source_id'].str.startswith('test', na=False)
]

    return df

# Users who play multiple languages or have multiple countries are consolidated
# to a single entry based on which combination took them the furthest in the game.
# If its a tie, will take the first entry. The reference to duplicates are users
# with multiple entries because of variations in these combinations


@st.cache_data(ttl="1d", show_spinner=False)
def clean_users_to_single_language(df_app_launch, df_cr_users):
    df_cr_users.info()

    # ✅  Identify and remove all duplicates from df_app_launch, but SAVE them for later
    duplicate_user_ids = df_app_launch[df_app_launch.duplicated(
        subset='cr_user_id', keep=False)]
    df_app_launch = df_app_launch[~df_app_launch["cr_user_id"].isin(
        duplicate_user_ids["cr_user_id"])]

    # ✅  Get list of users that had duplicates
    unique_duplicate_ids = duplicate_user_ids['cr_user_id'].unique().tolist()

    # ✅  Define event ranking of the funnel
    event_order = ["download_completed", "tapped_start",
                   "selected_level", "puzzle_completed", "level_completed"]
    event_rank = {event: rank for rank, event in enumerate(event_order)}

    # ✅  Ensure "furthest_event" has no missing values
    df_cr_users["furthest_event"] = df_cr_users["furthest_event"].fillna(
        "unknown")

    # ✅ Map event to numeric rank
    df_cr_users["event_rank"] = df_cr_users["furthest_event"].map(event_rank)

    # ✅ Flag whether event is "level_completed" - this means we switch to level number to determine furthest progress
    df_cr_users["is_level_completed"] = df_cr_users["furthest_event"] == "level_completed"

    # ✅ Ensure a single row per user across country & language
    df_cr_users = df_cr_users.sort_values(["cr_user_id", "is_level_completed", "max_user_level", "event_rank"],
                                          ascending=[True, False, False, False])

    df_cr_users = df_cr_users.drop_duplicates(
        subset=["cr_user_id"], keep="first")  # ✅ Keep only best progress row

    # ✅ Ensure every user in df_cr_users has a matching row in df_app_launch
    users_to_update = df_cr_users[["cr_user_id", "app_language", "country"]].merge(
        df_app_launch[["cr_user_id", "app_language", "country"]],
        on="cr_user_id",
        how="left",
        suffixes=("_cr", "_app")
    )

    # ✅ Find users where the `app_language` in df_app_launch does not match the selected best `app_language` from df_cr_users
    language_mismatch = users_to_update[users_to_update["app_language_cr"]
                                        != users_to_update["app_language_app"]]

    if not language_mismatch.empty:

        # ✅ Update df_app_launch to reflect the correct `app_language` from df_cr_users
        df_app_launch.loc[df_app_launch["cr_user_id"].isin(language_mismatch["cr_user_id"]), "app_language"] = \
            df_app_launch["cr_user_id"].map(
                df_cr_users.set_index("cr_user_id")["app_language"])

    # ✅ Ensure all users with duplicates exist in df_cr_users
    missing_users = set(unique_duplicate_ids) - set(df_cr_users["cr_user_id"])

    # ✅ Add back the correct user rows in df_app_launch, ensuring **matching language & country**
    users_to_add_back = duplicate_user_ids.merge(
        df_cr_users[["cr_user_id", "app_language", "country"]],
        on=["cr_user_id", "app_language", "country"],
        how="left"
    )

    # ✅ Drop NaN values to ensure only valid rows are added back
    users_to_add_back = users_to_add_back.dropna(subset=["app_language"])

    # ✅ If any users are still missing, add a fallback row for them
    fallback_users = duplicate_user_ids[duplicate_user_ids["cr_user_id"].isin(
        missing_users)]
    fallback_users = fallback_users.drop_duplicates(
        subset="cr_user_id", keep="first")

    # Append the fallback users
    users_to_add_back = pd.concat([users_to_add_back, fallback_users])

    # ✅ Deduplicate to ensure only one row per cr_user_id is added back
    users_to_add_back = users_to_add_back.drop_duplicates(
        subset="cr_user_id", keep="first")

    # ✅ Restore users into df_app_launch
    df_app_launch = pd.concat([df_app_launch, users_to_add_back])

    # ✅ Ensure df_app_launch has only unique cr_user_id

    df_app_launch = df_app_launch.drop_duplicates(
        subset="cr_user_id", keep="first")

    return df_app_launch, df_cr_users
