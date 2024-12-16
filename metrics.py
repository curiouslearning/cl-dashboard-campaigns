import streamlit as st
from rich import print
import pandas as pd
import numpy as np
import datetime as dt
import users


default_daterange = [dt.datetime(2024, 9, 11).date(), dt.date.today()]


# Takes the complete user lists (cr_user_id) and filters based on input data, and returns
# a new filtered dataset
def filter_user_data(
    daterange=default_daterange,
    countries_list=["All"],
    stat="LR",
    language=["All"],
    user_list=None,
    source_id=None
):

    # Check if necessary dataframes are available
    if not all(key in st.session_state for key in ["campaign_users_app_launch", "campaign_users_progress"]):
        print("PROBLEM!")
        return pd.DataFrame()

    # Select the appropriate dataframe based on app and stat

    if stat == "LR":
        df = st.session_state.campaign_users_app_launch
    else:
        df = st.session_state.campaign_users_progress

    # Initialize a boolean mask
    mask = (df['first_open'] >= daterange[0]) & (
        df['first_open'] <= daterange[1])

    # Apply country filter if not "All"
    if countries_list[0] != "All":
        mask &= df['country'].isin(set(countries_list))

    # Apply language filter if not "All" and stat is not "FO"
    if language[0] != "All" and stat != "FO":
        mask &= df['app_language'].isin(set(language))

    # Apply stat-specific filters
    if stat == "LA":
        mask &= (df['max_user_level'] >= 1)
    elif stat == "RA":
        mask &= (df['max_user_level'] >= 25)
    elif stat == "GC":  # Game completed
        mask &= (df['max_user_level'] >= 1) & (df['gpc'] >= 90)
    elif stat == "LR" or stat == "FO":
        # No additional filters for these stats beyond daterange and optional countries/language
        pass

    if source_id is not None:
        mask &= (df["source_id"] == source_id)
        
    # Filter the dataframe with the combined mask
    df = df.loc[mask]

    # If user list subset was passed in, filter on that as well
    if user_list is not None:
        df = df[df["cr_user_id"].isin(user_list)]
    return df


def get_totals_by_metric(
    daterange=default_daterange,
    countries_list=[],
    stat="LR",
    language="All",
    user_list=[], # New parameter allowing a filter of results by cr_user_id list
    source_id=None
):
    
    # if no list passed in then get the full list
    if len(countries_list) == 0:
        countries_list = users.get_country_list()

    df_user_list = filter_user_data(
        daterange, countries_list, stat,  language=language, user_list=user_list,source_id=source_id
    )

    if stat not in ["DC", "TS", "SL", "PC", "LA"]:
        return len(df_user_list)  # All LR or FO
    else:
        download_completed_count = len(
            df_user_list[df_user_list["furthest_event"]
                         == "download_completed"]
        )

        tapped_start_count = len(
            df_user_list[df_user_list["furthest_event"] == "tapped_start"]
        )
        selected_level_count = len(
            df_user_list[df_user_list["furthest_event"] == "selected_level"]
        )
        puzzle_completed_count = len(
            df_user_list[df_user_list["furthest_event"] == "puzzle_completed"]
        )
        level_completed_count = len(
            df_user_list[df_user_list["furthest_event"] == "level_completed"]
        )

        if stat == "DC":
            return (
                download_completed_count
                + tapped_start_count
                + selected_level_count
                + puzzle_completed_count
                + level_completed_count
            )

        if stat == "TS":
            return (
                tapped_start_count
                + selected_level_count
                + puzzle_completed_count
                + level_completed_count
            )

        if stat == "SL":  # all PC and SL users implicitly imply those events
            return tapped_start_count + puzzle_completed_count + level_completed_count

        if stat == "PC":
            return puzzle_completed_count + level_completed_count

        if stat == "LA":
            return level_completed_count
