import streamlit as st
from rich import print
import pandas as pd
from settings import default_daterange

def get_user_cohort_df(
    session_df,
    daterange=None,
    languages=["All"],
    countries_list=["All"],

    source_id=None,
    campaign_id=None,
):
    """
    Returns a DataFrame (all columns) for the cohort matching filters.
    """
    cohort_df = session_df.copy()

    # Apply filters
    if daterange is not None and len(daterange) == 2:
        start = pd.to_datetime(daterange[0])
        end = pd.to_datetime(daterange[1])
        cohort_df = cohort_df[
            (cohort_df["first_open"] >= start) & (
                cohort_df["first_open"] <= end)
        ]

    if countries_list and countries_list != ["All"]:
        cohort_df = cohort_df[cohort_df["country"].isin(countries_list)]

    if languages and languages != ["All"]:
        lang_col = "app_language" if "app_language" in cohort_df.columns else "language"
        cohort_df = cohort_df[cohort_df[lang_col].isin(languages)]


    # New: filter by source_id and campaign_id
    if source_id is not None:
        cohort_df = cohort_df[cohort_df["source_id"] == source_id]
    if campaign_id is not None:
        cohort_df = cohort_df[cohort_df["campaign_id"] == campaign_id]

    return cohort_df


def get_filtered_cohort(daterange, language, countries_list, source_id=None, campaign_id=None):
    """
    Returns (user_cohort_df, user_cohort_df_LR) with all filters applied.
    """
    session_df_LR = select_user_dataframe(stat="LR")
    user_cohort_df_LR = get_user_cohort_df(
        session_df=session_df_LR,
        daterange=daterange,
        languages=language,
        countries_list=countries_list,
        source_id=source_id,
        campaign_id=campaign_id
    )

    session_df = select_user_dataframe()
    user_cohort_df = get_user_cohort_df(
        session_df=session_df,
        daterange=daterange,
        languages=language,
        countries_list=countries_list,
        source_id=source_id,
        campaign_id=campaign_id
    )

    return user_cohort_df, user_cohort_df_LR



def select_user_dataframe(stat=None):
    """
    For this dashboard:
      - If stat == "LR": use campaign_users_app_launch
      - Else: use campaign_users_progress
    """
    if stat == "LR":
        return st.session_state["campaign_users_app_launch"]
    else:
        return st.session_state["campaign_users_progress"]


    
@st.cache_data(ttl="1d", show_spinner=False)
def get_cohort_totals_by_metric(
    cohort_df,
    stat="LR"
):
    """
    Given a cohort_df (already filtered!), count users in each funnel stage or apply stat-specific filter.
    - cohort_df: DataFrame, filtered to your user cohort (one row per user)
    - stat: string, which funnel metric to count ("LR", "DC", "TS", "SL", "PC", "LA", "RA", "GC")
    """

    # Stat-specific filters (formerly in filter_user_data)
    if stat == "LA":
        # Learners Acquired: max_user_level >= 1
        return (cohort_df['max_user_level'] >= 1).sum()
    elif stat == "RA":
        # Readers Acquired: max_user_level >= 25
        return (cohort_df['max_user_level'] >= 25).sum()
    elif stat == "GC":
        # Game Completed: max_user_level >= 1 AND gpc >= 90
        return ((cohort_df['max_user_level'] >= 1) & (cohort_df['gpc'] >= 90)).sum()
    elif stat == "LR":
        # Learner Reached: all users in cohort
        return len(cohort_df)

    # Otherwise: classic funnel by furthest_event
    furthest = cohort_df["furthest_event"]

    download_completed_count = (furthest == "download_completed").sum()
    tapped_start_count = (furthest == "tapped_start").sum()
    selected_level_count = (furthest == "selected_level").sum()
    puzzle_completed_count = (furthest == "puzzle_completed").sum()
    level_completed_count = (furthest == "level_completed").sum()

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
    if stat == "SL":
        return (
            selected_level_count
            + puzzle_completed_count
            + level_completed_count
        )
    if stat == "PC":
        return (
            puzzle_completed_count
            + level_completed_count
        )

    return 0  # default fallback

# Takes a dataframe and filters according to input parameters


def filter_dataframe(
    df,
    daterange=default_daterange,
    countries_list=["All"],
    language=["All"],
    source_id=None
):

    df["event_date"] = pd.to_datetime(
        df["event_date"], format="%Y%m%d").dt.date

    # Initialize a boolean mask
    mask = (df['event_date'] >= daterange[0]) & (
        df['event_date'] <= daterange[1])

    # Apply country filter if not "All"
    if countries_list[0] != "All":
        mask &= df['country'].isin(set(countries_list))

    # Apply language filter if not "All"
    if language[0] != "All":
        mask &= df['app_language'].isin(set(language))

    if 'source_id' in df.columns and source_id is not None:
        mask &= (df["source_id"] == source_id)

    # Filter the dataframe with the combined mask
    df = df.loc[mask]

    return df
