import streamlit as st
import pandas as pd
from rich import print as print
import asyncio
from pyinstrument import Profiler
import users

#Event data started getting the campaign metadata in production on this date
start_date = '2024-11-08'

# Proper UTM parameter data was implemented on the marketing side and in the CR production 

async def get_campaign_data():
    p = Profiler(async_mode="disabled")
    with p:
        bq_client = st.session_state.bq_client

        # Helper function to run BigQuery queries asynchronously
        async def run_query(query):
            return await asyncio.to_thread(bq_client.query(query).to_dataframe)


        # Google Ads Query
        google_ads_query = f"""
            SELECT
                distinct metrics.campaign_id,
                metrics.segments_date as segment_date,
                campaigns.campaign_name,
                metrics_cost_micros as cost
            FROM dataexploration-193817.marketing_data.p_ads_CampaignStats_6687569935 as metrics
            INNER JOIN dataexploration-193817.marketing_data.ads_Campaign_6687569935 as campaigns
            ON metrics.campaign_id = campaigns.campaign_id
            AND metrics.segments_date >= '{start_date}'

        """

        # Facebook Ads Query
        facebook_ads_query = f"""
            SELECT 
                d.campaign_id,
                d.data_date_start as segment_date,
                d.campaign_name,
                d.spend as cost
            FROM dataexploration-193817.marketing_data.facebook_ads_data as d
            WHERE d.data_date_start >= '{start_date}'
            ORDER BY d.data_date_start DESC;
        """

        sql_campaign_users_app_launch = f"""
            SELECT *
            FROM `dataexploration-193817.user_data.cr_app_launch_campaign_data`
            WHERE first_open >= '{start_date}'
         """

        sql_campaign_users_progress = f"""
            SELECT *
            FROM `dataexploration-193817.user_data.cr_user_progress_campaign_data`
            WHERE first_open >= '{start_date}'
         """


        # Run both queries concurrently using asyncio.gather
        campaign_users_progress, campaign_users_app_launch, google_ads_data, facebook_ads_data = await asyncio.gather(
            run_query(sql_campaign_users_progress),
            run_query(sql_campaign_users_app_launch),
            run_query(google_ads_query),
            run_query(facebook_ads_query)
        )

        # Process Google Ads Data
        google_ads_data["campaign_id"] = google_ads_data["campaign_id"].astype(str).str.replace(",", "")
        google_ads_data["cost"] = google_ads_data["cost"].divide(1000000).round(2)
        google_ads_data["segment_date"] = pd.to_datetime(google_ads_data["segment_date"])

    campaign_users_app_launch = users.cleanup_users(campaign_users_app_launch)
    campaign_users_progress = users.cleanup_users(campaign_users_progress)
    p.print(color="red")
    return campaign_users_progress,campaign_users_app_launch, google_ads_data, facebook_ads_data

@st.cache_data(ttl="1d", show_spinner=False)
# Looks for the string following the dash and makes that the associated country.
# This requires a strict naming convention of "[anything without dashes] - [country]]"
def add_country_and_language(df):

    # Define the regex patterns
    country_regex_pattern = r"-\s*(.*)"
    language_regex_pattern = r":\s*([^-]+?)\s*-"
    campaign_regex_pattern = r"\s*(.*)Campaign"

    # Extract the country
    df["country"] = (
        df["campaign_name"].str.extract(country_regex_pattern)[0].str.strip()
    )

    # Remove the word "Campaign" if it exists in the country field
    extracted = df["country"].str.extract(campaign_regex_pattern)
    df["country"] = extracted[0].fillna(df["country"]).str.strip()

    # Extract the language
    df["app_language"] = (
        df["campaign_name"].str.extract(language_regex_pattern)[0].str.strip()
    )

    # Set default values to None where there's no match
    country_contains_pattern = r"-\s*(?:.*)"
    language_contains_pattern = r":\s*(?:[^-]+?)\s*-"

    df["country"] = df["country"].where(
        df["campaign_name"].str.contains(
            country_contains_pattern, regex=True, na=False
        ),
        None,
    )
    df["app_language"] = df["app_language"].where(
        df["campaign_name"].str.contains(
            language_contains_pattern, regex=True, na=False
        ),
        None,
    ).str.lower()

    return df


# This function takes a campaign based dataframe and sums it up into a single row per campaign.  The original dataframe
# has many entries per campaign based on daily extractions.
def rollup_campaign_data(df):
    aggregation = {
        "segment_date": "last",
        "cost": "sum",
    }
    optional_columns = ["country", "app_language"]
    for col in optional_columns:
        if col in df.columns:
            aggregation[col] = "first"

    # find duplicate campaign_ids, create a dataframe for them and remove from original
    duplicates = df[df.duplicated("campaign_id", keep=False)]
    df = df.drop_duplicates("campaign_id", keep=False)

    # Get the newest campaign info according to segment date and use its campaign_name
    # for the other campaign names.
    duplicates = duplicates.sort_values(by="segment_date", ascending=False)
    duplicates["campaign_name"] = duplicates.groupby("campaign_id")[
        "campaign_name"
    ].transform("first")

    # Do another rollup on the duplicates.  This time the campaign name will be the same
    # so we can take any of them
    aggregation["campaign_name"] = "first"
    combined = duplicates.groupby(["campaign_id"], as_index=False).agg(aggregation)

    # put it all back together
    df = pd.concat([df, combined])
    df = df.drop(columns=["segment_date"])

    return df



@st.cache_data(ttl="1d", show_spinner=True)
def build_campaign_table(df, daterange):

    df = (
        df.groupby(["country", "app_language"], as_index=False)
        .agg(
            {
                "cost": "sum",
            }
        )
        .round(2)
    )

    stats = ["LR", "PC", "LA","RA"]
    for idx, row in df.iterrows():
        country_list = [row["country"]]
        language = [row["app_language"].lower()]

        for stat in stats:

            result = metrics.get_totals_by_metric(
                countries_list=country_list,
                language=language,
                daterange=daterange,
                stat=stat,
                app="CR",
            )
            df.at[idx, stat] = result
            df.at[idx, stat + "C"] = (
                (df.at[idx, "cost"] / result).round(2) if result != 0 else 0
            )
            if stat == "LR":
                LR = result
            elif stat == "PC":
                PC = result
            elif stat == "LA":
                LA = result
            else:
                RA = result
        try:
            LA_LR = round(LA / LR, 2) * 100
            PC_LR = round(PC / LR, 2) * 100
            RA_LR = round(RA / LR, 2) * 100
        except ZeroDivisionError:
            LA_LR = 0
            PC_LR = 0
            RA_LR = 0
        df.at[idx, "PC_LR %"] = PC_LR
        df.at[idx, "LA_LR %"] = LA_LR
        df.at[idx, "RA_LR %"] = RA_LR

    return df
