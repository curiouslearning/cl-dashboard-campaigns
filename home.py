import streamlit as st
import settings
from rich import print as rprint
import campaigns
import datetime as dt
from millify import prettify
import ui_widgets as ui
import users
import numpy as np
import metrics

settings.initialize()
settings.init_data()

# Define default date range
default_daterange = [dt.datetime(2024, 9, 9).date(), dt.date.today()]

# Load data from session state
campaign_users_app_launch = st.session_state["campaign_users_app_launch"]
df_campaigns_all = st.session_state["df_campaigns_all"]
df_campaigns_rollup = campaigns.rollup_campaign_data(df_campaigns_all)
df_campaign_names = df_campaigns_rollup[['campaign_id', 'campaign_name']]

# Define layout columns
col1, col2 = st.columns(2, gap="large")

with col1:
    # Country selection
    countries_list = users.get_country_list()
    countries_list = ui.multi_select_all(
        countries_list,
        title="Country Selection",
        key="LA_LR_Time",
        placement="middle",
    )
    if not countries_list:  # If the user unselects all, default to 'All'
        countries_list = ["All"]
with col2:
    # Language selection
    languages = users.get_language_list()
    language = ui.single_selector(
        languages, title="Select a language", key="a-2", placement="middle"
    )

    # Date range selection
    selected_date, option = ui.calendar_selector(key="fa-3", index=1, placement="middle")
    daterange = ui.convert_date_to_range(selected_date, option)

    source_ids = campaign_users_app_launch.source_id.unique()
    selected_source = col1.selectbox(
        label="Select a Source", options=source_ids, index=None)

df_users_filtered = metrics.filter_user_data(daterange=daterange,countries_list=countries_list,stat="LR",language=language,source_id=selected_source)

LR = metrics.get_totals_by_metric(
    daterange, countries_list, stat="LR",  language=language, source_id=selected_source
)

LA = metrics.get_totals_by_metric(
    daterange, countries_list, stat="LA",  language=language, source_id=selected_source
)


col1.metric(label="Learners Reached", value=prettify(int(LR)))
col2.metric(label="Learners Acquired", value=prettify(int(LA)))

# Group filtered users by campaign
df_users_filtered = df_users_filtered.groupby("campaign_id").agg({
    'country': 'first',
    'app_language': 'first',
    'user_pseudo_id': 'size'  # the column that represents the count for "LR"
}).rename(columns={'user_pseudo_id': 'LR'}).reset_index()

# Define query conditions for campaigns
campaign_conditions = [f"@daterange[0] <= segment_date <= @daterange[1]"]
if countries_list[0] != "All":
    campaign_conditions.append("country.isin(@countries_list)")
if language[0] != "All":
    campaign_conditions.append("app_language == @language")

# Apply query to filter campaigns
campaign_query = " and ".join(campaign_conditions)
df_campaigns_filtered = df_campaigns_all.query(campaign_query)

# Roll up campaign data and merge with filtered users
df_campaigns_rollup = campaigns.rollup_campaign_data(df_campaigns_filtered)

# Merge with users and campaigns rollup data
df_table = (
    df_users_filtered
    .merge(df_campaigns_rollup, on="campaign_id", how="left", suffixes=('', '_campaign'))
    .merge(df_campaign_names, on='campaign_id', how='left', suffixes=('', '_lookup'))
)

# Fill missing values in key columns and drop the unnecessary columns
df_table['country'] = df_table['country'].combine_first(df_table['country_campaign'])
df_table['app_language'] = df_table['app_language'].combine_first(df_table['app_language_campaign'])
df_table['campaign_name'] = df_table['campaign_name'].fillna(df_table['campaign_name_lookup'])

# Drop extra columns after filling values
df_table = df_table.drop(columns=['country_campaign', 'app_language_campaign', 'campaign_name_lookup'])

# Calculate 'LRC' if table is not empty
if not df_table.empty:
    df_table["LRC"] = np.where(df_table["LR"] != 0, (df_table["cost"] / df_table["LR"]).round(2), 0)
    
    # Reorder columns for final output
    final_columns = ['campaign_id', 'campaign_name', 'LR', "cost", "LRC", "country", "app_language"]
    df_table = df_table[final_columns]
    
    # Display the paginated dataframe
    ui.paginated_dataframe(df_table, keys=[1, 2, 3, 4, 5])
else:
    st.write("No data")

