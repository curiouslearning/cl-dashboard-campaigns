import streamlit as st
import settings
from rich import print as rprint
import campaigns
import datetime as dt
from millify import prettify
import ui_widgets as ui
import users
import numpy as np

settings.initialize()
settings.init_data()

# Define default date range
default_daterange = [dt.datetime(2024, 9, 9).date(), dt.date.today()]

# Load data from session state
df_campaign_users = st.session_state["df_campaign_users"]
df_campaigns_all = st.session_state["df_campaigns_all"]

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

    # Language selection
    languages = users.get_language_list()
    language = ui.single_selector(
        languages, title="Select a language", key="a-2", placement="middle"
    )

    # Date range selection
    selected_date, option = ui.calendar_selector(key="fa-3", index=1, placement="middle")
    daterange = ui.convert_date_to_range(selected_date, option)

# Define query conditions for campaign users
user_conditions = [f"@daterange[0] <= event_date <= @daterange[1]"]
if countries_list[0] != "All":
    user_conditions.append("country.isin(@countries_list)")
if language[0] != "All":
    user_conditions.append("app_language == @language")

# Apply query to filter campaign users
user_query = " and ".join(user_conditions)
df_users_filtered = df_campaign_users.query(user_query)
LR = len(df_users_filtered)

# Display Learners Reached metric
col1.metric(label="Learners Reached", value=prettify(int(LR)))

# Group filtered users by campaign
df_users_filtered = df_users_filtered.groupby("campaign_id").size().reset_index(name="LR")

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
df_table = df_users_filtered.merge(df_campaigns_rollup, on="campaign_id", how="left")
if not df_table.empty:
    df_table["LRC"] = np.where(df_table["LR"] != 0, (df_table["cost"] / df_table["LR"]).round(2), 0)
    new_order = ['campaign_id', 'campaign_name', 'LR', "cost","LRC","country","app_language"]
    df_table = df_table[new_order]
    ui.paginated_dataframe(df_table, keys=[1, 2, 3, 4, 5])
else:
    st.write("No data")
