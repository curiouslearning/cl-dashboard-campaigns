import streamlit as st
import settings
from rich import print as rprint
import metrics
import datetime as dt
from millify import prettify

settings.initialize()
settings.init_campaign_data()
settings.init_user_list()

default_daterange = [dt.datetime(2024, 9, 9).date(), dt.date.today()]

df_campaign_ids = st.session_state["df_campaign_ids"]
df_campaign_users = st.session_state["df_campaign_users"]
df_campaigns_rollup = st.session_state["df_campaigns_rollup"]

sources = df_campaign_ids.source.unique()
col1, col2 = st.columns([1,4])
selected_source = col1.selectbox(label="Source",options=sources,index=None)
df_users_filtered = df_campaign_users.loc[df_campaign_users['source'] == selected_source]

LR = len(df_users_filtered)
col1.metric(label="Learners Reached", value=prettify(int(LR)))

st.dataframe(df_users_filtered)  