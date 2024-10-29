import streamlit as st
import settings
from rich import print as rprint

settings.initialize()
settings.init_campaign_data()
settings.init_user_list()

df_campaign_ids = st.session_state["df_campaign_ids"]
df_campaign_users = st.session_state["df_campaign_users"]
df_campaigns_rollup = st.session_state["df_campaigns_rollup"]

sources = df_campaign_ids.source.unique()
col1, col2 = st.columns([1,4])
col1.selectbox(label="Source",options=sources,index=None)

st.dataframe(df_campaign_users)
st.dataframe(df_campaigns_rollup)  