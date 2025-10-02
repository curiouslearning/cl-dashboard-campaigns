from users import ensure_user_data_initialized
import streamlit as st
from campaigns import rollup_campaign_data
from millify import prettify
import ui_widgets as ui
import users
import numpy as np
from ui_components import create_funnels_by_cohort, unattributed_events_line_chart, country_pie_chart
from settings import default_daterange, init_data, initialize
from metrics import (
    get_filtered_cohort,
    filter_dataframe,
    get_cohort_totals_by_metric
)


# --- INIT ---
initialize()
init_data()
ensure_user_data_initialized()

st.markdown(
    """
    :blue-background[NOTE:]
    :orange[ Campaign data added to events starting 11-08-2024.  This dashboard only reflects data from that date forward]
    """
)

# --- Load data ---
campaign_users_app_launch = st.session_state["campaign_users_app_launch"]
df_campaigns_all = st.session_state["df_campaigns_all"]
df_campaigns_rollup = rollup_campaign_data(df_campaigns_all)
df_campaign_names = df_campaigns_rollup[['campaign_id', 'campaign_name']]
df_unattributed_app_launch_events = st.session_state["df_unattributed_app_launch_events"]

# --- Layout columns ---
col1, col2, col3 = st.columns([1, 1, 1], gap="large")

with col1:
    st.subheader("Source Cohort")

    df = campaign_users_app_launch
    source_ids = sorted(df["source_id"].dropna().unique())
    source_ids = [None] + source_ids  # "All sources" option

    selected_source = st.selectbox(
        "Select a Source",
        options=source_ids,
        format_func=lambda x: "All sources" if x is None else str(x),
        key="home-1",
        index=0
    )

    # --- Find campaigns for selected source (or all) ---
    if selected_source is None:
        filtered_df = df
    else:
        filtered_df = df[df["source_id"] == selected_source]
    campaign_ids = sorted(filtered_df["campaign_id"].dropna().unique())

    campaign_options = (
        df_campaign_names[df_campaign_names["campaign_id"].isin(campaign_ids)]
        .drop_duplicates(subset=["campaign_id"])
        .sort_values("campaign_name")
    )
    campaign_display = [
        f"{row['campaign_name']} ({row['campaign_id']})"
        for _, row in campaign_options.iterrows()
    ]
    campaign_id_lookup = dict(
        zip(campaign_display, campaign_options["campaign_id"]))
    campaign_display = ["All campaigns"] + campaign_display

    selected_campaign_display = st.selectbox(
        "Select a Campaign",
        options=campaign_display,
        key="campaign-selectbox",
        index=0
    )
    selected_campaign_id = None if selected_campaign_display == "All campaigns" else campaign_id_lookup[
        selected_campaign_display]

    # Date range selection
    st.write("Date subset")
    selected_date, option = ui.calendar_selector(
        key="fa-3", index=0, placement="middle")
    daterange = ui.convert_date_to_range(selected_date, option)
    if daterange[0] < default_daterange[0]:
        daterange[0] = default_daterange[0]

with col2:
    st.subheader("")
    # Language and country selection
    languages = users.get_language_list()
    language = ui.single_selector(
        languages, title="Select a language", key="a-2", placement="middle"
    )
    countries_list = users.get_country_list()
    countries_list = ui.multi_select_all(
        countries_list,
        title="Country Selection",
        key="LA_LR_Time",
        placement="middle",
    )
    if not countries_list:
        countries_list = ["All"]

if len(daterange) == 2:
    # --- Main filtering: get both user_cohort_df and user_cohort_df_LR with all filters ---
    user_cohort_df, user_cohort_df_LR = get_filtered_cohort(
        daterange,
        language,
        countries_list,
        source_id=selected_source,
        campaign_id=selected_campaign_id
    )

    LR = get_cohort_totals_by_metric(user_cohort_df_LR, stat="LR")
    LA = get_cohort_totals_by_metric(user_cohort_df, stat="LA")

    with col3:
        st.subheader("")
        st.metric(label="Learners Reached", value=prettify(int(LR)))
        st.metric(label="Learners Acquired", value=prettify(int(LA)))

    df_users_filtered = (
        user_cohort_df_LR.groupby("campaign_id")
        .agg({
            'country': 'first',
            'source_id': 'first',
            'app_language': 'first',
            'user_pseudo_id': 'size'
        })
        .rename(columns={'user_pseudo_id': 'LR'})
        .reset_index()
    )

    # --- Filter and roll up campaign cost data for the date range ---
    segment_dates = df_campaigns_all["segment_date"]
    if np.issubdtype(segment_dates.dtype, np.datetime64):
        segment_dates = segment_dates.dt.date
    campaign_mask = (segment_dates >= daterange[0]) & (segment_dates <= daterange[1])
    df_campaigns_filtered = df_campaigns_all.loc[campaign_mask]
    df_campaigns_rollup = rollup_campaign_data(df_campaigns_filtered)

    # --- Merge for table display ---
    df_table = (
        df_users_filtered
        .merge(df_campaigns_rollup, on="campaign_id", how="left", suffixes=('', '_campaign'))
        .merge(df_campaign_names, on='campaign_id', how='left', suffixes=('', '_lookup'))
    )

    # Fill missing values in key columns and drop the unnecessary columns
    for col in ['country', 'source_id', 'app_language']:
        campaign_col = f"{col}_campaign"
        if campaign_col in df_table:
            df_table[col] = df_table[col].combine_first(df_table[campaign_col])
    if "campaign_name_lookup" in df_table:
        df_table["campaign_name"] = df_table["campaign_name"].fillna(
            df_table["campaign_name_lookup"])

    df_table = df_table.drop(columns=[c for c in [
                             'country_campaign', 'source_id_campaign', 'app_language_campaign', 'campaign_name_lookup'] if c in df_table])

    tab1, tab2, tab3 = st.tabs(
        ["Data Table", "CR Funnel", "Unattributed Events"])
    with tab1:
        st.header("Data Table")
        if not df_table.empty:
            df_table["LRC"] = np.where(
                df_table["LR"] != 0, (df_table["cost"] / df_table["LR"]).round(2), 0)
            final_columns = ['campaign_id', 'source_id', 'campaign_name',
                             'LR', "cost", "LRC", "country", "app_language"]
            final_columns = [c for c in final_columns if c in df_table]
            df_table = df_table[final_columns]
            ui.paginated_dataframe(df_table, keys=[1, 2, 3, 4, 5])
        else:
            st.write("No data")

    st.divider()
    with tab2:
        st.header("Curious Reader Funnel")
        create_funnels_by_cohort(
            cohort_df=user_cohort_df,
            key_prefix="123",
            funnel_size="medium",
            cohort_df_LR=user_cohort_df_LR,
        )
    with tab3:
        attributed_df = campaign_users_app_launch
        unattributed_df = df_unattributed_app_launch_events

        attributed_df = filter_dataframe(
            df=attributed_df,
            countries_list=countries_list,
            daterange=daterange,
            language=language,
            source_id=selected_source
        )
        unattributed_df = filter_dataframe(
            df=unattributed_df,
            countries_list=countries_list,
            daterange=daterange,
            language=language
        )

        count = len(unattributed_df)
        st.header(f"Unattributed Learners Reached: {count}")
        unattributed_events_line_chart(
            unattributed_df=unattributed_df, attributed_df=attributed_df)
        country_pie_chart(unattributed_df)
