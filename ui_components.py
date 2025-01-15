import streamlit as st
import datetime as dt
from rich import print
import plotly.graph_objects as go
import metrics
from millify import prettify
import ui_widgets as ui
import pandas as pd


default_daterange = [dt.datetime(2024, 11, 8).date(), dt.date.today()]


def create_engagement_figure(funnel_data=[], key=""):

    fig = go.Figure(
        go.Funnel(
            y=funnel_data["Title"],
            x=funnel_data["Count"],
            textposition="auto",
            #          textinfo="value+percent initial+percent previous",
            hoverinfo="x+y+text+percent initial+percent previous",
            #           key=key,
            marker={
                "color": [
                    "#4F420A",
                    "#73600F",
                    "#947C13",
                    "#E0BD1D",
                    "#B59818",
                    "#D9B61C",
                ],
                "line": {
                    "width": [4, 3, 2, 2, 2, 1],
                    "color": ["wheat", "wheat", "wheat", "wheat"],
                },
            },
            connector={"line": {"color": "#4F3809", "dash": "dot", "width": 3}},
        )
    )
    fig.update_traces(texttemplate="%{value:,d}")
    fig.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
    )

    return fig


#Added user_list which is a list of cr_user_id to filter with
@st.cache_data(ttl="1d", show_spinner="Building funnel")
def create_funnels(selected_source,
    countries_list,
                   daterange,
                   languages,
                   key_prefix,
                   displayLR=True,
                   user_list=[],
                   display_FO=True):
    
    statsA = ["FO", "LR","DC", "TS","SL",  "PC", "LA", "RA" ,"GC",]
    statsB = ["LR","DC", "TS","SL",  "PC", "LA", "RA" ,"GC",]
    statsC = ["DC", "TS","SL",  "PC", "LA", "RA" ,"GC",]
    titlesA = ["First Open",
            "Learner Reached (app_launch)", "Download Completed", "Tapped Start", 
            "Selected Level", "Puzzle Completed", "Learners Acquired", "Readers Acquired", "Game Completed"
        ]
    titlesB = [
            "Learner Reached (app_launch)", "Download Completed", "Tapped Start", 
            "Selected Level", "Puzzle Completed", "Learners Acquired", "Readers Acquired", "Game Completed"
        ]
    titlesC = ["Download Completed", "Tapped Start", 
            "Selected Level", "Puzzle Completed", "Learners Acquired", "Readers Acquired", "Game Completed"
        ]


    stats = statsA
    titles = titlesA

    if languages[0]  != 'All' or display_FO == False:
        stats = statsB
        titles = titlesB


    if len(daterange) == 2:
        start = daterange[0].strftime("%b %d, %Y")
        end = daterange[1].strftime("%b %d, %Y")
        st.caption(start + " to " + end)

        metrics_data = {}
        for stat in stats:
            metrics_data[stat] = metrics.get_totals_by_metric(source_id=selected_source,
                daterange=daterange,
                stat=stat,
                language=languages,
                countries_list=countries_list,
                user_list=user_list
            )
        
        # If a specific app version is selected, we don't have LR data, so this is a way to not show it
        # The reason we don't use app_version directly is because if we are comparing funnels, if one uses it
        # we want the other to exclude that level as well.
        if displayLR:
            funnel_data = {
                "Title": titles,
                "Count": [metrics_data[stat] for stat in stats]
            }
        else:
            stats = statsC 
            titles = titlesC
            funnel_data = {
                "Title": titles,
                "Count": [metrics_data[stat] for stat in stats],
            }

        fig = create_engagement_figure(funnel_data, key=f"{key_prefix}-5")
        st.plotly_chart(fig, use_container_width=True,key=f"{key_prefix}-6")


@st.cache_data(ttl="1d", show_spinner=False)
def unattributed_events_line_chart(unattributed_df,
                                   attributed_df,
                                   daterange=default_daterange,
                                   countries_list=["All"],
                                   language=["All"],
                                   source_id=None):


    #Group and format 
    attributed_df = attributed_df.groupby(
        'event_date').size().reset_index(name='event_count')
    
    unattributed_df = unattributed_df.groupby(
        'event_date').size().reset_index(name='event_count')


    # Format event_count with commas for hover text
    unattributed_df["formatted_event_count"] = unattributed_df["event_count"].apply(lambda x: f"{
                                                                       x:,}")
    attributed_df["formatted_event_count"] = attributed_df["event_count"].apply(
        lambda x: f"{x:,}")

    # Create the figure
    fig = go.Figure()

    # Unattributed trace
    fig.add_trace(go.Scatter(
        x=unattributed_df["event_date"],
        y=unattributed_df["event_count"],
        mode="lines+markers",
        name="Unattributed Count per Day",
        text=unattributed_df.apply(
            lambda row: f"Date: {row['event_date'].strftime(
                '%Y-%m-%d')}<br>Event Count: {row['formatted_event_count']}",
            axis=1),
        hoverinfo="text"
    ))

    # Attributed trace
    fig.add_trace(go.Scatter(
        x=attributed_df["event_date"],
        y=attributed_df["event_count"],
        mode="lines+markers",
        name="Attributed Count per Day",
        text=attributed_df.apply(
            lambda row: f"Date: {row['event_date'].strftime(
                '%Y-%m-%d')}<br>Event Count: {row['formatted_event_count']}",
            axis=1),
        hoverinfo="text"
    ))

    # Customize layout
    fig.update_layout(
        title="Number of Unique Events Per Day",
        xaxis_title="Day",
        yaxis_title="Number of Events",
        xaxis=dict(type="date"),  # Ensure the x-axis is treated as a date
        template="plotly"
    )

    # Display the chart
    st.plotly_chart(fig, use_container_width=True)
