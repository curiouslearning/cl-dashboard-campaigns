
import streamlit as st
from rich import print
import plotly.graph_objects as go
from millify import prettify
from metrics import get_cohort_totals_by_metric


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


@st.cache_data(ttl="1d", show_spinner=False)
def unattributed_events_line_chart(unattributed_df,
                                   attributed_df,
                                   ):


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
    

def country_pie_chart(df):

# Grouping and counting entries for each country
    grouped_df = df.groupby(
    'country').size().reset_index(name='count')

# Calculating the total number of entries
    total_count = grouped_df['count'].sum()

# Filtering out entries with less than 1% of the total
    grouped_df = grouped_df[grouped_df['count'] / total_count >= 0.01]

# Creating a pie chart with Plotly Graph Objects
    fig = go.Figure(
    go.Pie(
        labels=grouped_df['country'],
        values=grouped_df['count'],
        title='Entries by Country',
        textinfo='label+percent',  # Show country names and percentage on the slices
        hole=0.3 , # Optional: for a donut chart effect
    )
)
    fig.update_layout(
    title='Entries by Country',
    width=600,  # Set the custom width (in pixels)
    height=600,  # Set the custom height (in pixels)
)

    st.plotly_chart(fig, use_container_width=True)
    

def create_funnels_by_cohort(
    cohort_df,
    key_prefix="",
    funnel_size="medium",
    cohort_df_LR=None,
):

    stats = ["LR", "DC", "TS", "SL", "PC", "LA", "RA", "GC"]
    titles = [
        "Learner Reached", "Download Completed", "Tapped Start",
        "Selected Level", "Puzzle Completed", "Learners Acquired", "Readers Acquired", "Game Completed"
    ]

    user_key = "cr_user_id"

    funnel_step_counts = []
    for stat in stats:
        if stat == "LR":
            count = (
                cohort_df_LR[user_key].nunique()
                if cohort_df_LR is not None and user_key in cohort_df_LR.columns
                else cohort_df[user_key].nunique()
            )
        else:
            count = get_cohort_totals_by_metric(cohort_df, stat=stat)
        funnel_step_counts.append(count)

    # --- Percentages ---
    percent_of_previous = [None]
    for i in range(1, len(funnel_step_counts)):
        prev = funnel_step_counts[i-1]
        curr = funnel_step_counts[i]
        percent = round(100 * curr / prev, 1) if prev and prev > 0 else None
        percent_of_previous.append(percent)

    percent_of_second = [None, None]
    if len(funnel_step_counts) >= 2 and funnel_step_counts[1]:
        for i in range(2, len(funnel_step_counts)):
            second = funnel_step_counts[1]
            curr = funnel_step_counts[i]
            percent = round(100 * curr / second,
                            1) if second and second > 0 else None
            percent_of_second.append(percent)
    else:
        percent_of_second += [None] * (len(funnel_step_counts) - 2)

    funnel_data = {
        "Title": titles,
        "Count": funnel_step_counts,
        "PercentOfPrevious": percent_of_previous,
        "PercentOfSecond": percent_of_second,
    }

    # Render with your existing function
    fig = create_engagement_figure(
        funnel_data,
        key=f"{key_prefix}-5",
    )

    st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}-6")
