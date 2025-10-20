import altair as alt
import duckdb
import numpy as np
import pandas as pd
import streamlit as st
from get_data import get_star_wars_data

# Title of the Streamlit app
st.title("Star Wars Data Explorer")

# Sidebar
st.sidebar.image("visualisation/assets/star_wars_sign.png")
st.sidebar.subheader("PyConDE 2025")
page = st.sidebar.radio("Choose a Page", ["Financials", "Attributes"])

# Get data from duckdb
film_df = get_star_wars_data("prs_film")
film_df.sort_values(by="release_date", ascending=True, inplace=True)
film_df["return"] = film_df["box_office"] / film_df["budget"]

counts_df = get_star_wars_data("prs_film_attributes")
counts_df.replace(0, np.nan, inplace=True)
film_df = film_df.join(counts_df, how="left", rsuffix="_counts").drop(
    ["episode_number_counts", "title_counts", "release_date_counts"], axis=1
)

# Add a chart to plot the data
if page == "Financials":
    st.header("💰 Film Financials")
    col1, col2 = st.columns(2)

    # Sort data by release_date
    chart_data = film_df[
        ["title", "budget", "release_date", "box_office", "return"]
    ].sort_values(by="release_date", ascending=True)

    # Create Altair chart with explicit ordering
    with col1:
        st.subheader("Film Budget Chart")
        budget_chart = (
            alt.Chart(chart_data)
            .mark_bar()
            .encode(
                x=alt.X(
                    "title:N", sort=list(chart_data["title"]), title="Star Wars Films"
                ),
                y=alt.Y(
                    "budget:Q",
                    title="Budget in Millions USD",
                    scale=alt.Scale(domain=[0, 2000]),
                ),
                tooltip=["title", "budget"],
            )
            .properties(height=400)
        )

        st.altair_chart(budget_chart, use_container_width=True)

    # Create Altair chart with explicit ordering
    with col2:
        st.subheader("Film Earnings Chart")
        earnings_chart = (
            alt.Chart(chart_data)
            .mark_bar(color="gold")
            .encode(
                x=alt.X(
                    "title:N", sort=list(chart_data["title"]), title="Star Wars Films"
                ),
                y=alt.Y(
                    "box_office:Q",
                    title="Box Office Revenue in Millions USD",
                ),
                tooltip=["title", "box_office"],
            )
            .properties(height=400)
        )

        st.altair_chart(earnings_chart, use_container_width=True)

    # Create Altair chart with explicit ordering
    st.subheader("Film Returns Chart")
    earnings_chart = (
        alt.Chart(chart_data)
        .mark_line(color="red")
        .encode(
            x=alt.X("title:N", sort=list(chart_data["title"]), title="Star Wars Films"),
            y=alt.Y("return:Q", title="Returns (Box Office / Budget)"),
            tooltip=["title", "return"],
        )
        .properties(height=400)
    )

    st.altair_chart(earnings_chart, use_container_width=True)

elif page == "Attributes":
    st.header("🪐 Film Attributes")
    budget_df = film_df[
        [
            "episode_number",
            "title",
            "budget",
            "species_count",
            "character_count",
            "planet_count",
            "starship_count",
        ]
    ]
    budget_df.dropna(inplace=True)

    count_df = pd.melt(
        budget_df,
        id_vars=["title"],
        value_vars=[
            "species_count",
            "character_count",
            "planet_count",
            "starship_count",
        ],
        var_name="attribute",
        value_name="count",
    )

    budget_df["species_ratio"] = budget_df["species_count"] / budget_df["budget"]
    budget_df["character_ratio"] = budget_df["character_count"] / budget_df["budget"]
    budget_df["planet_ratio"] = budget_df["planet_count"] / budget_df["budget"]
    budget_df["starship_ratio"] = budget_df["starship_count"] / budget_df["budget"]

    budget_df = pd.melt(
        budget_df,
        id_vars=["title"],
        value_vars=[
            "species_ratio",
            "character_ratio",
            "planet_ratio",
            "starship_ratio",
        ],
        var_name="attribute",
        value_name="ratio",
    )

    st.subheader("Budget Impact on Attribute Counts")
    counts_chart = (
        alt.Chart(count_df)
        .mark_line()
        .encode(
            x=alt.X("title:N", sort=list(counts_df["title"]), title="Star Wars Films"),
            y=alt.Y("count:Q", title="Resource to Budget Ratio"),
            color=alt.Color("attribute:N", title="Attribute"),
            tooltip=["title", "attribute", "count"],
        )
        .properties(height=400)
    )
    st.altair_chart(counts_chart, use_container_width=True)

    # Create Altair chart with explicit ordering
    st.subheader("Budget Impact Attribute Ratios")
    attributes_chart = (
        alt.Chart(budget_df)
        .mark_line()
        .encode(
            x=alt.X("title:N", sort=list(budget_df["title"]), title="Star Wars Films"),
            y=alt.Y("ratio:Q", title="Resource to Budget Ratio"),
            color=alt.Color("attribute:N", title="Attribute"),
            tooltip=["title", "attribute", "ratio"],
        )
        .properties(height=400)
    )

    st.altair_chart(attributes_chart, use_container_width=True)
st.header("👩🏻‍💻 The Data")
st.dataframe(film_df)
col1, col2, col3 = st.columns([1, 2, 1])
with col3:
    st.image("visualisation/assets/wookie.webp", width=200)
