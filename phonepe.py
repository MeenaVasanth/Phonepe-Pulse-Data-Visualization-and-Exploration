import json
import os
import streamlit as st
import pandas as pd
import psycopg2
import requests
import plotly.express as px
from streamlit_option_menu import option_menu
from streamlit_extras.let_it_rain import rain

# PostgreSQL connection
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="malathi03",
    database="phonepe_data",
    port="5432"
)
cursor = mydb.cursor()

# Function to fetch data from a table and convert to DataFrame
def fetch_data_to_dataframe(query, columns):
    cursor.execute(query)
    mydb.commit()
    data = cursor.fetchall()
    return pd.DataFrame(data, columns=columns)


# Fetch data into DataFrames
Aggre_insurance = fetch_data_to_dataframe("SELECT * FROM aggregated_insurance;", ["States", "Years", "Quarter", "Insurance_type", "Insurance_count", "Insurance_amount"])
Aggre_transaction = fetch_data_to_dataframe("SELECT * FROM aggregated_transaction;", ["States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"])
Aggre_user = fetch_data_to_dataframe("SELECT * FROM aggregated_user;", ["States", "Years", "Quarter", "Brands", "Transaction_count", "Percentage"])
Map_insurance = fetch_data_to_dataframe("SELECT * FROM map_insurance;", ["States", "Years", "Quarter", "District", "Transaction_count", "Transaction_amount"])
Map_transaction = fetch_data_to_dataframe("SELECT * FROM map_transaction;", ["States", "Years", "Quarter", "District", "Transaction_count", "Transaction_amount"])
Map_user = fetch_data_to_dataframe("SELECT * FROM map_user;", ["States", "Years", "Quarter", "Districts", "RegisteredUser", "AppOpens"])
Top_insurance = fetch_data_to_dataframe("SELECT * FROM top_insurance;", ["States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"])
Top_transaction = fetch_data_to_dataframe("SELECT * FROM top_transaction;", ["States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"])
Top_user = fetch_data_to_dataframe("SELECT * FROM top_user;", ["States", "Years", "Quarter", "Pincodes", "RegisteredUser"])

def Aggre_transaction_Y(df, year):
    # Filter the dataframe for the selected year
    aiy = df[df["Years"] == year]
    aiy.reset_index(drop=True, inplace=True)

    # Group by 'States' and aggregate 'Transaction_count' and 'Transaction_amount' using sum
    aiyg = aiy.groupby("States")[["Transaction_count", "Transaction_amount"]].sum()
    aiyg.reset_index(inplace=True)

    # Create two columns for side-by-side plots
    col1, col2 = st.columns(2)

    with col1:
        # Plot the total transaction amount per state as a bar chart
        fig_amount = px.bar(aiyg, x="States", y="Transaction_amount", 
                           title=f"{year} TRANSACTION AMOUNT",
                           width=600, height=650, color_discrete_sequence=px.colors.sequential.Agsunset)
        st.plotly_chart(fig_amount)

    with col2:
        # Plot the total transaction count per state as a bar chart
        fig_count = px.bar(aiyg, x="States", y="Transaction_count", 
                          title=f"{year} TRANSACTION COUNT",
                          width=600, height=650, color_discrete_sequence=px.colors.sequential.Agsunset)
        st.plotly_chart(fig_count)

    # Create two more columns for side-by-side maps
    col1, col2 = st.columns(2)

    with col1:
        # Load the GeoJSON data for Indian states
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)

        # Extract and sort the state names from the GeoJSON data
        states_name_tra = [feature["properties"]["ST_NM"] for feature in data1["features"]]
        states_name_tra.sort()

        # Create a choropleth map for transaction amount by state
        fig_india_1 = px.choropleth(aiyg, geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                    color="Transaction_amount", color_continuous_scale="Sunset",
                                    range_color=(aiyg["Transaction_amount"].min(), aiyg["Transaction_amount"].max()),
                                    hover_name="States", title=f"{year} TRANSACTION AMOUNT",
                                    fitbounds="locations", width=600, height=600)
        fig_india_1.update_geos(visible=False)
        st.plotly_chart(fig_india_1)

    with col2:
        # Create a choropleth map for transaction count by state
        fig_india_2 = px.choropleth(aiyg, geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                    color="Transaction_count", color_continuous_scale="Sunset",
                                    range_color=(aiyg["Transaction_count"].min(), aiyg["Transaction_count"].max()),
                                    hover_name="States", title=f"{year} TRANSACTION COUNT",
                                    fitbounds="locations", width=600, height=600)
        fig_india_2.update_geos(visible=False)
        st.plotly_chart(fig_india_2)

    return aiy


def Aggre_transaction_Y_Q(df, quarter):
    # Filter the dataframe for the selected quarter
    aiyq = df[df["Quarter"] == quarter]
    aiyq.reset_index(drop=True, inplace=True)

    # Group by 'States' and aggregate 'Transaction_count' and 'Transaction_amount' using sum
    aiyqg = aiyq.groupby("States")[["Transaction_count", "Transaction_amount"]].sum()
    aiyqg.reset_index(inplace=True)

    # Create two columns for side-by-side plots
    col1, col2 = st.columns(2)

    with col1:
        # Plot the total transaction amount per state as a bar chart
        fig_q_amount = px.bar(aiyqg, x="States", y="Transaction_amount", 
                             title=f"{aiyq['Years'].min()} AND {quarter} TRANSACTION AMOUNT",
                             width=600, height=650, color_discrete_sequence=px.colors.sequential.Agsunset)
        st.plotly_chart(fig_q_amount)

    with col2:
        # Plot the total transaction count per state as a bar chart
        fig_q_count = px.bar(aiyqg, x="States", y="Transaction_count", 
                            title=f"{aiyq['Years'].min()} AND {quarter} TRANSACTION COUNT",
                            width=600, height=650, color_discrete_sequence=px.colors.sequential.Agsunset)
        st.plotly_chart(fig_q_count)

    # Create two more columns for side-by-side maps
    col1, col2 = st.columns(2)

    with col1:
        # Load the GeoJSON data for Indian states
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)

        # Extract and sort the state names from the GeoJSON data
        states_name_tra = [feature["properties"]["ST_NM"] for feature in data1["features"]]
        states_name_tra.sort()

        # Create a choropleth map for transaction amount by state
        fig_india_1 = px.choropleth(aiyqg, geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                    color="Transaction_amount", color_continuous_scale="Sunset",
                                    range_color=(aiyqg["Transaction_amount"].min(), aiyqg["Transaction_amount"].max()),
                                    hover_name="States", title=f"{aiyq['Years'].min()} AND {quarter} TRANSACTION AMOUNT",
                                    fitbounds="locations", width=600, height=600)
        fig_india_1.update_geos(visible=False)
        st.plotly_chart(fig_india_1)

    with col2:
        # Create a choropleth map for transaction count by state
        fig_india_2 = px.choropleth(aiyqg, geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                    color="Transaction_count", color_continuous_scale="Sunset",
                                    range_color=(aiyqg["Transaction_count"].min(), aiyqg["Transaction_count"].max()),
                                    hover_name="States", title=f"{aiyq['Years'].min()} AND {quarter} TRANSACTION COUNT",
                                    fitbounds="locations", width=600, height=600)
        fig_india_2.update_geos(visible=False)
        st.plotly_chart(fig_india_2)

    return aiyq


def Aggre_Transaction_type(df, state):
    # Filter the dataframe for the selected state
    df_state = df[df["States"] == state]
    df_state.reset_index(drop=True, inplace=True)

    # Group by 'Transaction_type' and aggregate 'Transaction_count' and 'Transaction_amount' using sum
    agttg = df_state.groupby("Transaction_type")[["Transaction_count", "Transaction_amount"]].sum()
    agttg.reset_index(inplace=True)

    # Create two columns for side-by-side plots
    col1, col2 = st.columns(2)

    with col1:
        # Plot a horizontal bar chart for transaction count by transaction type
        fig_hbar_1 = px.bar(agttg, x="Transaction_count", y="Transaction_type", orientation="h",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, width=600,
                            title=f"{state.upper()} TRANSACTION TYPES AND TRANSACTION COUNT", height=500)
        st.plotly_chart(fig_hbar_1)

    with col2:
        # Plot a horizontal bar chart for transaction amount by transaction type
        fig_hbar_2 = px.bar(agttg, x="Transaction_amount", y="Transaction_type", orientation="h",
                            color_discrete_sequence=px.colors.sequential.Greens_r, width=600,
                            title=f"{state.upper()} TRANSACTION TYPES AND TRANSACTION AMOUNT", height=500)
        st.plotly_chart(fig_hbar_2)
        

def top_transaction_plot_year(df_top_tran_Y):
    # Group by states and calculate the sum of transactions
    df_grouped = df_top_tran_Y.groupby("States")["Transaction_amount"].sum().reset_index()

    # Plotting
    fig1 = px.bar(df_grouped, x="States", y="Transaction_amount", 
                  title="Top Transactions by State (Yearly)", 
                  color_discrete_sequence=px.colors.sequential.Plasma)
    
    st.plotly_chart(fig1)


def top_transaction_plot_quarter(df_top_tran_Y_Q):
    # Group by states and calculate the sum of transactions
    df_grouped = df_top_tran_Y_Q.groupby("States")["Transaction_amount"].sum().reset_index()

    # Plotting
    fig2 = px.bar(df_grouped, x="States", y="Transaction_amount", 
                  title="Top Transactions by State (Quarterly)", 
                  color_discrete_sequence=px.colors.sequential.Viridis)
    
    st.plotly_chart(fig2)


def Aggre_user_plot_1(df, year):
    # Filter the dataframe for the selected year
    aguy = df[df["Years"] == year]
    aguy.reset_index(drop=True, inplace=True)

    # Group by 'Brands' and aggregate 'Transaction_count' using sum
    aguyg = pd.DataFrame(aguy.groupby("Brands")["Transaction_count"].sum())
    aguyg.reset_index(inplace=True)

    # Plot a bar chart for transaction count by brand
    fig_line_1 = px.bar(aguyg, x="Brands", y="Transaction_count", 
                        title=f"{year} BRANDS AND TRANSACTION COUNT",
                        width=1000, color_discrete_sequence=px.colors.sequential.haline_r)
    st.plotly_chart(fig_line_1)

    return aguy


def Aggre_user_plot_2(df, quarter):
    # Filter the dataframe for the selected quarter
    auqs = df[df["Quarter"] == quarter]
    auqs.reset_index(drop=True, inplace=True)

    # Plot a pie chart for the transaction count percentage by brand
    fig_pie_1 = px.pie(
        data_frame=auqs, 
        names="Brands", 
        values="Transaction_count", 
        hover_data="Percentage",  # Additional data to show on hover
        width=1000, 
        title=f"{quarter} QUARTER TRANSACTION COUNT PERCENTAGE", 
        hole=0.5,  # Creates a donut chart
        color_discrete_sequence=px.colors.sequential.Magenta_r  # Color scheme for the pie chart
    )
    st.plotly_chart(fig_pie_1)

    return auqs


def Aggre_user_plot_3(df, state):
    # Filter the dataframe for the selected state
    aguqy = df[df["States"] == state]
    aguqy.reset_index(drop=True, inplace=True)

    # Group by 'Brands' and sum the 'Transaction_count'
    aguqyg = pd.DataFrame(aguqy.groupby("Brands")["Transaction_count"].sum())
    aguqyg.reset_index(inplace=True)

    # Plot a line chart showing transaction count by brand
    fig_scatter_1 = px.line(
        aguqyg, 
        x="Brands", 
        y="Transaction_count", 
        markers=True,  # Adds markers to the line chart
        width=1000, 
        title=f"Transaction Count by Brand in {state.upper()}"  # Title indicating the state
    )
    st.plotly_chart(fig_scatter_1)

    return aguqy


def map_transaction_plot_1(df, state):
    # Filter the dataframe for the selected state
    miys = df[df["States"] == state]
    
    # Group by 'Districts' and sum the 'Transaction_count' and 'Transaction_amount'
    miysg = miys.groupby("District")[["Transaction_count", "Transaction_amount"]].sum()
    miysg.reset_index(inplace=True)

    # Create two columns for side-by-side plotting
    col1, col2 = st.columns(2)
    
    # Plot transaction amount by district
    with col1:
        fig_map_bar_1 = px.bar(
            miysg, 
            x="District", 
            y="Transaction_amount",
            width=600, 
            height=500, 
            title=f"{state.upper()} DISTRICT TRANSACTION AMOUNT",
            color_discrete_sequence=px.colors.sequential.Mint_r  # Color scheme for the bar chart
        )
        st.plotly_chart(fig_map_bar_1)

    # Plot transaction count by district
    with col2:
        fig_map_bar_2 = px.bar(
            miysg, 
            x="District", 
            y="Transaction_count",
            width=600, 
            height=500, 
            title=f"{state.upper()} DISTRICT TRANSACTION COUNT",
            color_discrete_sequence=px.colors.sequential.Mint  # Color scheme for the bar chart
        )
        st.plotly_chart(fig_map_bar_2)

    return miysg


def map_transaction_plot_2(df, state):
    # Filter the dataframe for the selected state
    miys = df[df["States"] == state]
    
    # Group by 'Districts' and sum the 'Transaction_count' and 'Transaction_amount'
    miysg = miys.groupby("District")[["Transaction_count", "Transaction_amount"]].sum()
    miysg.reset_index(inplace=True)

    # Create two columns for side-by-side plotting
    col1, col2 = st.columns(2)
    
    # Plot transaction amount distribution by district using a pie chart
    with col1:
        fig_map_pie_1 = px.pie(
            miysg, 
            names="District", 
            values="Transaction_amount",
            width=600, 
            height=500, 
            title=f"{state.upper()} DISTRICT TRANSACTION AMOUNT",
            hole=0.5,  # Creates a donut-style pie chart
            color_discrete_sequence=px.colors.sequential.Mint_r  # Color scheme for the pie chart
        )
        st.plotly_chart(fig_map_pie_1)

    # Plot transaction count distribution by district using a pie chart
    with col2:
        fig_map_pie_2 = px.pie(
            miysg, 
            names="District", 
            values="Transaction_count",
            width=600, 
            height=500, 
            title=f"{state.upper()} DISTRICT TRANSACTION COUNT",
            hole=0.5,  # Creates a donut-style pie chart
            color_discrete_sequence=px.colors.sequential.Agsunset  # Color scheme for the pie chart
        )
        st.plotly_chart(fig_map_pie_2)

    return miysg


def map_user_plot_1(df, year):
    # Filter the dataframe for the selected year
    muy = df[df["Years"] == year]
    muy.reset_index(drop=True, inplace=True)
    
    # Group by 'States' and sum 'RegisteredUser' and 'AppOpens'
    muyg = muy.groupby("States")[["RegisteredUser", "AppOpens"]].sum()
    muyg.reset_index(inplace=True)

    # Create a line plot for 'RegisteredUser' and 'AppOpens' by 'States'
    fig_map_user_plot_1 = px.line(
        muyg, 
        x="States", 
        y=["RegisteredUser", "AppOpens"], 
        markers=True,  # Add markers to the line plot
        width=1000, 
        height=800, 
        title=f"{year} REGISTERED USER AND APP OPENS",
        color_discrete_sequence=px.colors.sequential.Viridis_r  # Color scheme for the line plot
    )
    
    # Display the plot using Streamlit
    st.plotly_chart(fig_map_user_plot_1)

    return muy


def map_user_plot_2(df, quarter):
    # Filter the dataframe for the selected quarter
    muyq = df[df["Quarter"] == quarter]
    muyq.reset_index(drop=True, inplace=True)
    
    # Group by 'States' and sum 'RegisteredUser' and 'AppOpens'
    muyqg = muyq.groupby("States")[["RegisteredUser", "AppOpens"]].sum()
    muyqg.reset_index(inplace=True)

    # Create a line plot for 'RegisteredUser' and 'AppOpens' by 'States'
    fig_map_user_plot_1 = px.line(
        muyqg, 
        x="States", 
        y=["RegisteredUser", "AppOpens"], 
        markers=True,  # Add markers to the line plot
        title=f"{df['Years'].min()}, {quarter} QUARTER REGISTERED USER AND APP OPENS",
        width=1000, 
        height=800, 
        color_discrete_sequence=px.colors.sequential.Rainbow_r  # Color scheme for the line plot
    )
    
    # Display the plot using Streamlit
    st.plotly_chart(fig_map_user_plot_1)

    return muyq


def map_user_plot_3(df, state):
    # Filter the dataframe for the selected state
    muyqs = df[df["States"] == state]
    muyqs.reset_index(drop=True, inplace=True)
    
    # Group by 'Districts' and sum 'RegisteredUser' and 'AppOpens'
    muyqsg = muyqs.groupby("Districts")[["RegisteredUser", "AppOpens"]].sum()
    muyqsg.reset_index(inplace=True)

    # Create two columns for displaying plots side by side
    col1, col2 = st.columns(2)

    with col1:
        # Create a horizontal bar plot for 'RegisteredUser' by 'Districts'
        fig_map_user_plot_1 = px.bar(
            muyqsg, 
            x="RegisteredUser", 
            y="Districts", 
            orientation="h",  # Horizontal bar plot
            title=f"{state.upper()} REGISTERED USER",
            height=800,  # Set the height of the plot
            color_discrete_sequence=px.colors.sequential.Rainbow_r  # Color scheme
        )
        st.plotly_chart(fig_map_user_plot_1)  # Display the plot using Streamlit

    with col2:
        # Create a horizontal bar plot for 'AppOpens' by 'Districts'
        fig_map_user_plot_2 = px.bar(
            muyqsg, 
            x="AppOpens", 
            y="Districts", 
            orientation="h",  # Horizontal bar plot
            title=f"{state.upper()} APP OPENS",
            height=800,  # Set the height of the plot
            color_discrete_sequence=px.colors.sequential.Rainbow  # Color scheme
        )
        st.plotly_chart(fig_map_user_plot_2)  # Display the plot using Streamlit


def top_user_plot_1(df, year):
    # Filter the dataframe for the specified year
    tuy = df[df["Years"] == year]
    tuy.reset_index(drop=True, inplace=True)
    
    # Group by 'States' and 'Quarter', then sum the 'RegisteredUser'
    tuyg = pd.DataFrame(tuy.groupby(["States", "Quarter"])["RegisteredUser"].sum())
    tuyg.reset_index(inplace=True)

    # Create a grouped bar plot showing 'RegisteredUser' by 'States' and 'Quarter'
    fig_top_plot_1 = px.bar(
        tuyg, 
        x="States", 
        y="RegisteredUser", 
        barmode="group",  # Group bars for different quarters
        color="Quarter",  # Different colors for each quarter
        width=1000,  # Set the width of the plot
        height=800,  # Set the height of the plot
        color_continuous_scale=px.colors.sequential.Burgyl  # Color scale for the bars
    )
    st.plotly_chart(fig_top_plot_1)  # Display the plot using Streamlit

    return tuy  # Return the filtered dataframe for further use


def top_user_plot_2(df, state):
    # Filter the dataframe for the specified state
    tuys = df[df["States"] == state]
    tuys.reset_index(drop=True, inplace=True)
    
    # Group by 'Quarter' and sum the 'RegisteredUser' for each quarter
    tuysg = pd.DataFrame(tuys.groupby("Quarter")["RegisteredUser"].sum())
    tuysg.reset_index(inplace=True)

    # Create a bar plot showing 'RegisteredUser' by 'Quarter'
    fig_top_plot_1 = px.bar(
        tuys,  # DataFrame to plot
        x="Quarter",  # X-axis represents different quarters
        y="RegisteredUser",  # Y-axis represents the sum of registered users
        barmode="group",  # Group bars for different quarters side by side
        width=1000,  # Set the width of the plot
        height=800,  # Set the height of the plot
        color="Pincodes",  # Color bars based on 'Pincodes'
        hover_data="Pincodes",  # Show 'Pincodes' when hovering over bars
        color_continuous_scale=px.colors.sequential.Magenta  # Color scale for the bars
    )
    st.plotly_chart(fig_top_plot_1)  # Display the plot using Streamlit

    return tuys  # Return the filtered dataframe for further use


def Aggre_insurance_Y(df, year):
    # Filter the dataframe to include data only for the specified year
    df_year = df[df["Years"] == year]
    
    # Reset the index of the filtered dataframe to ensure a clean index
    df_year.reset_index(drop=True, inplace=True)
    
    # Return the filtered dataframe for further analysis
    return df_year


def Aggre_insurance_Y_Q(df, quarter):
    # Filter the dataframe to include data only for the specified quarter
    df_quarter = df[df["Quarter"] == quarter]
    
    # Reset the index of the filtered dataframe to ensure a clean index
    df_quarter.reset_index(drop=True, inplace=True)
    
    # Return the filtered dataframe for further analysis
    return df_quarter


def Aggre_Insurance_type(df, state):
    # Filter the dataframe to include only data for the specified state
    df_state = df[df["States"] == state]
    
    # Reset the index of the filtered dataframe to ensure a clean index
    df_state.reset_index(drop=True, inplace=True)
    
    # Group the filtered data by "InsuranceType" and sum the "Amount" for each type
    df_grouped = df_state.groupby(["InsuranceType"])["Amount"].sum()
    
    # Reset the index of the grouped dataframe to turn the series into a dataframe
    df_grouped.reset_index(inplace=True)
    
    # Create a pie chart to visualize the distribution of insurance types by amount
    fig = px.pie(df_grouped, names="InsuranceType", values="Amount",
                 title=f"{state.upper()} INSURANCE TYPE DISTRIBUTION", 
                 color_discrete_sequence=px.colors.sequential.Viridis_r)
    
    # Render the pie chart using Streamlit
    st.plotly_chart(fig)


def map_insure_plot_1(df, year):
    # Filter the dataframe to include only data for the specified year
    df_year = df[df["Years"] == year]
    
    # Reset the index of the filtered dataframe to ensure a clean index
    df_year.reset_index(drop=True, inplace=True)
    
    # Group the filtered data by "States" and count the number of occurrences in each state
    # Use size() to get the count of occurrences per state
    df_grouped = df_year.groupby("States").size().reset_index(name='Counts')
    
    # Create two columns in Streamlit layout for side-by-side charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Create a bar chart to show the count of insurance data by state
        fig1 = px.bar(df_grouped, x="States", y="Counts", orientation="v",
                      title=f"{year} INSURANCE DATA COUNT BY STATE", height=800,
                      color_discrete_sequence=px.colors.sequential.Viridis_r)
        # Render the bar chart using Streamlit
        st.plotly_chart(fig1)

    with col2:
        # Create a second bar chart (example placeholder) with a different color scale
        fig2 = px.bar(df_grouped, x="States", y="Counts", orientation="v",
                      title=f"{year} ADDITIONAL METRIC", height=800,
                      color_discrete_sequence=px.colors.sequential.Inferno_r)
        # Render the second bar chart using Streamlit
        st.plotly_chart(fig2)

    # Return the filtered dataframe for further use if needed
    return df_year


def map_insure_plot_2(df, quarter):
    # Filter the dataframe to include only data for the specified quarter
    df_quarter = df[df["Quarter"] == quarter]
    
    # Reset the index of the filtered dataframe to ensure a clean index
    df_quarter.reset_index(drop=True, inplace=True)
    
    # Group the filtered data by "States" and count the number of occurrences in each state
    # Use size() to get the count of occurrences per state
    df_grouped = df_quarter.groupby("States").size().reset_index(name='Counts')
    
    # Create a scatter plot to visualize the distribution of insurance data by state
    fig = px.scatter(df_grouped, x="States", y="Counts", size="Counts",
                     color="Counts", hover_name="States",
                     title=f"{df['Years'].min()}, {quarter} INSURANCE DATA DISTRIBUTION",
                     size_max=60, height=800, color_continuous_scale=px.colors.sequential.Blues)
    
    # Render the scatter plot using Streamlit
    st.plotly_chart(fig)

    # Return the filtered dataframe for further use if needed
    return df_quarter


def map_insurance_plot_3(df, state):
    # Filter the dataframe to include only data for the specified state
    df_state = df[df["States"] == state]
    
    # Reset the index of the filtered dataframe to ensure a clean index
    df_state.reset_index(drop=True, inplace=True)
    
    # Group the filtered data by "States" and count the number of occurrences in each state
    # Use size() to get the count of occurrences per state
    df_grouped = df_state.groupby("States").size().reset_index(name='Counts')

    # Create a bar plot to visualize the insurance data distribution by state
    fig = px.bar(df_grouped, x="States", y="Counts", orientation="v",
                 title=f"{state.upper()} INSURANCE DATA DISTRIBUTION BY STATE",
                 height=800, color_discrete_sequence=px.colors.sequential.Rainbow_r)
    
    # Render the bar plot using Streamlit
    st.plotly_chart(fig)

    # Return the filtered dataframe for further use if needed
    return df_state


def top_insurance_plot_1(df, year):
    # Filter the dataframe to include only data for the specified year
    df_year = df[df["Years"] == year]
    
    # Reset the index of the filtered dataframe for a clean index
    df_year.reset_index(drop=True, inplace=True)

    # Group the filtered data by "States" and "Quarter", then count the number of records for each group
    df_grouped = df_year.groupby(["States", "Quarter"]).size().reset_index(name='Counts')

    # Create a grouped bar plot to visualize insurance data counts by state and quarter
    fig1 = px.bar(df_grouped, x="States", y="Counts", barmode="group", color="Quarter",
                 title=f"{year} INSURANCE DATA BY STATE AND QUARTER (COUNT)",
                 width=1000, height=800)
    
    # Render the bar plot using Streamlit
    st.plotly_chart(fig1)
    
    # Return the filtered dataframe for further analysis or use
    return df_year


def top_insurance_plot_2(df, state):
    # Filter the dataframe to include only data for the specified state
    df_state = df[df["States"] == state]
    
    # Reset the index of the filtered dataframe for a clean index
    df_state.reset_index(drop=True, inplace=True)

    # Group the filtered data by "Quarter" and count the number of records for each quarter
    df_grouped = df_state.groupby("Quarter").size().reset_index(name='Counts')

    # Create a bar plot to visualize insurance data counts by quarter
    fig2 = px.bar(df_grouped, x="Quarter", y="Counts", barmode="group",
                 title=f"INSURANCE DATA BY QUARTER IN {state.upper()} (COUNT)",
                 width=1000, height=800)
    
    # Render the bar plot using Streamlit
    st.plotly_chart(fig2)

    # Return the filtered dataframe for further analysis or use
    return df_state


def Aggre_insurance_plot_1(df, year):
    # Filter the dataframe to include only records for the specified year
    df_year = df[df["Years"] == year]
    
    # Reset the index of the filtered dataframe for clean indexing
    df_year.reset_index(drop=True, inplace=True)

    # Group the filtered data by "States" and count the number of records for each state
    df_grouped = df_year.groupby("States").size().reset_index(name='Counts')

    # Create a line plot to visualize policy counts by state
    fig = px.line(df_grouped, x="States", y="Counts", markers=True,
                  title=f"{year} - Policy Count by State", 
                  width=1000, height=800, color_discrete_sequence=px.colors.sequential.Viridis_r)
    
    # Render the line plot using Streamlit
    st.plotly_chart(fig)

    # Return the filtered dataframe for further analysis or use
    return df_year


def Aggre_insurance_plot_2(df, quarter):
    # Filter the dataframe to include only records for the specified quarter
    df_quarter = df[df["Quarter"] == quarter]
    
    # Reset the index of the filtered dataframe for clean indexing
    df_quarter.reset_index(drop=True, inplace=True)

    # Group the filtered data by "States" and count the number of records for each state
    df_grouped = df_quarter.groupby("States").size().reset_index(name='Counts')

    # Create a line plot to visualize policy counts by state
    fig = px.line(df_grouped, x="States", y="Counts", markers=True,
                  title=f"{df['Years'].min()}, {quarter} QUARTER - Policy Count by State",
                  width=1000, height=800, color_discrete_sequence=px.colors.sequential.Rainbow_r)
    
    # Render the line plot using Streamlit
    st.plotly_chart(fig)

    # Return the filtered dataframe for further analysis or use
    return df_quarter


def Aggre_insurance_plot_3(df, state):
    # Filter the dataframe to include only records for the specified state
    df_state = df[df["States"] == state]
    
    # Reset the index for clean indexing after filtering
    df_state.reset_index(drop=True, inplace=True)

    # Group the filtered data by "States" and count the number of records for each state
    # This step might seem redundant if we are filtering by a single state, but is included for completeness
    df_grouped = df_state.groupby("States").size().reset_index(name='Counts')

    # Create two columns in Streamlit layout for side-by-side plotting
    col1, col2 = st.columns(2)
    
    with col1:
        # Plot a horizontal bar chart for insurance data count by state
        fig1 = px.bar(df_grouped, x="Counts", y="States", orientation="h",
                      title=f"{state.upper()} INSURANCE DATA COUNT BY STATE", height=800,
                      color_discrete_sequence=px.colors.sequential.Viridis_r)
        st.plotly_chart(fig1)

    with col2:
        # Plot a horizontal bar chart for an additional metric by state
        # Placeholder for a different metric visualization, adjust as necessary
        fig2 = px.bar(df_grouped, x="Counts", y="States", orientation="h",
                      title=f"{state.upper()} ADDITIONAL METRIC BY STATE", height=800,
                      color_discrete_sequence=px.colors.sequential.Inferno_r)
        st.plotly_chart(fig2)

    # Return the filtered dataframe for further analysis or use
    return df_state




#QUESTIONS
def ques1():
    # Aggregate transaction count by mobile brand
    brand = Aggre_user[["Brands", "Transaction_count"]]
    brand_summary = brand.groupby("Brands")["Transaction_count"].sum().sort_values(ascending=False)
    brand_df = pd.DataFrame(brand_summary).reset_index()

    # Create a pie chart of transaction counts by brand
    fig_brands = px.pie(brand_df, values="Transaction_count", names="Brands",
                       color_discrete_sequence=px.colors.sequential.Agsunset,
                       title="Top Mobile Brands by Transaction Count")
    return st.plotly_chart(fig_brands)

def ques2():
    # Aggregate transaction amount by state
    lt = Aggre_transaction[["States", "Transaction_amount"]]
    lt_summary = lt.groupby("States")["Transaction_amount"].sum().sort_values(ascending=True)
    lt_df = pd.DataFrame(lt_summary).reset_index().head(10)

    # Create a bar chart of the lowest transaction amounts by state
    fig_lts = px.bar(lt_df, x="States", y="Transaction_amount",
                    title="Lowest Transaction Amount by States",
                    color_discrete_sequence=px.colors.sequential.Agsunset)
    return st.plotly_chart(fig_lts)

def ques3():
    # Aggregate transaction amount by district
    htd = Map_transaction[["District", "Transaction_amount"]]
    htd_summary = htd.groupby("District")["Transaction_amount"].sum().sort_values(ascending=False)
    htd_df = pd.DataFrame(htd_summary).head(10).reset_index()

    # Create a pie chart of the top 10 districts by transaction amount
    fig_htd = px.pie(htd_df, values="Transaction_amount", names="District",
                    title="Top 10 Districts by Highest Transaction Amount",
                    color_discrete_sequence=px.colors.sequential.Emrld_r)
    return st.plotly_chart(fig_htd)

def ques4():
    # Aggregate transaction amount by district
    htd = Map_transaction[["District", "Transaction_amount"]]
    htd_summary = htd.groupby("District")["Transaction_amount"].sum().sort_values(ascending=True)
    htd_df = pd.DataFrame(htd_summary).head(10).reset_index()

    # Create a pie chart of the top 10 districts by lowest transaction amount
    fig_htd = px.pie(htd_df, values="Transaction_amount", names="District",
                    title="Top 10 Districts by Lowest Transaction Amount",
                    color_discrete_sequence=px.colors.sequential.Agsunset)
    return st.plotly_chart(fig_htd)

def ques5():
    # Aggregate app opens by state
    sa = Map_user[["States", "AppOpens"]]
    sa_summary = sa.groupby("States")["AppOpens"].sum().sort_values(ascending=False)
    sa_df = pd.DataFrame(sa_summary).reset_index().head(10)

    # Create a bar chart of the top 10 states by app opens
    fig_sa = px.bar(sa_df, x="States", y="AppOpens",
                    title="Top 10 States by App Opens",
                    color_discrete_sequence=px.colors.sequential.Agsunset)
    return st.plotly_chart(fig_sa)

def ques6():
    # Aggregate app opens by state
    sa = Map_user[["States", "AppOpens"]]
    sa_summary = sa.groupby("States")["AppOpens"].sum().sort_values(ascending=True)
    sa_df = pd.DataFrame(sa_summary).reset_index().head(10)

    # Create a bar chart of the lowest 10 states by app opens
    fig_sa = px.bar(sa_df, x="States", y="AppOpens",
                    title="Lowest 10 States by App Opens",
                    color_discrete_sequence=px.colors.sequential.Agsunset)
    return st.plotly_chart(fig_sa)

def ques7():
    # Aggregate transaction count by state
    stc = Aggre_transaction[["States", "Transaction_count"]]
    stc_summary = stc.groupby("States")["Transaction_count"].sum().sort_values(ascending=True)
    stc_df = pd.DataFrame(stc_summary).reset_index()

    # Create a bar chart of states with the lowest transaction count
    fig_stc = px.bar(stc_df, x="States", y="Transaction_count",
                     title="States with Lowest Transaction Count",
                     color_discrete_sequence=px.colors.sequential.Jet_r)
    return st.plotly_chart(fig_stc)

def ques8():
    # Aggregate transaction count by state
    stc = Aggre_transaction[["States", "Transaction_count"]]
    stc_summary = stc.groupby("States")["Transaction_count"].sum().sort_values(ascending=False)
    stc_df = pd.DataFrame(stc_summary).reset_index()

    # Create a bar chart of states with the highest transaction count
    fig_stc = px.bar(stc_df, x="States", y="Transaction_count",
                     title="States with Highest Transaction Count",
                     color_discrete_sequence=px.colors.sequential.Magenta_r)
    return st.plotly_chart(fig_stc)

def ques9():
    # Aggregate transaction amount by state
    ht = Aggre_transaction[["States", "Transaction_amount"]]
    ht_summary = ht.groupby("States")["Transaction_amount"].sum().sort_values(ascending=False)
    ht_df = pd.DataFrame(ht_summary).reset_index().head(10)

    # Create a bar chart of the highest transaction amounts by state
    fig_lts = px.bar(ht_df, x="States", y="Transaction_amount",
                     title="Highest Transaction Amount by States",
                     color_discrete_sequence=px.colors.sequential.Agsunset)
    return st.plotly_chart(fig_lts)

def ques10():
    # Aggregate transaction amount by district
    dt = Map_transaction[["District", "Transaction_amount"]]
    dt_summary = dt.groupby("District")["Transaction_amount"].sum().sort_values(ascending=True)
    dt_df = pd.DataFrame(dt_summary).reset_index().head(50)

    # Create a bar chart of districts with the lowest transaction amounts
    fig_dt = px.bar(dt_df, x="District", y="Transaction_amount",
                    title="Districts with Lowest Transaction Amount",
                    color_discrete_sequence=px.colors.sequential.Mint_r)
    return st.plotly_chart(fig_dt)



#Streamlit part

import streamlit as st

# Set up the main page configuration
st.set_page_config(page_title="PHONEPE Dashboard", page_icon="📱", layout="wide")

# Sidebar navigation
st.sidebar.title("Navigation")
select = st.sidebar.radio(
    "Go to",
    ["Home", "Data Overview", "Explore Data", "Basic Insights"],
    index=0
)

# Home Page
if select == "Home":
    st.title("📱 PHONEPE - India’s Leading Fintech Solution")
    st.markdown("**Domain**: `Fintech`")
    st.markdown("**Technologies Used**: `GitHub`, `Python`, `Pandas`, `PostgreSQL`, `Streamlit`, `Plotly`")
    
    st.subheader("About PhonePe")
    st.write(
        "PhonePe is a pioneering digital payments platform and fintech company, based in Bengaluru, India. "
        "Launched in 2015, PhonePe has revolutionized the way India transacts, making payments simple, "
        "secure, and seamless through its UPI-based app. It is a subsidiary of Flipkart, owned by Walmart."
    )

    st.write("### Key Features")
    st.write("- **Credit & Debit Card Linking**")
    st.write("- **Bank Balance Check**")
    st.write("- **Money Storage**")
    st.write("- **PIN Authorization**")

    st.markdown("[Download the App Now](https://www.phonepe.com/app-download/)", unsafe_allow_html=True)

# Data Overview Page
if select == "Data Overview":
    st.title("PhonePe Pulse Data: Insights for India")
    
    st.subheader("Key Data Dimensions")
    st.write("- **State**: All Indian States")
    st.write("- **Year**: 2018 to 2023")
    st.write("- **Quarter**: Q1 (Jan-Mar), Q2 (Apr-Jun), Q3 (Jul-Sep), Q4 (Oct-Dec)")

    st.subheader("Aggregated Transaction Data")
    st.write("- **Payment Types**: Recharge & Bill Payments, Peer-to-Peer, Merchant Payments, Financial Services, Others")
    
    st.subheader("Aggregated User Data")
    st.write("User data categorized by device brands across states.")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.write("- **Apple**")
        st.write("- **Asus**")
        st.write("- **Coolpad**")
        st.write("- **Gionee**")
        st.write("- **HMD Global**")
    with col2:
        st.write("- **Huawei**")
        st.write("- **Infinix**")
        st.write("- **Lava**")
        st.write("- **Lenovo**")
        st.write("- **Lyf**")
    with col3:
        st.write("- **Micromax**")
        st.write("- **Motorola**")
        st.write("- **OnePlus**")
        st.write("- **Oppo**")
        st.write("- **Realme**")
    with col4:
        st.write("- **Samsung**")
        st.write("- **Tecno**")
        st.write("- **Vivo**")
        st.write("- **Xiaomi**")
        st.write("- **Others**")
    
    st.subheader("Map Insights")
    st.write("- **Transactions**: Number and value at state/district level.")
    st.write("- **Users**: Registered users and app opens at state/district level.")
    
    st.subheader("Top Transactions and Users")
    st.write("- **Transactions**: Top 10 States, Districts, and Pincodes by transaction volume.")
    st.write("- **Users**: Top 10 States, Districts, and Pincodes by user registrations.")

# Explore Data Page
if select == "Explore Data":
    st.title("Explore Data")
    
    Type = st.selectbox("Select Data Type", ["Transactions", "Users","insurance"])
    
    if Type == "Transactions":
        method = st.radio("Choose Analysis Method", ["Aggregated Analysis", "Map Analysis", "Top Analysis"])
        
        if method == "Aggregated Analysis":
            st.subheader("Aggregated Transaction Analysis")
            col1, col2 = st.columns(2)
            with col1:
                years_at = st.slider("Select the Year", Aggre_transaction["Years"].min(), Aggre_transaction["Years"].max(), Aggre_transaction["Years"].min())
            
            df_agg_tran_Y = Aggre_transaction_Y(Aggre_transaction, years_at)
            with col1:
                quarters_at = st.slider("Select the Quarter", df_agg_tran_Y["Quarter"].min(), df_agg_tran_Y["Quarter"].max(), df_agg_tran_Y["Quarter"].min())
            
            df_agg_tran_Y_Q = Aggre_transaction_Y_Q(df_agg_tran_Y, quarters_at)
            state_Y_Q = st.selectbox("Select the State", df_agg_tran_Y_Q["States"].unique())
            Aggre_Transaction_type(df_agg_tran_Y_Q, state_Y_Q)
        
        elif method == "Map Analysis":
            st.subheader("Transaction Map Analysis")
            col1, col2 = st.columns(2)
            
            with col1:
                years_m2 = st.slider("Select the Year", Map_transaction["Years"].min(), Map_transaction["Years"].max(), Map_transaction["Years"].min())
            
            df_map_tran_Y = Aggre_transaction_Y(Map_transaction, years_m2)
            
            with col1:
                state_m3 = st.selectbox("Select the State", df_map_tran_Y["States"].unique())
            map_transaction_plot_1(df_map_tran_Y, state_m3)
            
            with col1:
                quarters_m2 = st.slider("Select the Quarter", df_map_tran_Y["Quarter"].min(), df_map_tran_Y["Quarter"].max(), df_map_tran_Y["Quarter"].min())
            
            df_map_tran_Y_Q = Aggre_transaction_Y_Q(df_map_tran_Y, quarters_m2)
            
            state_m4 = st.selectbox("Select the State", df_map_tran_Y_Q["States"].unique(), key="state_selectbox_1")
            map_transaction_plot_2(df_map_tran_Y_Q, state_m4)

        elif method == "Top Analysis":
            st.subheader("Top Transaction Analysis")
            col1, col2 = st.columns(2)
            
            with col1:
                years_t2 = st.slider("Select the Year", Top_transaction["Years"].min(), Top_transaction["Years"].max(), Top_transaction["Years"].min())
            
            df_top_tran_Y = Aggre_transaction_Y(Top_transaction, years_t2)
            
            with col1:
                quarters_t2 = st.slider("Select the Quarter", df_top_tran_Y["Quarter"].min(), df_top_tran_Y["Quarter"].max(), df_top_tran_Y["Quarter"].min())
            
            df_top_tran_Y_Q = Aggre_transaction_Y_Q(df_top_tran_Y, quarters_t2)

            # Visualization for Yearly data
            top_transaction_plot_year(df_top_tran_Y)

            # Visualization for Quarterly data
            top_transaction_plot_quarter(df_top_tran_Y_Q)

    
    if Type == "Users":
        method_U = st.radio("Choose Analysis Method", ["Aggregated Analysis", "Map Analysis", "Top Analysis"])
        
        if method_U == "Aggregated Analysis":
            st.subheader("Aggregated User Analysis")
            year_au = st.selectbox("Select the Year", Aggre_user["Years"].unique())
            agg_user_Y = Aggre_user_plot_1(Aggre_user, year_au)
            
            quarter_au = st.selectbox("Select the Quarter", agg_user_Y["Quarter"].unique())
            agg_user_Y_Q = Aggre_user_plot_2(agg_user_Y, quarter_au)
            
            state_au = st.selectbox("Select the State", agg_user_Y["States"].unique())
            Aggre_user_plot_3(agg_user_Y_Q, state_au)
        
        elif method_U == "Map Analysis":
            st.subheader("User Map Analysis")
            col1, col2 = st.columns(2)
            with col1:
                year_mu1 = st.selectbox("Select the Year", Map_user["Years"].unique())
            map_user_Y = map_user_plot_1(Map_user, year_mu1)
            
            with col1:
                quarter_mu1 = st.selectbox("Select the Quarter", map_user_Y["Quarter"].unique())
            map_user_Y_Q = map_user_plot_2(map_user_Y, quarter_mu1)
            
            state_mu1 = st.selectbox("Select the State", map_user_Y_Q["States"].unique())
            map_user_plot_3(map_user_Y_Q, state_mu1)
        
        elif method_U == "Top Analysis":
            st.subheader("Top User Analysis")
            col1, col2 = st.columns(2)
            with col1:
                years_t3 = st.selectbox("Select the Year", Top_user["Years"].unique())
            df_top_user_Y = top_user_plot_1(Top_user, years_t3)
            with col1:
                state_t3 = st.selectbox("Select the State", df_top_user_Y["States"].unique())
            df_top_user_Y_S = top_user_plot_2(df_top_user_Y, state_t3)

    if Type == "insurance":
        method_I = st.radio("Choose Analysis Method", ["Aggregated Analysis", "Map Analysis", "Top Analysis"])
        
        if method_I == "Aggregated Analysis":
            st.subheader("Aggregated Insurance Analysis")
            year_ai = st.selectbox("Select the Year", Aggre_insurance["Years"].unique())
            agg_insurance_Y = Aggre_insurance_plot_1(Aggre_insurance, year_ai)
            
            quarter_ai = st.selectbox("Select the Quarter", agg_insurance_Y["Quarter"].unique())
            agg_insurance_Y_Q = Aggre_insurance_plot_2(agg_insurance_Y, quarter_ai)
            
            state_ai = st.selectbox("Select the State", agg_insurance_Y["States"].unique())
            Aggre_insurance_plot_3(agg_insurance_Y_Q, state_ai)
        
        elif method_I == "Map Analysis":
            st.subheader("Insurance Map Analysis")
            col1, col2 = st.columns(2)

            # Select Year
            with col1:
                year_mi1 = st.selectbox("Select the Year", Map_insurance["Years"].unique())
                map_insurance_Y = map_insure_plot_1(Map_insurance, year_mi1)

            # Select Quarter
            with col1:
                # Ensure the DataFrame returned from map_insure_plot_1 includes 'Quarter'
                if "Quarter" in map_insurance_Y.columns:
                    quarter_mi1 = st.selectbox("Select the Quarter", map_insurance_Y["Quarter"].unique())
                    map_insurance_Y_Q = map_insure_plot_2(map_insurance_Y, quarter_mi1)
                else:
                    st.error("The selected year does not contain any data for quarters.")

            # Select State
            state_mi1 = st.selectbox("Select the State", map_insurance_Y_Q["States"].unique())
            map_insurance_plot_3(map_insurance_Y_Q, state_mi1)

        
        elif method_I == "Top Analysis":
            st.subheader("Top Insurance Analysis")
            col1, col2 = st.columns(2)

            # Select Year
            with col1:
                years_ti = st.selectbox("Select the Year", Top_insurance["Years"].unique())
                df_top_insurance_Y = top_insurance_plot_1(Top_insurance, years_ti)

            # Select State
            with col1:
                if "States" in df_top_insurance_Y.columns:
                    state_ti = st.selectbox("Select the State", df_top_insurance_Y["States"].unique())
                    df_top_insurance_Y_S = top_insurance_plot_2(df_top_insurance_Y, state_ti)
                else:
                    st.error("The selected year does not contain any data for states.")

# Basic Insights Page
if select == "Basic Insights":
    st.title("BASIC INSIGHTS")
    st.write("----")
    st.subheader("Let's explore some basic insights about the PhonePe data.")

    ques = st.selectbox(
        "**Select a Question**",
        (
            "Top Brands Of Mobiles Used",
            "States With Lowest Transaction Amount",
            "Districts With Highest Transaction Amount",
            "Top 10 Districts With Lowest Transaction Amount",
            "Top 10 States With AppOpens",
            "Least 10 States With AppOpens",
            "States With Lowest Transaction Count",
            "States With Highest Transaction Count",
            "States With Highest Transaction Amount",
            "Top 50 Districts With Lowest Transaction Amount",
        )
    )

    st.write("----")  # Separator line for better visual distinction

    if ques == "Top Brands Of Mobiles Used":
        ques1()  # Function to handle this question
    elif ques == "States With Lowest Transaction Amount":
        ques2()
    elif ques == "Districts With Highest Transaction Amount":
        ques3()
    elif ques == "Top 10 Districts With Lowest Transaction Amount":
        ques4()
    elif ques == "Top 10 States With AppOpens":
        ques5()
    elif ques == "Least 10 States With AppOpens":
        ques6()
    elif ques == "States With Lowest Transaction Count":
        ques7()
    elif ques == "States With Highest Transaction Count":
        ques8()
    elif ques == "States With Highest Transaction Amount":
        ques9()
    elif ques == "Top 50 Districts With Lowest Transaction Amount":
        ques10()




