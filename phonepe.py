"""
##########################################################
      PHONEPE PULSE DATA VISUALIZATION & EXPLORATION
##########################################################
"""


#-----------------MODULES-USED-------------------
import mysql.connector as mc
import pandas as pd
import streamlit as st
import plotly.express as px
import base64
from PIL import Image


#-----------------SQL-CONNECTION-------------------
def get_mysql_connection():
    return mc.connect(
        host="localhost",
        port="3306",
        user="root",
        passwd="saipavan55",
        database="phonepe",
        auth_plugin='mysql_native_password'
    )


#-----------------AGGREGATED-USER------------------
def agg_user():
    cursor.execute("SELECT * FROM aggregated_user")
    table1 = cursor.fetchall()
    agree_user = pd.DataFrame(table1, columns=("States", "Years", "Quarter", "Brands", "Transaction_count", "Percentage"))
    agree_user.fillna(value = "Unknown", inplace=True)
    return agree_user


#----------------AGGREGATED-INSURANCE---------------
def agg_insur():
    cursor.execute("SELECT * FROM aggregated_insurance")
    table2 = cursor.fetchall()
    agree_insurance = pd.DataFrame(table2, columns=("States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))
    agree_insurance.fillna(value = "Unknown", inplace=True)
    return agree_insurance


#---------------AGGREGATED-TRANSACTION---------------
def agg_tran():
    cursor.execute("SELECT * FROM aggregated_transaction")
    table3 = cursor.fetchall()
    agree_transaction = pd.DataFrame(table3, columns=("States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))
    agree_transaction.fillna(value = "Unknown", inplace=True)
    return agree_transaction


#------------------MAP-USER----------------------
def map_user():
    cursor.execute("SELECT * FROM map_user")
    table4 = cursor.fetchall()
    map_user = pd.DataFrame(table4, columns=("States", "Years", "Quarter", "District", "RegisteredUser", "AppOpens"))
    map_user.fillna(value = "Unknown", inplace=True)
    return map_user


#-----------------MAP-INSURANCE--------------------
def map_insur():
    cursor.execute("SELECT * FROM map_insurance")
    table5 = cursor.fetchall()
    map_insurance = pd.DataFrame(table5, columns=("States", "Years", "Quarter", "District", "Transaction_count","Transaction_amount"))
    map_insurance.fillna(value = "Unknown", inplace=True)
    return map_insurance


#---------------MAP-TRANSACTION----------------
def map_tran():
    cursor.execute("SELECT * FROM map_transaction")
    table6 = cursor.fetchall()
    map_transaction = pd.DataFrame(table6, columns = ("States", "Years", "Quarter", "District", "Transaction_count", "Transaction_amount"))
    map_transaction.fillna(value = "Unknown", inplace=True)
    return map_transaction


#----------------TOP-USER--------------------
def tp_user():
    cursor.execute("SELECT * FROM top_user")
    table7 = cursor.fetchall()
    top_user = pd.DataFrame(table7, columns = ("States", "Years", "Quarter", "Pincodes", "RegisteredUser"))
    top_user.fillna(value = "Unknown", inplace=True)
    return top_user


#----------------TOP-INSURANCE---------------
def tp_insur():
    cursor.execute("SELECT * FROM top_insurance")
    table8 = cursor.fetchall()
    top_insurance = pd.DataFrame(table8, columns = ("States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"))
    top_insurance.fillna(value = "Unknown", inplace=True)
    return top_insurance


#-----------------TOP-TRANSACTION-----------------
def tp_tran():
    cursor.execute("SELECT * FROM top_transaction")
    table9 = cursor.fetchall()
    top_transaction = pd.DataFrame(table9, columns = ("States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"))
    top_transaction.fillna(value = "Unknown", inplace=True)
    return top_transaction


#----------------HOMEPAGE------------------
def homepage():
    st.title("PHONEPE PULSE DATA VISUALIZATION & EXPLORATION")
    st.write("Project By Tumu Mani Sai Pavan")
    file = open("C:/Users/saipa/Downloads/ppe1.gif", "rb")
    contents = file.read()
    data_gif = base64.b64encode(contents).decode("utf-8")
    file.close()
    st.markdown(f'<img src="data:image/gif;base64,{data_gif}" style="width: 800px;" >',unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Overview")
    st.write("""The Phonepe pulse Github repository contains a large amount of data related to
                various metrics and statistics. The goal is to extract this data and process it to obtain
                insights and information that can be visualized in a user-friendly manner through Python, MySQL and Plotly.""")
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Skills Takeaway")
    st.write("""
                1) Github Cloning
                2) Python
                3) Pandas
                4) Plotly
                5) Streamlit 
                6) Data Management Using MySQL""")
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("About me")
    st.write(""""Self-motivated computer science student with keen interest in coding". Engineer with a passion for machine learning, With a mix of academic knowledge, practical skills, and a growth-oriented attitude, I'm eager to make my debut in the AI & ML field.""")
    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Contact")
    st.write("For any queries or collaborations, feel free to reach out to me:")
    email_icon = Image.open("C:/Users/saipa/Downloads/mail.jpg")
    st.write("Email:")
    col1, col2 = st.columns([0.4, 5])
    with col1:
        st.image(email_icon, width=50)
    with col2:
        st.write("tmsaipavan@gmail.com")
    lin_icon = Image.open("C:/Users/saipa/Downloads/in.jpg")
    st.write("LinkedIn:")
    col1, col2 = st.columns([0.4, 5])
    with col1:
        st.image(lin_icon, width=50)
    with col2:
        st.write("[Sai Pavan TM](https://www.linkedin.com/in/saipavantm/)")


#---------------LOAD-LATITUDE-LONGITUDE----------------
def load_lat_lon_data():
    lat_lon_data = pd.read_csv("C:/Users/saipa/Downloads/district_lat_lon.csv")
    df = pd.DataFrame(lat_lon_data)
    return df


#----------------DATA-EXPLORATION----------------
def data_exploration():
    st.title("Data Exploration")
    lat_lon_data = load_lat_lon_data()
    tab1, tab2, tab3 = st.tabs(["Top", "Map", "Aggregated"])

    with tab1:
        st.header("Top Data")
        top_data_type = st.selectbox("Select Data Type", ["User", "Insurance", "Transaction"], key="top_data_type")
        if top_data_type == "User":
            df = tp_user()
            value_col = "RegisteredUser"
            title = "Top Users by Pincode"
        elif top_data_type == "Insurance":
            df = tp_insur()
            value_col = "Transaction_amount"
            title = "Top Insurance Transactions by Pincode"
        elif top_data_type == "Transaction":
            df = tp_tran()
            value_col = "Transaction_count"
            title = "Top Transactions by Pincode"
        year = st.selectbox("Select Year", df["Years"].unique(), key="top_year")
        quarter = st.selectbox("Select Quarter", df["Quarter"].unique(), key="top_quarter")
        filtered_df = df[(df["Years"] == year) & (df["Quarter"] == quarter)]
        st.write(filtered_df)
        lat_lon_data = load_lat_lon_data()
        filtered_df = pd.merge(filtered_df, lat_lon_data, on="States", how="left")
        st.header(f"{title} - Mapbox Map")
        fig = px.scatter_mapbox(filtered_df, lat="Lat", lon="Lon", hover_name="States", size=value_col,
                                color="States", color_continuous_scale="Viridis",
                                title=f"{title} - Mapbox Map",
                                zoom=3, center={"lat": 20.5937, "lon": 78.9629},
                                height=600)
        fig.update_layout(mapbox_style="carto-positron")
        st.plotly_chart(fig)
        st.success("Fetched Successfully!!!")

    with tab2:
        st.header("Map Data")
        map_data_type = st.selectbox("Select Data Type", ["User", "Insurance", "Transaction"], key="map_data_type")
        if map_data_type == "User":
            df = map_user()
        elif map_data_type == "Insurance":
            df = map_insur()
        elif map_data_type == "Transaction":
            df = map_tran()
        year_options = df["Years"].unique()
        quarter_options = df["Quarter"].unique()
        year = st.selectbox("Select Year", options=year_options, key="map_year")
        quarter = st.selectbox("Select Quarter", options=quarter_options, key="map_quarter")
        filtered_df = df[(df["Years"] == year) & (df["Quarter"] == quarter)]
        st.subheader(f"{map_data_type} Distribution")
        st.write(filtered_df)
        if not filtered_df.empty:
            filtered_df = pd.merge(filtered_df, lat_lon_data, on="States", how="left")
            fig = px.scatter_mapbox(filtered_df, lat="Lat", lon="Lon", hover_name="District", size="RegisteredUser" if map_data_type == "User" else "Transaction_amount",
                                    color="States", color_continuous_scale="Viridis", title=f"{map_data_type} Distribution", mapbox_style="carto-positron", zoom=3, center={"lat": 20.5937, "lon": 78.9629})
            st.plotly_chart(fig)
            st.success("Fetched Successfully!!!")

    with tab3:
        st.header("Aggregated Data")
        agg_data_type = st.selectbox("Select Data Type", ["User", "Insurance", "Transaction"], key="agg_data_type")
        if agg_data_type == "User":
            df = agg_user()
        elif agg_data_type == "Insurance":
            df = agg_insur()
        elif agg_data_type == "Transaction":
            df = agg_tran()
        year = st.selectbox("Select Year", df["Years"].unique(), key="agg_year")
        quarter = st.selectbox("Select Quarter", df["Quarter"].unique(), key="agg_quarter")
        filtered_df = df[(df["Years"] == year) & (df["Quarter"] == quarter)]
        st.write(filtered_df)
        if agg_data_type == "User":
            fig = px.bar(filtered_df, x="Brands", y="Transaction_count", color="States", title="Aggregated User Data")
        elif agg_data_type == "Insurance":
            fig = px.bar(filtered_df, x="Transaction_type", y="Transaction_count", color="States", title="Aggregated Insurance Data")
        elif agg_data_type == "Transaction":
            fig = px.bar(filtered_df, x="Transaction_type", y="Transaction_count", color="States", title="Aggregated Transaction Data")
        st.plotly_chart(fig)
        st.success("Fetched Successfully!!!")


#--------------TOTAL-TRANSACTION-COUNT-----------------
def total_transaction_count(cursor):
    query = """
        SELECT States, Years, Quarter, 
               SUM(Transaction_count) AS Total_Transactions, 
               SUM(Transaction_amount) AS Total_Amount
        FROM aggregated_transaction
        GROUP BY States, Years, Quarter
        ORDER BY States, Years, Quarter
    """
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.title("Total Transactions and Amounts Per State, Year, and Quarter")
    for quarter in df['Quarter'].unique():
        df_quarter = df[df['Quarter'] == quarter]
        st.subheader(f'Quarter {quarter}')
        fig_transactions = px.bar(df_quarter, x='States', y='Total_Transactions',
                                  color='Years', barmode='group',
                                  title=f'Total Transactions for Quarter {quarter}',
                                  labels={'Total_Transactions': 'Total Transactions'},
                                  height=600, width=1000)
        fig_transactions.update_layout(
            title_font_size=24,
            xaxis_title_font_size=20,
            yaxis_title_font_size=20,
            legend_title_font_size=18,
            legend_font_size=16
        )
        st.plotly_chart(fig_transactions)
        fig_amount = px.bar(df_quarter, x='States', y='Total_Amount',
                            color='Years', barmode='group',
                            title=f'Total Amount for Quarter {quarter}',
                            labels={'Total_Amount': 'Total Amount'},
                            height=600, width=1000)
        fig_amount.update_layout(
            title_font_size=24,
            xaxis_title_font_size=20,
            yaxis_title_font_size=20,
            legend_title_font_size=18,
            legend_font_size=16
        )
        st.plotly_chart(fig_amount)
    return data, columns


#----------------TRANSACTION-TYPE---------------
def transaction_type(cursor):
    query = """
        SELECT States, Years, Transaction_type, 
               SUM(Transaction_count) AS Total_Transactions, 
               SUM(Transaction_amount) AS Total_Amount
        FROM aggregated_transaction
        GROUP BY States, Years, Transaction_type
        ORDER BY States, Years, Total_Amount DESC
    """
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.title("Transaction Types Dominating Each State")
    for state in df['States'].unique():
        df_state = df[df['States'] == state]
        st.subheader(f'State: {state}')
        max_transactions = float(df_state['Total_Transactions'].max())
        max_amount = float(df_state['Total_Amount'].max())
        fig_transactions = px.bar(df_state, x='Transaction_type', y='Total_Transactions',
                                  color='Years', barmode='group',
                                  title=f'Total Transactions by Transaction Type for {state}',
                                  labels={'Total_Transactions': 'Total Transactions'},
                                  height=600, width=1000)
        fig_transactions.update_layout(
            title_font_size=24,
            xaxis_title='Transaction Type',
            yaxis_title='Total Transactions',
            xaxis_title_font_size=20,
            yaxis_title_font_size=20,
            legend_title_font_size=18,
            legend_font_size=16,
            bargap=0.2,
            bargroupgap=0.1,
        )
        fig_transactions.update_yaxes(range=[0, max_transactions * 1.1])
        st.plotly_chart(fig_transactions)
        fig_amount = px.bar(df_state, x='Transaction_type', y='Total_Amount',
                            color='Years', barmode='group',
                            title=f'Total Amount by Transaction Type for {state}',
                            labels={'Total_Amount': 'Total Amount'},
                            height=600, width=1000)
        fig_amount.update_layout(
            title_font_size=24,
            xaxis_title='Transaction Type',
            yaxis_title='Total Amount',
            xaxis_title_font_size=20,
            yaxis_title_font_size=20,
            legend_title_font_size=18,
            legend_font_size=16,
            bargap=0.2,
            bargroupgap=0.1,
        )
        fig_amount.update_yaxes(range=[0, max_amount * 1.1])
        st.plotly_chart(fig_amount)
    return data, columns


#---------------USER-ENGAGEMENT----------------
def user_engagement(cursor):
    query = """
        SELECT States, Years, Quarter, 
               SUM(RegisteredUser) AS Total_Registered_Users, 
               SUM(AppOpens) AS Total_App_Opens
        FROM map_user
        GROUP BY States, Years, Quarter
        ORDER BY States, Years, Quarter
    """
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.title("User Engagement by State and Quarter")
    for state in df['States'].unique():
        df_state = df[df['States'] == state]
        st.subheader(f'State: {state}')
        st.write(df_state)
        fig_registered_users = px.pie(df_state, values='Total_Registered_Users', names='Quarter',
                                      title=f'Total Registered Users for {state}',
                                      height=600, width=800)
        fig_registered_users.update_layout(
            title_font_size=24,
            legend_title_font_size=18,
            legend_font_size=16,
        )
        st.plotly_chart(fig_registered_users)
        fig_app_opens = px.pie(df_state, values='Total_App_Opens', names='Quarter',
                               title=f'Total App Opens for {state}',
                               height=600, width=800)
        fig_app_opens.update_layout(
            title_font_size=24,
            legend_title_font_size=18,
            legend_font_size=16,
        )
        st.plotly_chart(fig_app_opens)
    return data, columns


#----------------BRAND-CATEGORIES----------------
def brands_categories(cursor):
    query = """
        SELECT States, Years, Quarter, Brands, 
               SUM(Transaction_count) AS Total_Transactions, 
               AVG(Percentage) AS Average_Percentage
        FROM aggregated_user
        GROUP BY States, Years, Quarter, Brands
        ORDER BY States, Years, Total_Transactions DESC
    """
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.title("Highest Transaction Percentages by Brands or Categories")
    for state in df['States'].unique():
        st.subheader(f' State: {state}')
        for quarter in df[df['States'] == state]['Quarter'].unique():
            df_state_quarter = df[(df['States'] == state) & (df['Quarter'] == quarter)]
            if not df_state_quarter.empty:
                fig = px.pie(df_state_quarter, values='Average_Percentage', names='Brands',
                             title=f'Highest Transaction {state}, Q{quarter}',
                             height=400, width=600)
                fig.update_layout(
                    title_font_size=20,
                    legend_title_font_size=16,
                    legend_font_size=14,
                    margin=dict(l=20, r=20, t=50, b=20),
                    paper_bgcolor='rgba(0,0,0,0)', 
                    plot_bgcolor='rgba(0,0,0,0)', 
                )
                st.plotly_chart(fig)
            else:
                st.write("No data available for this quarter.")
    return data, columns


#----------------AVERAGE-INSURANCE----------------
def avg_insurance(cursor):
    query = """
        SELECT States, Years, Quarter, 
               AVG(Insurance_amount) AS Average_Insurance_Amount
        FROM aggregated_insurance
        GROUP BY States, Years, Quarter
        ORDER BY States, Years, Quarter
    """
    try:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        if not data:
            st.warning("No data found.")
            return
        df = pd.DataFrame(data, columns=columns)
        st.title("Average Insurance Amount per State, Year, and Quarter")
        st.write(df)
        fig = px.bar(df, x='States', y='Average_Insurance_Amount', color='Quarter',
                     title='Average Insurance Amount by State, Year, and Quarter',
                     labels={'States': 'State', 'Average_Insurance_Amount': 'Average Insurance Amount'})
        fig.update_layout(
            xaxis_title='States',
            yaxis_title='Average Insurance Amount',
            title_font_size=24,
            legend_title_font_size=18,
            legend_font_size=16,
            barmode='group',
            margin=dict(l=20, r=20, t=50, b=20),
            uniformtext_minsize=12,
            uniformtext_mode='hide',
            height=600,
            width=1000,
        )
        st.plotly_chart(fig)
    except mysql.connector.Error as e:
        st.error(f"Error executing MySQL query: {e}")
    return data, columns


#-----------------INSURANCE-TYPES-----------------
def insurance_types(cursor):
    query = """
        SELECT States, Years, Insurance_type, 
               SUM(Insurance_count) AS Total_Transactions, 
               SUM(Insurance_amount) AS Total_Amount
        FROM aggregated_insurance
        GROUP BY States, Years, Insurance_type
        ORDER BY States, Years, Total_Amount DESC
    """
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    df['Total_Transactions'] = pd.to_numeric(df['Total_Transactions'], errors='coerce')
    df['Total_Amount'] = pd.to_numeric(df['Total_Amount'], errors='coerce')
    df.dropna(subset=['Total_Amount'], inplace=True)
    st.title("Insurance Types with Highest Transaction Counts and Amounts Annually")
    st.write(df)
    if not df.empty:
        years = df['Years'].unique()
        for year in years:
            df_year = df[df['Years'] == year]
            df_annual_highest = df_year.groupby(['States']).apply(lambda x: x.nlargest(1, 'Total_Amount')).reset_index(drop=True)
            fig = px.bar(df_annual_highest, x='Insurance_type', y='Total_Amount', color='States',
                         title=f'Highest Transaction Amounts by Insurance Type for Year {year}',
                         labels={'insurance_type': 'Insurance Type', 'Total_Amount': 'Total Amount'})
            fig.update_layout(
                xaxis_title='Insurance Type',
                yaxis_title='Total Amount',
                title_font_size=24,
                legend_title_font_size=18,
                legend_font_size=16,
                margin=dict(l=20, r=20, t=50, b=20),
                uniformtext_minsize=12, 
                uniformtext_mode='hide',
                height=600,
                width=1000,
            )
            st.plotly_chart(fig)
    else:
        st.write("No data available for insurance types.")
    return data, columns


#-----------------TRANSACTION-PATTERNS-----------------
def transaction_patterns(cursor):
    query = """
        SELECT States, Years, Quarter, District, 
               SUM(Transaction_count) AS Total_Transactions, 
               SUM(Transaction_amount) AS Total_Amount
        FROM map_transaction
        GROUP BY States, Years, Quarter, District
        ORDER BY States, Years, Quarter, SUM(Transaction_count) DESC
    """
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.title("Transaction Patterns Across States, Quarters, and Districts")
    st.write(df)
    if not df.empty:
        states = df['States'].unique()
        quarters = df['Quarter'].unique()
        for state in states:
            for quarter in quarters:
                df_state_quarter = df[(df['States'] == state) & (df['Quarter'] == quarter)]
                fig = px.bar(df_state_quarter, x='District', y='Total_Transactions', color='District',
                             title=f'Transaction Patterns in {state}, Q{quarter}',
                             labels={'District': 'District', 'Total_Transactions': 'Total Transactions'})
                fig.update_layout(
                    xaxis_title='District',
                    yaxis_title='Total Transactions',
                    title_font_size=24,
                    legend_title_font_size=18,
                    legend_font_size=16,
                    margin=dict(l=20, r=20, t=50, b=20),
                    uniformtext_minsize=12, 
                    uniformtext_mode='hide',
                    height=600,
                    width=1000,
                )
                st.subheader(f'State: {state}, Quarter: {quarter}')
                st.plotly_chart(fig)
    else:
        st.write("No data available for transaction patterns.")
    return data, columns


#---------------PINCODE-CONTRIBUTION--------------
def pincode_contribution(cursor):
    query = """
        SELECT States, Years, Quarter, Pincodes, 
               SUM(Transaction_count) AS Total_Transactions, 
               SUM(Transaction_amount) AS Total_Amount
        FROM top_transaction
        GROUP BY States, Years, Quarter, Pincodes
        ORDER BY States, Years, Quarter, Total_Amount DESC
    """
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.title("Pincode Contribution to Transaction Counts and Amounts")
    st.write(df)
    if not df.empty:
        states = df['States'].unique()
        quarters = df['Quarter'].unique()
        for state in states:
            st.subheader(f'State: {state}')
            for quarter in quarters:
                df_state_quarter = df[(df['States'] == state) & (df['Quarter'] == quarter)]
                fig = px.pie(df_state_quarter, values='Total_Amount', names='Pincodes',
                             title=f'Pincode Contribution in Q{quarter}',
                             labels={'Pincodes': 'Pincode', 'Total_Amount': 'Total Amount'})
                fig.update_layout(
                    title_font_size=24,
                    legend_title_font_size=18,
                    legend_font_size=16,
                    height=600,
                    width=800,
                )
                st.plotly_chart(fig)
    else:
        st.write("No data available for pincode contributions.")
    return data, columns


#----------------TOP-USERS-----------------
def top_users(cursor):
    query = """
            SELECT t.States, t.Years, t.Quarter, 
            t.Transaction_type,
            SUM(t.Transaction_count) AS Total_Transactions, 
            SUM(t.Transaction_amount) AS Total_Amount,
            CASE WHEN SUM(t.Transaction_amount) > 190000000 THEN 'Top User' ELSE 'Regular User' END AS User_Type
            FROM aggregated_transaction t
            LEFT JOIN top_user u ON t.States = u.States 
            AND t.Years = u.Years 
            AND t.Quarter = u.Quarter
            GROUP BY t.States, t.Years, t.Quarter, t.Transaction_type
            ORDER BY t.States, t.Years, t.Quarter, Total_Amount DESC
    """
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.title("Comparison of Transaction Behavior Between Top Users and Regular Users")
    st.write(df)
    if not df.empty:
        states = df['States'].unique()
        quarters = df['Quarter'].unique()
        for state in states:
            for quarter in quarters:
                df_state_quarter = df[(df['States'] == state) & (df['Quarter'] == quarter)]
                fig = px.bar(df_state_quarter, x='Transaction_type', y='Total_Transactions', color='User_Type',
                             title=f'Transaction Behavior in {state}, Q{quarter}',
                             labels={'Transaction_type': 'Transaction Type', 'Total_Transactions': 'Total Transactions'},
                             barmode='group')
                fig.update_layout(
                    xaxis_title='Transaction Type',
                    yaxis_title='Total Transactions',
                    title_font_size=24,
                    legend_title_font_size=18,
                    legend_font_size=16,
                    margin=dict(l=20, r=20, t=50, b=20),
                    uniformtext_minsize=12, 
                    uniformtext_mode='hide',
                    height=600,
                    width=1000,
                )
                st.subheader(f'State: {state}, Quarter: {quarter}')
                st.plotly_chart(fig)
    else:
        st.write("No data available for comparing transaction behavior.")
    return data, columns


#----------------DIFFERENT-STATES----------------
def different_states(cursor):
    query = """
        SELECT States, CONCAT(Years, '-', Quarter) AS Period,
               SUM(Transaction_count) AS Total_Transactions,
               SUM(Transaction_amount) AS Total_Amount
        FROM aggregated_transaction
        GROUP BY States, Years, Quarter
        ORDER BY States, Years, Quarter
    """
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    st.title('Transaction Volumes and Amounts Across Different States')
    for state in df['States'].unique():
        df_state = df[df['States'] == state]
        fig = px.line(df_state, x='Period', y='Total_Transactions', 
                      title=f'Transaction Volumes Over Time - {state}',
                      labels={'Period': 'Period', 'Total_Transactions': 'Total Transactions'})
        fig.add_scatter(x=df_state['Period'], y=df_state['Total_Amount'], 
                        mode='lines', name='Total Amount')
        fig.update_layout(xaxis_title='Period', yaxis_title='Total Transactions/Amount',
                          legend_title='Metric', legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1))
        st.plotly_chart(fig)
    return data, columns


#-----------------QUERY-PATH------------------
def query_part():
    st.title("Query Part")
    queries = {
        "What are the total transaction counts and amounts per state, year, and quarter?": total_transaction_count,
        "Which transaction types (e.g., peer, recharge) dominate each state?": transaction_type,
        "How does user engagement (e.g., app opens, registered users) vary by state and quarter?": user_engagement,
        "Which brands or categories have the highest transaction percentages in each state and quarter?": brands_categories,
        "What is the average insurance amount per state and quarter?": avg_insurance,
        "Which insurance types (e.g., life, health) show the highest transaction counts and amounts annually?": insurance_types,
        "How do transaction patterns vary across districts within each state and quarter?": transaction_patterns,
        "Which pincodes contribute the most to transaction counts and amounts in each state and quarter?": pincode_contribution,
        "Compare the transaction behavior between top users and regular users across different states and quarters.": top_users,
        "How do transaction volumes and amounts vary seasonally across different states?": different_states
    }
    selected_query = st.selectbox("Select a box to execute", list(queries.keys()))
    if st.button("Execute"):
        results, columns = queries[selected_query](cursor)
        mycon.close()
        st.success("Fetched Successfully!!!")   


#---------------MAIN-FUNCTION-----------------
def main():
    app_mode = st.sidebar.radio("Go to", ("Homepage", "Data Exploration", "Query Part"))
    if app_mode=="Homepage":
        homepage()
    elif app_mode=="Data Exploration":
        data_exploration()
    elif app_mode=="Query Part":
        query_part()


#---------------EXECUTE-MAIN-FUNCTION--------------
mycon = get_mysql_connection()
cursor = mycon.cursor()
main()