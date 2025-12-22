import streamlit as st
import snowflake.connector
import plotly.express as px
import pandas as pd

# 1. Page Configuration
st.set_page_config(layout="wide", page_title="Walmart Sales Analysis")
st.title("📊 Walmart Sales Performance Analytics")

# 2. Connection function using Streamlit Cloud Secrets
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"],
        client_session_keep_alive=True
    )

conn = init_connection()

# 3. Data Loading (Fetching from your Fact and Dim tables)
@st.cache_data
def load_walmart_data():
    query = """
    SELECT 
        F.STORE_ID, F.DEPT_ID, F.STORE_SIZE, F.STORE_WEEKLY_SALES,
        F.TEMPERATURE, F.FUEL_PRICE, F.UNEMPLOYMENT, F.CPI,
        F.MARKDOWN1, F.MARKDOWN2, F.MARKDOWN3, F.MARKDOWN4, F.MARKDOWN5,
        D.STORE_DATE,
        D.ISHOLIDAY AS HOLIDAY_FLAG,
        YEAR(D.STORE_DATE) AS YEAR,
        MONTHNAME(D.STORE_DATE) AS MONTH_NAME,
        MONTH(D.STORE_DATE) AS MONTH_NUM,
        S.STORE_TYPE
    FROM WALMART_DB.ANALYTICS.WALMART_FACT_TABLE F
    JOIN WALMART_DB.ANALYTICS.WALMART_DATE_DIM D ON F.DATE_ID = D.DATE_ID
    JOIN WALMART_DB.ANALYTICS.WALMART_STORE_DIM S ON F.STORE_ID = S.STORE_ID AND F.DEPT_ID = S.DEPT_ID
    WHERE F.VRSN_END_DATE IS NULL
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()

try:
    df_raw = load_walmart_data()
    
    # Global Filter for Performance
    st.sidebar.header("Dashboard Filter")
    years = sorted(df_raw['YEAR'].unique())
    sel_year = st.sidebar.selectbox("Choose Year", years)
    df = df_raw[df_raw['YEAR'] == sel_year].copy()

    # --- THE 10 REPORTS ---
    st.subheader("1. Weekly Sales by Store and Holiday")
    st.plotly_chart(px.bar(df, x="STORE_ID", y="STORE_WEEKLY_SALES", color="HOLIDAY_FLAG", barmode="group"), use_container_width=True)

    st.subheader("2. Weekly Sales by Temperature")
    st.plotly_chart(px.scatter(df, x="TEMPERATURE", y="STORE_WEEKLY_SALES", render_mode="svg"), use_container_width=True)

    st.subheader("3. Weekly Sales by Store Size")
    st.plotly_chart(px.scatter(df, x="STORE_SIZE", y="STORE_WEEKLY_SALES", color="STORE_TYPE"), use_container_width=True)

    st.subheader("4. Weekly Sales by Store Type and Month")
    m_sales = df.groupby(['MONTH_NAME', 'STORE_TYPE', 'MONTH_NUM'])['STORE_WEEKLY_SALES'].sum().reset_index().sort_values('MONTH_NUM')
    st.plotly_chart(px.line(m_sales, x="MONTH_NAME", y="STORE_WEEKLY_SALES", color="STORE_TYPE"), use_container_width=True)

    st.subheader("5. Markdown Sales by Store")
    df['TOTAL_MARKDOWNS'] = df[['MARKDOWN1','MARKDOWN2','MARKDOWN3','MARKDOWN4','MARKDOWN5']].fillna(0).sum(axis=1)
    st.plotly_chart(px.bar(df, x="STORE_ID", y="TOTAL_MARKDOWNS"), use_container_width=True)

    st.subheader("6. Weekly Sales by Store Type")
    st.plotly_chart(px.pie(df, values='STORE_WEEKLY_SALES', names='STORE_TYPE', hole=0.4), use_container_width=True)

    st.subheader("7. Fuel Price Distribution")
    st.plotly_chart(px.box(df, y="FUEL_PRICE"), use_container_width=True)

    st.subheader("8. Sales Timeline")
    timeline = df.groupby('STORE_DATE')['STORE_WEEKLY_SALES'].sum().reset_index()
    st.plotly_chart(px.line(timeline, x="STORE_DATE", y="STORE_WEEKLY_SALES"), use_container_width=True)

    st.subheader("9. Weekly Sales by CPI")
    st.plotly_chart(px.scatter(df, x="CPI", y="STORE_WEEKLY_SALES", opacity=0.3, render_mode="svg"), use_container_width=True)

    st.subheader("10. Department Wise Weekly Sales")
    d_sales = df.groupby('DEPT_ID')['STORE_WEEKLY_SALES'].sum().reset_index().sort_values('STORE_WEEKLY_SALES', ascending=False)
    st.plotly_chart(px.bar(d_sales, x="DEPT_ID", y="STORE_WEEKLY_SALES"), use_container_width=True)

except Exception as e:
    st.error(f"Error connecting to Snowflake: {e}")