import streamlit as st
import snowflake.connector
import plotly.express as px
import pandas as pd

# 1. Page Configuration
st.set_page_config(layout="wide", page_title="Walmart Analytical Dashboard")
st.title("🛒 Walmart Sales Performance Analytics")

# 2. Connection Logic (using Secrets from Streamlit Cloud)
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"],
        client_session_keep_alive=True
    )

conn = init_connection()

# 3. Data Loading (Corrected for Snowflake Connector)
@st.cache_data
def get_data():
    # Note: We use VRSN_END_DATE IS NULL to get the current SCD2 record
    query = """
    SELECT 
        F.*, 
        D.STORE_DATE, 
        D.ISHOLIDAY AS HOLIDAY_FLAG, 
        S.STORE_TYPE,
        YEAR(D.STORE_DATE) AS YEAR_VAL,
        MONTHNAME(D.STORE_DATE) AS MONTH_VAL,
        MONTH(D.STORE_DATE) as MONTH_NUM
    FROM WALMART_DB.ANALYTICS.WALMART_FACT_TABLE F
    JOIN WALMART_DB.ANALYTICS.WALMART_DATE_DIM D ON F.DATE_ID = D.DATE_ID
    JOIN WALMART_DB.ANALYTICS.WALMART_STORE_DIM S ON F.STORE_ID = S.STORE_ID AND F.DEPT_ID = S.DEPT_ID
    WHERE F.VRSN_END_DATE IS NULL
    """
    # Use the connector's native cursor to fetch data
    with conn.cursor() as cur:
        cur.execute(query)
        # This is the fastest way to get data into a DataFrame with Snowflake
        df = cur.fetch_pandas_all()
    return df

try:
    df_raw = get_data()
    
    # Sidebar Filter (Helps performance and user experience)
    st.sidebar.header("Filter Options")
    years = sorted(df_raw['YEAR_VAL'].unique())
    selected_year = st.sidebar.selectbox("Select Year", years)
    
    # Filtered dataframe for reports
    df = df_raw[df_raw['YEAR_VAL'] == selected_year].copy()

    # --- THE 10 REPORTS ---

    # 1. Weekly sales by store and holiday
    st.header("1. Weekly Sales by Store and Holiday")
    fig1 = px.bar(df, x="STORE_ID", y="STORE_WEEKLY_SALES", color="HOLIDAY_FLAG", barmode="group")
    st.plotly_chart(fig1, use_container_width=True)

    # 2. Weekly sales by temperature and year
    st.header("2. Weekly Sales by Temperature")
    fig2 = px.scatter(df, x="TEMPERATURE", y="STORE_WEEKLY_SALES")
    st.plotly_chart(fig2, use_container_width=True)

    # 3. Weekly sales by store size
    st.header("3. Weekly Sales by Store Size")
    fig3 = px.scatter(df, x="STORE_SIZE", y="STORE_WEEKLY_SALES", color="STORE_TYPE")
    st.plotly_chart(fig3, use_container_width=True)

    # 4. Weekly sales by store type and month
    st.header("4. Weekly Sales by Store Type and Month")
    # Grouping to ensure clean lines
    m_sales = df.groupby(['MONTH_VAL', 'STORE_TYPE', 'MONTH_NUM'])['STORE_WEEKLY_SALES'].sum().reset_index()
    m_sales = m_sales.sort_values('MONTH_NUM')
    fig4 = px.line(m_sales, x="MONTH_VAL", y="STORE_WEEKLY_SALES", color="STORE_TYPE")
    st.plotly_chart(fig4, use_container_width=True)

    # 5. Markdown sales by year and store
    st.header("5. Markdown Sales by Store")
    # Handling NULLs in markdowns before summing
    df['TOTAL_MARKDOWNS'] = df[['MARKDOWN1','MARKDOWN2','MARKDOWN3','MARKDOWN4','MARKDOWN5']].fillna(0).sum(axis=1)
    fig5 = px.bar(df, x="STORE_ID", y="TOTAL_MARKDOWNS")
    st.plotly_chart(fig5, use_container_width=True)

    # 6. Weekly sales by store type
    st.header("6. Weekly Sales by Store Type")
    fig6 = px.pie(df, values='STORE_WEEKLY_SALES', names='STORE_TYPE', hole=0.4)
    st.plotly_chart(fig6, use_container_width=True)

    # 7. Fuel price distribution
    st.header("7. Fuel Price Distribution")
    fig7 = px.box(df, y="FUEL_PRICE")
    st.plotly_chart(fig7, use_container_width=True)

    # 8. Weekly sales timeline
    st.header("8. Weekly Sales Trend over Date")
    timeline = df.groupby('STORE_DATE')['STORE_WEEKLY_SALES'].sum().reset_index()
    fig8 = px.line(timeline, x="STORE_DATE", y="STORE_WEEKLY_SALES")
    st.plotly_chart(fig8, use_container_width=True)

    # 9. Weekly sales by CPI
    st.header("9. Weekly Sales by CPI")
    fig9 = px.scatter(df, x="CPI", y="STORE_WEEKLY_SALES", opacity=0.5)
    st.plotly_chart(fig9, use_container_width=True)

    # 10. Department wise weekly sales
    st.header("10. Department Wise Weekly Sales")
    dept_sales = df.groupby('DEPT_ID')['STORE_WEEKLY_SALES'].sum().reset_index().sort_values('STORE_WEEKLY_SALES', ascending=False)
    fig10 = px.bar(dept_sales, x="DEPT_ID", y="STORE_WEEKLY_SALES")
    st.plotly_chart(fig10, use_container_width=True)

except Exception as e:
    st.error(f"Error loading report: {e}")
