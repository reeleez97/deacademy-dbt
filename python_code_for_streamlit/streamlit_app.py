import streamlit as st
import snowflake.connector
import plotly.express as px
import pandas as pd

# 1. Page Configuration
st.set_page_config(layout="wide", page_title="Walmart Analytical Dashboard")
st.title("📊 Walmart Data Analysis: 10 Strategic Reports")

# 2. Connection Logic (Connecting to your Snowflake Account)
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"],
        client_session_keep_alive=True
    )

conn = init_connection()

# 3. Data Loading (Using your ANALYTICS schema tables)
@st.cache_data
def get_report_data():
    # We join the fact table with dims and filter for the active SCD2 record
    query = """
    SELECT 
        F.STORE_ID, F.DEPT_ID, F.STORE_SIZE, F.STORE_WEEKLY_SALES,
        F.TEMPERATURE, F.FUEL_PRICE, F.UNEMPLOYMENT, F.CPI,
        F.MARKDOWN1, F.MARKDOWN2, F.MARKDOWN3, F.MARKDOWN4, F.MARKDOWN5,
        D.STORE_DATE,
        D.ISHOLIDAY AS HOLIDAY_FLAG,
        S.STORE_TYPE,
        YEAR(D.STORE_DATE) AS YEAR_VAL,
        MONTHNAME(D.STORE_DATE) AS MONTH_NAME,
        MONTH(D.STORE_DATE) AS MONTH_NUM
    FROM WALMART_DB.ANALYTICS.WALMART_FACT_TABLE F
    JOIN WALMART_DB.ANALYTICS.WALMART_DATE_DIM D ON F.DATE_ID = D.DATE_ID
    JOIN WALMART_DB.ANALYTICS.WALMART_STORE_DIM S ON F.STORE_ID = S.STORE_ID AND F.DEPT_ID = S.DEPT_ID
    WHERE F.VRSN_END_DATE IS NULL -- Logic for SCD2: Pulling current records
    """
    with conn.cursor() as cur:
        cur.execute(query)
        df = cur.fetch_pandas_all()
    return df

try:
    df = get_report_data()

    # --- REPORT 1: Weekly sales by store and holiday ---
    st.header("1. Weekly Sales by Store and Holiday")
    fig1 = px.bar(df, x="STORE_ID", y="STORE_WEEKLY_SALES", color="HOLIDAY_FLAG", barmode="group")
    st.plotly_chart(fig1, use_container_width=True)

    # --- REPORT 2: Weekly sales by temperature and year ---
    st.header("2. Weekly Sales by Temperature and Year")
    fig2 = px.scatter(df, x="TEMPERATURE", y="STORE_WEEKLY_SALES", color="YEAR_VAL")
    st.plotly_chart(fig2, use_container_width=True)

    # --- REPORT 3: Weekly sales by store size ---
    st.header("3. Weekly Sales by Store Size")
    fig3 = px.scatter(df, x="STORE_SIZE", y="STORE_WEEKLY_SALES", color="STORE_TYPE")
    st.plotly_chart(fig3, use_container_width=True)

    # --- REPORT 4: Weekly sales by store type and month ---
    st.header("4. Weekly Sales by Store Type and Month")
    # Grouping to get aggregated monthly sales
    monthly_sales = df.groupby(['MONTH_NAME', 'STORE_TYPE', 'MONTH_NUM'])['STORE_WEEKLY_SALES'].sum().reset_index().sort_values('MONTH_NUM')
    fig4 = px.line(monthly_sales, x="MONTH_NAME", y="STORE_WEEKLY_SALES", color="STORE_TYPE")
    st.plotly_chart(fig4, use_container_width=True)

    # --- REPORT 5: Markdown sales by year and store ---
    st.header("5. Markdown Sales by Year and Store")
    df['TOTAL_MARKDOWNS'] = df[['MARKDOWN1','MARKDOWN2','MARKDOWN3','MARKDOWN4','MARKDOWN5']].fillna(0).sum(axis=1)
    fig5 = px.bar(df, x="YEAR_VAL", y="STORE_ID", color="TOTAL_MARKDOWNS")
    st.plotly_chart(fig5, use_container_width=True)

    # --- REPORT 6: Weekly sales by store type ---
    st.header("6. Weekly Sales by Store Type")
    fig6 = px.pie(df, values='STORE_WEEKLY_SALES', names='STORE_TYPE', hole=0.4)
    st.plotly_chart(fig6, use_container_width=True)

    # --- REPORT 7: Fuel price by year ---
    st.header("7. Fuel Price by Year")
    fig7 = px.box(df, x="YEAR_VAL", y="FUEL_PRICE", color="YEAR_VAL")
    st.plotly_chart(fig7, use_container_width=True)

    # --- REPORT 8: Weekly sales by year, month and date ---
    st.header("8. Weekly Sales Trend (Timeline)")
    daily_trend = df.groupby(['STORE_DATE', 'YEAR_VAL'])['STORE_WEEKLY_SALES'].sum().reset_index()
    fig8 = px.line(daily_trend, x="STORE_DATE", y="STORE_WEEKLY_SALES", color="YEAR_VAL")
    st.plotly_chart(fig8, use_container_width=True)

    # --- REPORT 9: Weekly sales by CPI ---
    st.header("9. Weekly Sales by CPI")
    fig9 = px.scatter(df, x="CPI", y="STORE_WEEKLY_SALES", opacity=0.4)
    st.plotly_chart(fig9, use_container_width=True)

    # --- REPORT 10: Department wise weekly sales ---
    st.header("10. Department Wise Weekly Sales")
    dept_sales = df.groupby('DEPT_ID')['STORE_WEEKLY_SALES'].sum().reset_index().sort_values('STORE_WEEKLY_SALES', ascending=False)
    fig10 = px.bar(dept_sales, x="DEPT_ID", y="STORE_WEEKLY_SALES", color="STORE_WEEKLY_SALES")
    st.plotly_chart(fig10, use_container_width=True)

except Exception as e:
    st.error(f"Error connecting to Snowflake or processing data: {e}")


