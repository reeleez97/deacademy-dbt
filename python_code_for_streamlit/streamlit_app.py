import streamlit as st
import snowflake.connector
import plotly.express as px
import pandas as pd

st.set_page_config(layout="wide", page_title="Walmart Analytical Dashboard")

# Initialize connection
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"],
        client_session_keep_alive=True
    )

conn = init_connection()

# Load Data
@st.cache_data
def get_data():
    query = """
    SELECT f.*, d.STORE_DATE, d.ISHOLIDAY, s.STORE_TYPE
    FROM WALMART_DB.ANALYTICS.WALMART_FACT_TABLE f
    JOIN WALMART_DB.ANALYTICS.WALMART_DATE_DIM d ON f.DATE_ID = d.DATE_ID
    JOIN WALMART_DB.ANALYTICS.WALMART_STORE_DIM s ON f.STORE_ID = s.STORE_ID AND f.DEPT_ID = s.DEPT_ID
    WHERE f.is_current_version = TRUE
    """
    return pd.read_sql(query, conn)

df = get_data()
df['STORE_DATE'] = pd.to_datetime(df['STORE_DATE'])
df['YEAR'] = df['STORE_DATE'].dt.year
df['MONTH'] = df['STORE_DATE'].dt.month_name()

st.title("🛒 Walmart Performance Analytics")

# 1. Weekly sales by store and holiday
st.header("1. Weekly Sales by Store and Holiday")
fig1 = px.bar(df, x="STORE_ID", y="STORE_WEEKLY_SALES", color="ISHOLIDAY", barmode="group")
st.plotly_chart(fig1, use_container_width=True)

# 2. Weekly sales by temperature and year
st.header("2. Weekly Sales by Temperature and Year")
fig2 = px.scatter(df, x="TEMPERATURE", y="STORE_WEEKLY_SALES", color="YEAR")
st.plotly_chart(fig2, use_container_width=True)

# 3. Weekly sales by store size
st.header("3. Weekly Sales by Store Size")
fig3 = px.scatter(df, x="STORE_SIZE", y="STORE_WEEKLY_SALES", color="STORE_TYPE")
st.plotly_chart(fig3, use_container_width=True)

# 4. Weekly sales by store type and month
st.header("4. Weekly Sales by Store Type and Month")
fig4 = px.line(df.groupby(['MONTH', 'STORE_TYPE'])['STORE_WEEKLY_SALES'].sum().reset_index(), 
              x="MONTH", y="STORE_WEEKLY_SALES", color="STORE_TYPE")
st.plotly_chart(fig4, use_container_width=True)

# 5. Markdown sales by year and store
st.header("5. Markdown Sales by Year and Store")
df['TOTAL_MARKDOWN'] = df[['MARKDOWN1','MARKDOWN2','MARKDOWN3','MARKDOWN4','MARKDOWN5']].sum(axis=1)
fig5 = px.bar(df, x="STORE_ID", y="TOTAL_MARKDOWN", color="YEAR")
st.plotly_chart(fig5, use_container_width=True)

# 6. Weekly sales by store type
st.header("6. Weekly Sales by Store Type")
fig6 = px.pie(df, values="STORE_WEEKLY_SALES", names="STORE_TYPE")
st.plotly_chart(fig6, use_container_width=True)

# 7. Fuel price by year
st.header("7. Fuel Price by Year")
fig7 = px.box(df, x="YEAR", y="FUEL_PRICE")
st.plotly_chart(fig7, use_container_width=True)

# 8. Weekly sales by year, month and date
st.header("8. Weekly Sales Trend")
fig8 = px.line(df.sort_values('STORE_DATE'), x="STORE_DATE", y="STORE_WEEKLY_SALES")
st.plotly_chart(fig8, use_container_width=True)

# 9. Weekly sales by CPI
st.header("9. Weekly Sales by CPI")
fig9 = px.scatter(df, x="CPI", y="STORE_WEEKLY_SALES")
st.plotly_chart(fig9, use_container_width=True)

# 10. Department wise weekly sales
st.header("10. Department Wise Weekly Sales")
fig10 = px.bar(df.groupby('DEPT_ID')['STORE_WEEKLY_SALES'].sum().reset_index(), x="DEPT_ID", y="STORE_WEEKLY_SALES")
st.plotly_chart(fig10, use_container_width=True)
