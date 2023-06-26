import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
import pandas as pd

st.set_page_config(page_title="Unpaid Invoice Analysis", page_icon="💡", layout="wide")
st.header("Unpaid Invoice Analysis")


session = get_active_session()
df = session.sql("""SELECT LAST_NAME ||', ' || FIRST_NAME || ' (' ||  C.CUSTOMER_ID || ')' AS "Customer" , C.CUSTOMER_ID, 
INV_STATUS, TOTAL as "Invoice Total", C.HOME_PHONE, C.WORK_PHONE, C.CELL_PHONE, C.STREET,  C.CITY, C.STATE 
FROM SUMMIT_23_PROCESSED_DB.INVOICE.INVOICE_DETAILS I, SUMMIT_23_PROCESSED_DB.CUSTOMER.CUSTOMER C   
WHERE I.CUSTOMER_ID = C.CUSTOMER_ID
 AND INV_STATUS = 'Overdue' ORDER BY TOTAL DESC""")


df_pandas = df.to_pandas()


state_df = df_pandas['STATE'].unique()
state_df = pd.DataFrame(state_df,columns =['STATE'])
state_df = state_df.sort_values(by='STATE')


container = st.container()
all = st.checkbox("Select all", value=True)
 
if all:
    selected_options = container.multiselect("Select one or more options:",
         state_df['STATE'],state_df['STATE'])
else:
    selected_options =  container.multiselect("Select one or more options:",
        state_df)


df_pandas = df_pandas.loc[df_pandas['STATE'].isin(selected_options)]


st.title('Inpaid Invoices by Customer')
c = alt.Chart(df_pandas).mark_bar().encode(
   x=alt.X('Customer:N', sort=None), y='Invoice Total:Q', color='Invoice Total:Q'
    
).transform_window(
    rank='rank(Invoice Total)',
    sort=[alt.SortField('Invoice Total', order='descending')]
).transform_filter(
    (alt.datum.rank < 20)
)

st.altair_chart(c, use_container_width=True)

st.dataframe(df_pandas)