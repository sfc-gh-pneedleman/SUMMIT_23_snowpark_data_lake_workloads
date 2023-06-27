import streamlit as st

import pandas as pd
import numpy as np
from pandas.tseries.offsets import DateOffset
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import avg, sum, col, to_date

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

st.set_page_config(page_title="Simple Recommendation App", page_icon="⚛️", layout="wide")
st.header("Simple Recommendation App")

@st.cache_data
def get_curated_sales_data():
    session = get_active_session()
    table_name = "SUMMIT_23_CURATED_DB.SALES_PREDICTION.PRODUCT_SALES"
    df = session.sql(f"""
        SELECT CUSTOMER_ID,  LAST_NAME || ', ' || FIRST_NAME AS CUST_NAME, MAIN_CATEGORY, SUB_CATEGORY, TITLE, PRODUCT_ID FROM  {table_name} 
            WHERE CUSTOMER_ID IN (
                SELECT customer_id
                FROM
                    {table_name}
                WHERE  PRODUCT_ID LIKE 'b%'
                GROUP BY customer_id
                HAVING COUNT(*) >= 20
          )
        LIMIT 1000000
        """
    )
    df_pandas = df.to_pandas()
    st.success("Loaded customer sales data successfully...")

    return df_pandas

@st.cache_data
def prepare_recommendation_candidates():
    curated_sales_events = get_curated_sales_data()
    selected_customers = np.random.choice(curated_sales_events['CUSTOMER_ID'].unique(), size=1000, replace=False)
    selected_data = curated_sales_events[curated_sales_events['CUSTOMER_ID'].isin(selected_customers)].reset_index(drop=True)
    selected_data['PRODUCT_TEXT'] = selected_data['TITLE'] + ' _XYZ_ ' + selected_data['PRODUCT_ID']
    vectorizer = TfidfVectorizer()
    with st.spinner(text="Computing product similarity ..."):
        product_features = vectorizer.fit_transform(selected_data['PRODUCT_TEXT'].values.astype('U'))
        # Compute the cosine similarity matrix between products
        cosine_sim = cosine_similarity(product_features, product_features)
    st.success("Computed product similarity matrix...")
    return selected_customers, selected_data, cosine_sim

def compute_recommendations(customer_id, selected_data, cosine_sim, top_n=5):
    # Filter selected_data by customer_id
    customer_data = selected_data[selected_data['CUSTOMER_ID'] == customer_id]

    # Get the row index of the customer in selected_data
    customer_row_index = customer_data.index[0]
        
    # Get the cosine similarity scores for the customer
    customer_cosine_scores = cosine_sim[customer_row_index]

    # Initialize an empty DataFrame to store recommendations
    recommended_products = pd.DataFrame(columns=selected_data.columns)

    # Get the main categories the customer had bought before
    customer_main_category = customer_data['MAIN_CATEGORY'].unique()
    
    # st.write(customer_main_category)
    # Generate recommendations for each main category
    for main_category in customer_main_category:
        # Filter selected_data by customer_id and main_category
        category_data = selected_data[(selected_data['MAIN_CATEGORY'] == main_category) & (selected_data['TITLE'] != 'N/A')]

        # Get the row indices for the main_category
        category_row_indices = category_data.index
        # st.write(main_category)
        # st.write(category_row_indices)
        # Sort the row indices based on cosine similarity scores
        sorted_indices = sorted(category_row_indices, key=lambda x: customer_cosine_scores[x], reverse=True)

        # Select the top indices (up to top_n) for the recommendations
        top_indices = sorted_indices[:top_n]

        # Get the top recommendations in the main_category
        category_recommendations = selected_data.loc[top_indices].copy()

        # Remove products already bought by the customer from recommendations
        category_recommendations = category_recommendations[~category_recommendations['PRODUCT_ID'].isin(customer_data['PRODUCT_ID'])]

        # Append the recommendations to the overall recommended products DataFrame
        recommended_products = recommended_products.append(category_recommendations)

    recommended_product_ids = recommended_products['PRODUCT_ID'].str.split(' _XYZ_ ').str[-1]
    selected_rows = selected_data[selected_data['PRODUCT_ID'].isin(recommended_product_ids)]
    recommended_products = selected_rows[['MAIN_CATEGORY', 'SUB_CATEGORY', 'TITLE', 'PRODUCT_ID']].drop_duplicates()
    customer_data = customer_data[['PRODUCT_ID', 'TITLE', 'MAIN_CATEGORY', 'SUB_CATEGORY']]
    customer_data = customer_data[customer_data['TITLE']!='N/A']
    return customer_data[['PRODUCT_ID', 'TITLE', 'MAIN_CATEGORY', 'SUB_CATEGORY']], recommended_products[['PRODUCT_ID', 'TITLE', 'MAIN_CATEGORY', 'SUB_CATEGORY']]

def simple_recommendation_app(selected_customers, selected_data, cosine_sim):
    # Select a list of 10 customers to show in the drop-down for whom we'll check out recommendations
    customer_id = st.selectbox("Select a customer:", selected_customers[:10])
    top_n = st.slider('Select the number of recommendations/category', 1, 10, 3)
    previous_products, recommendations = compute_recommendations(customer_id, selected_data, cosine_sim, top_n)
    previous_products = previous_products.reset_index(drop=True)
    recommendations = recommendations.reset_index(drop=True)
    st.subheader("Previously Bought Products:")
    st.write(previous_products)
    
    st.subheader("Recommended Products:")
    st.write(recommendations)
    
    st.success("Done with recommendation ... ")

if __name__ == "__main__":
    st.title("Product Recommendation App")
    with st.spinner('Preparing Recommendation Model App'):
        selected_customers, selected_data, cosine_sim = prepare_recommendation_candidates()
    with st.spinner('Refreshing Recommendation Model App'):
        simple_recommendation_app(selected_customers, selected_data, cosine_sim)
        st.snow()
        st.balloons()