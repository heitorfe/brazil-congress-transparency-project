# Query DuckDB for ad-hoc analysis and debugging. Not intended for production use.
import streamlit as st

from queries import adhoc_query
from queries import list_tables

with st.sidebar:
    st.header("Available Tables")
    tables = list_tables()
    selected_table = st.selectbox("Select a table", tables)
    if st.button("Use Table"):
        st.session_state["sql"] = f"SELECT * FROM {selected_table}"


st.set_page_config(
    page_title="Ad Hoc SQL Query",
    page_icon="🧪",
    layout="wide"
)


sql = st.text_area("SQL Query", height=200, value="SELECT * FROM main_marts.dim_senador LIMIT 10")
if st.button("Run Query"):
    try:
        df = adhoc_query(sql)
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error running query: {e}")
