import streamlit as st
import duckdb
import pandas as pd

# -----------------------------
# Page Configuration
# -----------------------------
st.set_page_config(
    page_title="Growth Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -----------------------------
# DB Connection
# -----------------------------
con = duckdb.connect("db/hm.duckdb")

# -----------------------------
# UI Filters
# -----------------------------
def load_filter_options():
    df = con.execute("""
        SELECT DISTINCT country, transaction_category
        FROM s_active_users_d
    """).fetchdf()
    return sorted(df["country"].unique()), sorted(df["transaction_category"].unique())

def render_filters(countries, categories):
    st.markdown("### 🔍 Filters")
    fcol1, fcol2 = st.columns(2)
    with fcol1:
        selected_country = st.selectbox("Country", countries, index=countries.index('all'))
    with fcol2:
        selected_category = st.selectbox("Transaction Category", categories, index=categories.index('all'))
    return selected_country, selected_category

def make_where_clause(country, category):
    return f"""
        WHERE country = '{country}' AND transaction_category = '{category}'
    """

# -----------------------------
# Load Active User Metrics
# -----------------------------
def load_active_users(where_clause):
    return {
        "dau": con.execute(f"""
            SELECT event_date_utc AS activity_date, dau FROM s_active_users_d {where_clause} ORDER BY event_date_utc
        """).fetchdf(),
        "wau": con.execute(f"""
            SELECT concat(week,'(',week_start_date,')') as week, wau FROM s_active_users_w {where_clause} ORDER BY week
        """).fetchdf(),
        "mau": con.execute(f"""
            SELECT month, mau FROM s_active_users_m {where_clause} ORDER BY month
        """).fetchdf(),
    }

# -----------------------------
# Load Retention Data
# -----------------------------
def load_retention_data():
    monthly_table = con.execute("""
        SELECT month_start, new_user_count, retained_user_count, resurrected_user_count, churned_user_count FROM s_user_retention_m ORDER BY month_start DESC
    """).fetchdf()
    weekly_table = con.execute("""
        SELECT week_start, new_user_count, retained_user_count, resurrected_user_count, churned_user_count FROM s_user_retention_w ORDER BY week_start DESC
    """).fetchdf()

    monthly_triangle = con.execute("""
        SELECT cohort_month, month_offset, retained_users, retention_rate FROM s_user_retention_triangle_m ORDER BY cohort_month, month_offset
    """).fetchdf()
    weekly_triangle = con.execute("""
        SELECT cohort_week, week_offset, retained_users, retention_rate FROM s_user_retention_triangle_w ORDER BY cohort_week, week_offset
    """).fetchdf()

    return monthly_table, weekly_table, monthly_triangle, weekly_triangle

def create_triangle_display(triangle_df, cohort_col, offset_col):
    pivot_rate = triangle_df.pivot(index=cohort_col, columns=offset_col, values="retention_rate").fillna(0)
    pivot_users = triangle_df.pivot(index=cohort_col, columns=offset_col, values="retained_users").fillna(0)

    display = pivot_rate.copy()
    for i in pivot_rate.index:
        for j in pivot_rate.columns:
            rate = pivot_rate.loc[i, j]
            count = int(pivot_users.loc[i, j])
            display.loc[i, j] = f"{rate:.0%} ({count})"
    return display

# -----------------------------
# Main App Logic
# -----------------------------
st.title("📈 Growth Metrics Dashboard")

countries, categories = load_filter_options()
selected_country, selected_category = render_filters(countries, categories)
where_clause = make_where_clause(selected_country, selected_category)

# Active User Charts
metrics = load_active_users(where_clause)
col1, col2, col3 = st.columns(3)
with col1:
    st.subheader("DAU")
    st.line_chart(metrics["dau"].rename(columns={"activity_date": "Date"}).set_index("Date"))
with col2:
    st.subheader("WAU")
    st.line_chart(metrics["wau"].rename(columns={"week": "Week"}).set_index("Week"))
with col3:
    st.subheader("MAU")
    st.line_chart(metrics["mau"].rename(columns={"month": "Month"}).set_index("Month"))

# Retention
monthly_df, weekly_df, monthly_tri_df, weekly_tri_df = load_retention_data()

st.markdown("### Monthly Retention")
mcol1, mcol2 = st.columns([6, 4])
with mcol1:
    st.dataframe(monthly_df, use_container_width=True)
with mcol2:
    st.dataframe(create_triangle_display(monthly_tri_df, "cohort_month", "month_offset"), use_container_width=True)

st.markdown("### Weekly Retention")
wcol1, wcol2 = st.columns([6, 4])
with wcol1:
    st.dataframe(weekly_df, use_container_width=True)
with wcol2:
    st.dataframe(create_triangle_display(weekly_tri_df, "cohort_week", "week_offset"), use_container_width=True)
