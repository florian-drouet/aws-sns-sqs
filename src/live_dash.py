import time

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Live Cumulative Line Graph from API", layout="wide")

st.title("📊 Live Cumulative Line Chart (updates every 5 seconds from API)")

API_URL = "http://localhost:8000/aggregate"


def fetch_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        raw_data = response.json()  # List of [timestamp, value]

        # Convert to DataFrame
        df = pd.DataFrame(raw_data, columns=["Time", "Value"])
        df["Time"] = pd.to_datetime(df["Time"])
        df = df.sort_values("Time")

        # Apply cumulative sum
        df["Value"] = df["Value"].cumsum()
        return df
    except Exception as e:
        st.error(f"Failed to fetch data: {e}")
        return pd.DataFrame(columns=["Time", "Value"])


df = fetch_data()

if not df.empty:
    st.line_chart(df.set_index("Time"))

# Auto-refresh every 5 seconds
time.sleep(5)
st.rerun()
