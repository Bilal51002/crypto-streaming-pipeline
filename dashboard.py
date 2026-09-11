import time

import pandas as pd
import streamlit as st
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.io.libevreactor import LibevConnection

Cluster.connection_class = LibevConnection


@st.cache_resource
def get_cassandra_session():
    cluster = Cluster(["cassandra"], port=9042)

    last_error = None
    for attempt in range(20):
        try:
            session = cluster.connect("crypto_keyspace")
            return session
        except NoHostAvailable as e:
            last_error = e
            time.sleep(5)

    raise last_error


def get_latest_prices(session, symbol, limit=50):
    query = f"""
        SELECT symbol, fetched_at, price
        FROM prices
        WHERE symbol = %s
        ORDER BY fetched_at DESC
        LIMIT {limit}
    """
    rows = session.execute(query, (symbol,))
    df = pd.DataFrame(rows, columns=["symbol", "fetched_at", "price"])
    return df.sort_values("fetched_at")


st.set_page_config(page_title="Crypto Live Dashboard", layout="wide")
st.title("📈 Crypto Prices — Live Dashboard")

session = get_cassandra_session()

symbol = st.selectbox("Choisis une crypto", ["BTCUSDT", "ETHUSDT", "SOLUSDT"])

df = get_latest_prices(session, symbol)

if df.empty:
    st.warning("Aucune donnée pour le moment. Vérifie que le pipeline tourne bien.")
else:
    col1, col2 = st.columns(2)
    col1.metric("Dernier prix", f"${df['price'].iloc[-1]:,.2f}")
    col2.metric(
        "Variation (fenêtre affichée)",
        f"{df['price'].iloc[-1] - df['price'].iloc[0]:+.2f}",
    )

    st.line_chart(df.set_index("fetched_at")["price"])
    st.dataframe(df.sort_values("fetched_at", ascending=False), use_container_width=True)

time.sleep(5)
st.rerun()
