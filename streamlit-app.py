import streamlit as st
import time
import psycopg2
from kafka.consumer import KafkaConsumer
import simplejson as json

@st.cache_data
def fetch_voting_stats():
    connection = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cursor = connection.cursor()

    cursor.execute("""
        SELECT COUNT(*) AS voters_count FROM voters
    """)

    voters_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) AS candidates_count FROM candidates
    """)

    candidates_count = cursor.fetchone()[0]

    return voters_count, candidates_count

def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    return consumer

def fetch_data_from_kafka(consumer):
    # messages = consumer.poll(timeout_ms=1000)
    pass

def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    voters_count, candidates_count = fetch_voting_stats()
    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

if __name__ == "__main__":
    st.title("Realtime Election Voting Dashboard")
    update_data()