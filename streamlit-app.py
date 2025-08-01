import streamlit as st
import time
import psycopg2
from kafka.consumer import KafkaConsumer
import simplejson as json
import pandas as pd

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
    messages = consumer.poll(timeout_ms=1000)
    data = []

    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)

    return data

def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    voters_count, candidates_count = fetch_voting_stats()
    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

    consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    data = fetch_data_from_kafka(consumer)
    results = pd.DataFrame(data)

    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    st.markdown("""---""")
    st.header("Leading Candidate")
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader(f"Total Vote: {leading_candidate['total_votes']}")


if __name__ == "__main__":
    st.title("Realtime Election Voting Dashboard")
    update_data()