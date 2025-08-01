import streamlit as st
import time
import psycopg2
from kafka.consumer import KafkaConsumer
import simplejson as json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from streamlit_autorefresh import st_autorefresh
import time

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

def sidebar():
    if st.session_state.get('latest_update') is None:
        st.session_state['last_update'] = time.time()
    
    refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key='auto')

def plot_colored_bar_chart(results):
    data_type = results['candidate_name']
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))
    plt.bar(data_type, results['total_votes'], colors)
    plt.xlabel("Candidate")
    plt.ylabel("Total Votes")
    plt.title("Vote Counts per Candidates")
    plt.xticks(rotation=90)
    return plt

def plot_pie_chart(data):
    labels = list(data['candidate_name'])
    sizes = list(data['total_votes'])
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%')
    ax.axis('equal')
    plt.title("Candidates Votes")
    return fig

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

    st.markdown("""---""")
    st.header('Voting Statistics')
    results = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']]
    results = results.reset_index(drop=True)
    
    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_colored_bar_chart(results)
        st.pyplot(bar_fig)

    with col2:
        pie_fig = plot_pie_chart(results)

if __name__ == "__main__":
    st.title("Realtime Election Voting Dashboard")
    sidebar()
    update_data()