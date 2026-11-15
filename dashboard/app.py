import os
import pandas as pd
import plotly.express as px
import streamlit as st

RES = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'results')
BRACKETS = ['<1000', '1000-1399', '1400-1799', '1800-2199', '2200+']
CLOCKS = ['0-9s', '10-29s', '30-59s', '60-179s', '180s+']

st.set_page_config(page_title='Chess Game Analytics', layout='wide')


@st.cache_data
def load(name):
    try:
        return pd.read_csv(os.path.join(RES, name + '.csv'))
    except (FileNotFoundError, pd.errors.EmptyDataError):
        return pd.DataFrame()


def need(df, label):
    if df.empty:
        st.warning('No data for ' + label + ' yet. Run the Spark job and export the results.')
        st.stop()


st.title('Chess Game Analytics: Hadoop + Spark on Lichess data')
page = st.sidebar.radio('Page', ['Overview', 'Openings', 'Game length',
                                 'Colour and rating', 'Time pressure', 'Cluster'])

if page == 'Overview':
    s = load('summary')
    need(s, 'the summary')
    r = s.iloc[0]
    c = st.columns(5)
    c[0].metric('Games analysed', f'{int(r.games):,}')
    c[1].metric('Average moves', r.avg_moves)
    c[2].metric('White wins %', r.white_pct)
    c[3].metric('Draws %', r.draw_pct)
    c[4].metric('Black wins %', r.black_pct)

    h = load('hourly_activity')
    if not h.empty:
        st.plotly_chart(px.bar(h, x='hour', y='games', title='Games by hour of day (UTC)'),
                        use_container_width=True)

    t = load('terminations')
    if not t.empty:
        st.plotly_chart(px.bar(t, x='speed', y='games', color='termination',
                               title='How games ended'), use_container_width=True)

elif page == 'Openings':
    df = load('opening_winrates')
    need(df, 'openings')
    b = st.selectbox('Rating bracket', BRACKETS)
    n = st.slider('Number of openings', 5, 30, 15)
    d = df[df.elo_bracket == b].sort_values('games', ascending=False).head(n)
    st.plotly_chart(px.bar(d, x='opening_family', y=['white_pct', 'draw_pct', 'black_pct'],
                           barmode='stack', title='Result split of the most played openings (%)'),
                    use_container_width=True)
    st.dataframe(d, use_container_width=True)

    pop = load('opening_popularity')
    if not pop.empty:
        st.subheader('Most popular openings overall')
        st.plotly_chart(px.bar(pop.head(15), x='opening_family', y='games'),
                        use_container_width=True)

elif page == 'Game length':
    df = load('game_length')
    need(df, 'game length')
    st.plotly_chart(px.bar(df, x='speed', y='avg_moves', title='Average number of moves'),
                    use_container_width=True)
    st.dataframe(df, use_container_width=True)

elif page == 'Colour and rating':
    c = load('color_by_bracket')
    need(c, 'colour advantage')
    c['elo_bracket'] = pd.Categorical(c['elo_bracket'], BRACKETS, ordered=True)
    c = c.sort_values('elo_bracket')
    st.plotly_chart(px.bar(c, x='elo_bracket', y=['white_pct', 'draw_pct', 'black_pct'],
                           barmode='group', title='Results by rating bracket (%)'),
                    use_container_width=True)

    u = load('rating_upsets')
    if not u.empty:
        st.plotly_chart(px.line(u, x='elo_diff_bucket',
                                y=['actual_white_score', 'expected_white_score'], markers=True,
                                title='White score vs rating difference: actual and Elo-expected'),
                        use_container_width=True)

elif page == 'Time pressure':
    df = load('time_pressure')
    need(df, 'time pressure (this needs a month with clock and evaluation data)')
    df['clock_bucket'] = pd.Categorical(df['clock_bucket'], CLOCKS, ordered=True)
    df = df.sort_values(['speed', 'clock_bucket'])
    st.plotly_chart(px.line(df, x='clock_bucket', y='blunder_pct', color='speed', markers=True,
                            title='Blunder rate by time left on the clock (%)'),
                    use_container_width=True)
    st.dataframe(df, use_container_width=True)

else:  # Cluster
    st.markdown('**Pipeline:** PGN → parser → HDFS → Spark on YARN → result CSVs → this dashboard')
    st.markdown('- HDFS NameNode UI: http://master:9870')
    st.markdown('- YARN ResourceManager UI: http://master:8088')
    st.markdown('- Spark History Server: http://master:18080')

