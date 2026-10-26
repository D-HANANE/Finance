import os
import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st

st.set_page_config(page_title='Marché simulé', layout='wide')
st.title('Plateforme de marché financier simulé')
st.caption('Entreprises réelles, transactions et prix entièrement simulés. Heures en UTC.')


def query(sql, params=()):
    with psycopg2.connect(os.environ['PG_DSN']) as connection, connection.cursor() as cursor:
        cursor.execute(sql, params)
        return pd.DataFrame(cursor.fetchall(), columns=[c[0] for c in cursor.description])


@st.fragment(run_every='5s')
def dashboard():
    symbols = query('SELECT symbol FROM latest_prices ORDER BY symbol')
    if symbols.empty:
        st.info('En attente des premières transactions traitées par Spark.')
        return
    symbol = st.selectbox('Action', symbols['symbol'].tolist())
    candles = query('''SELECT * FROM candles WHERE symbol=%s
                      AND minute_start>now()-interval '1 day' ORDER BY minute_start''', (symbol,))
    if not candles.empty:
        st.subheader('Chandeliers en direct')
        st.caption('La minute courante est provisoire. Finalisation après le watermark de deux minutes ; les deux étapes de flux ajoutent un délai.')
        chart = go.Figure(go.Candlestick(x=candles['minute_start'], open=candles['open'],
                         high=candles['high'], low=candles['low'], close=candles['close']))
        st.plotly_chart(chart, use_container_width=True)
        st.dataframe(candles[['minute_start','close','volume','is_final']].tail(8), hide_index=True)
    live = query('''SELECT * FROM live_indicators WHERE symbol=%s
                   ORDER BY minute_start DESC LIMIT 1''', (symbol,))
    st.subheader('Indicateurs en direct, provisoires')
    st.dataframe(live, hide_index=True)
    st.caption('MA20 et MA50 : 20 et 50 minutes observées. VWAP : depuis minuit UTC.')
    st.subheader('Journal des alertes détectées')
    alerts = query('SELECT * FROM alerts ORDER BY event_time DESC LIMIT 100')
    if alerts.empty:
        st.info('Aucune alerte détectée. Il faut au moins cinq minutes de référence puis une variation strictement supérieure à 5 %.')
    else:
        st.dataframe(alerts, hide_index=True)
    st.subheader('Gold finalisé')
    runs = query('SELECT * FROM pipeline_runs ORDER BY completed_at DESC LIMIT 1')
    if runs.empty:
        st.info('Gold sera disponible après finalisation Silver et une exécution Dagster réussie.')
    else:
        st.dataframe(runs, hide_index=True)
        st.dataframe(query('SELECT * FROM gold_daily WHERE symbol=%s ORDER BY day DESC', (symbol,)), hide_index=True)
        gold = query('SELECT * FROM gold_minute WHERE symbol=%s ORDER BY minute_start', (symbol,))
        if not gold.empty:
            st.line_chart(gold.set_index('minute_start')[['close','ma20','ma50']])
        st.subheader('Corrélations des rendements minute')
        st.dataframe(query('''SELECT * FROM gold_correlations WHERE symbol_a=%s OR symbol_b=%s
                             ORDER BY symbol_a,symbol_b''', (symbol,symbol)), hide_index=True)


dashboard()
