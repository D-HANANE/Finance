import os
import pandas as pd
import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st
from market import SYMBOLS
from alerting import close_resolved

st.set_page_config(page_title='Marché simulé',layout='wide')
st.title('Analyse du marché financier simulé')
st.caption('Prix simulés ; heures UTC. Calculs Spark SQL dans Iceberg Gold, publication PostgreSQL pour Streamlit.')

def query(sql,params=()):
    with psycopg2.connect(os.environ['PG_DSN']) as pg,pg.cursor() as cur:
        cur.execute(sql,params)
        return pd.DataFrame(cur.fetchall(),columns=[c[0] for c in cur.description])

@st.cache_data(ttl=15)
def metrics(symbol):
    # One consistent transaction: no mixed publication while a batch commits.
    with psycopg2.connect(os.environ['PG_DSN']) as pg:
        pg.set_session(isolation_level='REPEATABLE READ',readonly=True)
        with pg.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('SELECT snapshots,published_at FROM gold_serving_publication WHERE id=1')
            publication=cur.fetchone()
            if not publication: return {'minute':[],'daily':[],'correlations':[],'published_at':None}
            cur.execute("""SELECT * FROM gold_minute WHERE symbol=%s AND source_snapshot IS NOT NULL
                ORDER BY minute_start DESC LIMIT 1440""",(symbol,))
            minute=[dict(row) for row in cur.fetchall()]
            cur.execute("""SELECT * FROM gold_daily WHERE symbol=%s AND source_snapshot IS NOT NULL
                ORDER BY day DESC LIMIT 30""",(symbol,))
            daily=[dict(row) for row in cur.fetchall()]
            cur.execute("""SELECT * FROM gold_correlations WHERE (symbol_a=%s OR symbol_b=%s)
                AND source_snapshot IS NOT NULL""",(symbol,symbol))
            correlations=[dict(row) for row in cur.fetchall()]
    return dict(minute=minute,daily=daily,correlations=correlations,
        published_at=publication['published_at'],snapshots=publication['snapshots'])

def formatted(value,percentage=False):
    if value is None or pd.isna(value): return 'Indisponible'
    return f'{float(value)*100:.3f} %' if percentage else f'{float(value):.4f}'

@st.fragment(run_every='5s')
def dashboard():
    available=query('SELECT symbol FROM latest_prices UNION SELECT symbol FROM gold_minute ORDER BY symbol')
    symbols=available['symbol'].tolist() or list(SYMBOLS)
    symbol=st.selectbox('Action',symbols)
    st.subheader('Métriques financières Gold')
    st.caption("MA20/MA50 : 20/50 clôtures minute observées. VWAP pondéré par quantité. Volatilité : écart type des log-rendements minute du jour, sans annualisation.")
    try:
        data=metrics(symbol)
        if data['minute']:
            minute=pd.DataFrame(data['minute']).sort_values('minute_start')
            daily=pd.DataFrame(data['daily']).sort_values('day')
            for name in ('close','ma20','ma50','vwap_day'):
                minute[name]=pd.to_numeric(minute[name],errors='coerce')
            st.caption('Action analysée : '+symbol)
            last=minute.iloc[-1];day=daily.iloc[-1] if not daily.empty else {}
            values=[('Daily Return',day.get('daily_return'),True),('Daily Volatility',day.get('daily_volatility'),True),
                    ('VWAP',last['vwap_day'],False),('MA20',last['ma20'],False),('MA50',last['ma50'],False)]
            for column,(label,value,percentage) in zip(st.columns(5),values):
                column.metric(label,formatted(value,percentage))
            if day.get('daily_return') is None or pd.isna(day.get('daily_return')):
                st.info("Daily Return indisponible : clôture du jour UTC précédent absente. Aucun zéro artificiel n'est inséré.")
            st.caption('Dernière minute : '+str(last['minute_start'])+' ; journée : '+str(day.get('day'))+
                       ' ; couverture : '+str(day.get('minutes'))+' minutes. La journée courante reste provisoire.')
            chart=go.Figure()
            for name in ('close','ma20','ma50','vwap_day'):
                chart.add_scatter(x=minute['minute_start'],y=minute[name],name=name,mode='lines',connectgaps=False)
            st.plotly_chart(chart,use_container_width=True)
            st.dataframe(minute[['minute_start','close','ma20','ma50','vwap_day','change_5m']].tail(10),hide_index=True)
            st.subheader('Indicateurs quotidiens')
            st.dataframe(daily[['day','close','daily_return','daily_volatility','vwap','minutes','is_complete']],hide_index=True)
            st.subheader('Corrélations des rendements minute')
            st.dataframe(pd.DataFrame(data['correlations']),hide_index=True)
            st.caption('Source réelle : PostgreSQL gold_minute / gold_daily, projection Iceberg Gold ; publication '+str(data['published_at']))
        else:
            st.info('Aucune publication Gold pour cette action. Exécuter lakehouse_refresh dans Dagster.')
    except Exception as error:
        st.warning('Lecture de Gold temporairement indisponible : '+str(error))
    st.subheader('Couverture des alertes par action')
    st.caption("Même règle pour tous les symboles : abs(variation sur cinq minutes) > 5 %. Zéro dépassement signifie qu'aucune alerte n'est requise ; une référence absente rend l'observation non évaluable.")
    coverage=query('SELECT * FROM alert_symbol_coverage ORDER BY symbol')
    st.dataframe(coverage,hide_index=True)
    selected=st.selectbox('Filtrer les alertes',['Toutes les actions']+symbols)
    params=() if selected=='Toutes les actions' else (selected,)
    predicate='' if not params else ' AND symbol=%s'
    st.subheader('Alertes actives')
    active=query("""SELECT alert_id,symbol,alert_type,description,opened_at,last_observed_at,
       observed_value,trigger_value,peak_value,threshold,status,price,reference_price
       FROM alerts WHERE status='active'"""+predicate+' ORDER BY last_observed_at DESC',params)
    if active.empty: st.info('Aucune alerte active pour cette sélection.')
    else: st.dataframe(active,hide_index=True)
    st.subheader('Historique des alertes')
    history=query("""SELECT alert_id,symbol,alert_type,opened_at,last_observed_at,updated_at,
       observed_value,threshold,status,origin FROM alerts WHERE true"""+predicate+' ORDER BY opened_at DESC LIMIT 500',params)
    st.dataframe(history,hide_index=True)
    changes=query("""SELECT h.alert_id,a.symbol,h.status,h.action,h.occurred_at,h.recorded_at
       FROM alert_history h JOIN alerts a USING(alert_id) WHERE true"""+
       ('' if not params else ' AND a.symbol=%s')+' ORDER BY h.history_id DESC LIMIT 200',params)
    st.dataframe(changes,hide_index=True)
    resolved=query("SELECT alert_id,symbol,opened_at FROM alerts WHERE status='resolved'"+predicate+' ORDER BY opened_at DESC',params)
    if not resolved.empty:
        chosen=st.selectbox('Alerte résolue à clôturer',resolved['alert_id'].astype(str).tolist())
        if st.button("Clôturer l'alerte résolue"):
            close_resolved(chosen)
            st.success('Clôture enregistrée dans PostgreSQL.')
            st.rerun()
    st.caption('Dernière évaluation des alertes — schedule toutes les cinq minutes')
    st.dataframe(query("SELECT source_cursor,source_snapshot,updated_at FROM alert_state WHERE name='gold_minute'"),hide_index=True)

dashboard()
