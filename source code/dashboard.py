import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import pandas_ta as ta
import streamlit.components.v1 as components
import time
import numpy as np
import druid_api
import utils
import subprocess
import os
from st_pages import Page, show_pages


st.set_page_config(
    layout="wide"
)

show_pages(
    [
        Page("dashboard.py", "Dashboard"),
        Page("pages/manage_indicators.py", "Manage your indicators"),
    ]
)

directory = os.path.dirname(__file__)
st.session_state.indicators = st.session_state.get("indicators", [])
num_rows = 1

get_cursor = st.cache_resource(druid_api.get_cursor) 
get_tickers = st.cache_resource(utils.get_tickers)


curs = get_cursor()
tickers = get_tickers()


def add_trace(traces, yaxis_title):
    global num_rows

    num_rows += 1

    if type(traces) != list:
        traces = [traces]

    for trace in traces:
        trace.yaxis = 'y' + str(num_rows)
        trace.legendgroup = str(num_rows)
        fig.add_trace(trace)

    fig.update_layout(grid=dict(rows=num_rows, columns=1))
    fig.layout.height += 200
    n = num_rows + 2
    res = {'yaxis': {'domain': [1 - 3 / n, 1]}}
    res.update({'yaxis' + str(i): {'domain': [1 - (3 + i - 1)/n, 1 -  (3 + i - 2) / n - 1 / 4 / n]} for i in range(2, num_rows + 1)})
    res['yaxis' + str(num_rows)]['title'] = yaxis_title
    fig.update_layout(res)

def custom_indicator_calculate(indicator, ticker):
    return subprocess.run(["python", os.path.join(directory, f"custom-indicators/{indicator}.py"), ticker], 
                            capture_output=True, text=True)

with st.sidebar:
    st.header("Dashboard")
    st.button("Update")
    ticker = st.selectbox("Ticker", tickers)

    st.subheader("Volume")
    is_volume = st.checkbox("Volume")

    st.subheader("SMA - Simple Moving Average")
    col1_3, col2_3, col3_3, col4_3 = st.columns(4)
    is_sma = col1_3.checkbox("SMA")
    sma_color = col2_3.color_picker("sma_color", "#00f900", label_visibility="collapsed")
    is_sma_on_candlestick = st.sidebar.checkbox("On candlestick chart", key='sma_on_candlestick_chart')
    sma_series_type = "close"
    sma_period = 10

    st.subheader("OBV - On Balance Volume")
    col1_1, col2_1, col3_1, col4_1 = st.columns(4)
    is_obv = col1_1.checkbox("OBV")
    obv_color = col2_1.color_picker("obv_color", "#00f900", label_visibility="collapsed")
    
    st.subheader("ADL - Accumulation/Distribution Line")
    col1_2, col2_2, col3_2, col4_2 = st.columns(4)
    is_adl = col1_2.checkbox("ADL")
    adl_color = col2_2.color_picker("adl_color", "#00f900", label_visibility="collapsed")

    st.subheader("Aroon")
    is_aroon = st.checkbox("Aroon")
    aroon_period = 25

    st.subheader("EMA - Exponential Moving Average")
    col1_4, col2_4, col3_4, col4_4 = st.columns(4)
    is_ema = col1_4.checkbox("EMA")
    ema_color = col2_4.color_picker("ema_color", "#00f900", label_visibility="collapsed")
    is_ema_on_candlestick = st.sidebar.checkbox("On candlestick chart", key='ema_on_candlestick_chart')
    ema_series_type = "close"
    ema_period = 10

    st.subheader("MACD - Moving Average Convergence Divergence")
    is_macd = st.checkbox("MACD")
    macd_series_type = "close"
    macd_fast_period = 12
    macd_slow_period = 26
    macd_signal_period = 9

    st.subheader("RSI - Relative Strength Index")
    is_rsi = st.checkbox("RSI")
    rsi_series_type = "close"
    rsi_period = 14

    st.subheader("Your Indicators")
    col1_5, col2_5, col3_5, col4_5 = st.columns(4)
    is_show = col1_5.checkbox("Show")
    color = col2_5.color_picker("color", "#00f900", label_visibility="collapsed")
    chosen_indicator = st.selectbox("indicator_name", st.session_state.indicators, label_visibility="collapsed")

while 'StockPrices' not in druid_api.get_data_sources():
    time.sleep(5)

ohlcv = druid_api.get_query_result(f"""
    SELECT *
    FROM StockPrices
    WHERE ticker='{ticker}'
""", curs)

layout =  go.Layout(height=600, 
                    plot_bgcolor='rgb(255,255,255)', 
                    xaxis_rangeslider_visible=False,
                    dragmode='pan',
                    hoversubplots="axis",
                    hovermode="x",
                    template="plotly"
                   )

config = {'scrollZoom': True}

ohlc_trace = go.Candlestick(x=ohlcv.time, 
                               open=ohlcv.open, 
                               high=ohlcv.high, 
                               low=ohlcv.low, 
                               close=ohlcv.close,
                               yaxis='y',
                               name='Candlestick',
                               )

data = [ohlc_trace]

fig = go.Figure(data=data, layout=layout)

if is_volume:
    volume_trace = go.Bar(x=ohlcv.time,
                            y=ohlcv.volume,
                            name='Volume',
                            marker=dict(color='#636EFA'))
    add_trace(volume_trace, "Volume")

if is_sma:
    while 'SMA' not in druid_api.get_data_sources():
        time.sleep(5)
    sma = []
    while len(sma) < len(ohlcv):
        sma = druid_api.get_query_result(f"""
            SELECT *
            FROM SMA
            WHERE ticker='{ticker}'
        """, curs)
        time.sleep(5)
    sma.loc[:(sma_period - 2), "sma"] = np.nan

    sma_trace = go.Scatter(x=ohlcv['time'],
                            y=sma['sma'],
                            line=dict(color=sma_color),
                            name=str(sma_period)+' Day SMA')
    if is_sma_on_candlestick:
        fig.add_trace(sma_trace)
    else:
        add_trace(sma_trace, "SMA")


if is_obv:
    obv = ta.obv(ohlcv["close"], ohlcv["volume"])
    obv_trace = go.Scatter(x=ohlcv.time,
                           y=obv,
                           line=dict(color=obv_color),
                           name="OBV")
    add_trace(obv_trace, "OBV")



if is_adl:
    adl = ta.ad(ohlcv["high"], ohlcv["low"], ohlcv["close"], ohlcv["volume"])
    adl_trace = go.Scatter(x=ohlcv.time,
                           y=adl,
                           line=dict(color=adl_color),
                           name="ADL")
    add_trace(adl_trace, "ADL")

if is_aroon:
    aroon = ta.aroon(ohlcv['high'], ohlcv['low'], 25)
    aroon_up_trace = go.Scatter(x=ohlcv.time,
                                y=aroon[f"AROONU_{aroon_period}"],
                                line=dict(color='green', width=1.5),
                                name="Aroon Up")
    aroon_down_trace = go.Scatter(x=ohlcv.time,
                                  y=aroon[f"AROOND_{aroon_period}"],
                                  line=dict(color='red', width=1.5),
                                  name="Aroon Down")
    
    aroon_oscillator_trace = go.Scatter(x=ohlcv.time,
                                        y=aroon[f"AROONOSC_{aroon_period}"],
                                        line=dict(color='black', width=1.5),
                                        name="Aroon Oscillator")
    baseline = go.Scatter(mode='lines',
                            x=[min(ohlcv.time), max(ohlcv.time)],
                            y=[0, 0],
                            line=dict(color='gray',
                                    width=0.5,
                                    dash='dashdot'),
                            name='baseline')
    add_trace([aroon_up_trace, aroon_down_trace], "Aroon")
    add_trace([aroon_oscillator_trace, baseline], "Aroon")


if is_ema:
    ema = ta.ema(ohlcv[ema_series_type], ema_period)
    ema_trace = go.Scatter(x=ohlcv.time,
                            y=ema,
                            line=dict(color=ema_color),
                            name=str(ema_period)+' Day EMA')
    if is_ema_on_candlestick:
        fig.add_trace(ema_trace)
    else:
        add_trace(ema_trace, "EMA")

if is_macd:

    macd = ta.macd(ohlcv[macd_series_type], macd_fast_period, macd_slow_period, macd_signal_period)

    macd_trace =  go.Scatter(x=ohlcv.time,
                        y=macd[f'MACD_{macd_fast_period}_{macd_slow_period}_{macd_signal_period}'],
                        name='MACD',
                        line=dict(color='black', width=1.5))
    
    macdh_trace = go.Bar(x=ohlcv.time,
                        y=macd[f'MACDh_{macd_fast_period}_{macd_slow_period}_{macd_signal_period}'],
                        name='MACD Histogram',
                        marker=dict(color='gray'))
    
    macds_trace = go.Scatter(x=ohlcv.time,
                            y=macd[f'MACDs_{macd_fast_period}_{macd_slow_period}_{macd_signal_period}'],
                            name='Signal',
                            line=dict(color='red', width=1.5))
    
    add_trace([macd_trace, macdh_trace, macds_trace], "MACD")

if is_rsi:
    rsi = ta.rsi(ohlcv[rsi_series_type], rsi_period)

    rsi_trace = go.Scatter(x=ohlcv.time,
                        y=rsi,
                        mode='lines',
                        name='RSI',
                        line=dict(color='black',
                                    width=1.5))
    rsi_30_trace = go.Scatter(mode='lines',
                            x=[min(ohlcv.time), max(ohlcv.time)],
                            y=[30, 30],
                            name='Oversold < 30%',
                            line=dict(color='red',
                                    width=0.5,
                                    dash='dot'))
    
    rsi_50_trace = go.Scatter(mode='lines',
                            x=[min(ohlcv.time), max(ohlcv.time)],
                            y=[50, 50],
                            line=dict(color='gray',
                                    width=0.5,
                                    dash='dashdot'),
                            name='50%')
    
    rsi_70_trace = go.Scatter(mode='lines',
                            x=[min(ohlcv.time), max(ohlcv.time)],
                            y=[70, 70],
                            name='Overbought > 70%',
                            line=dict(color='green',
                                    width=0.5,
                                    dash='dot'))
    
    add_trace([rsi_trace, rsi_30_trace, rsi_50_trace, rsi_70_trace], "RSI")

if is_show:
    custom_indicator_calculate(chosen_indicator, ticker)
    data = pd.read_csv(os.path.join(directory, f"tmp/{chosen_indicator}.csv"))
    data = data[data.columns[0]]
    custom_indicator_trace = go.Scatter(x=ohlcv.time,
                            y=data,
                            line=dict(color=color),
                            name=chosen_indicator)
    add_trace(custom_indicator_trace, chosen_indicator)

components.html(fig.to_html(include_plotlyjs="cdn", config = config), height=fig.layout.height)





