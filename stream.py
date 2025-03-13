import streamlit as st
import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import asyncio
import httpx
import time
from concurrent.futures import ProcessPoolExecutor

def load_data(file):
    df = pd.read_csv(file, parse_dates=['timestamp'])
    return df

def calculate_moving_average(df, window=30):
    df['moving_avg'] = df['temperature'].rolling(window=window, min_periods=1).mean()
    df['std_dev'] = df['temperature'].rolling(window=window, min_periods=1).std()
    return df

def detect_anomalies(df):
    df['is_anomaly'] = (df['temperature'] > (df['moving_avg'] + 2 * df['std_dev'])) | (df['temperature'] < (df['moving_avg'] - 2 * df['std_dev']))
    return df

def parallel_analysis(df):
    with ProcessPoolExecutor() as executor:
        df_split = np.array_split(df, 4)
        results = list(executor.map(calculate_moving_average, df_split))
        df = pd.concat(results)
        df = detect_anomalies(df)
    return df

def calculate_temperature_extremes(df):
    return df.groupby(['season']).agg(Max_Temperature=('temperature', 'max'), Min_Temperature=('temperature', 'min')).reset_index()

def calculate_summary_statistics(df):
    return df[['temperature']].describe().reset_index().rename(columns={'index': 'Метрика'})

def plot_seasonal_profiles(df):
    return px.box(df, x='season', y='temperature', title='Сезонные профили температуры')

async def fetch_temperature_async(city, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()

def fetch_temperature_sync(city, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    return requests.get(url).json()

def check_temperature_anomaly(current_temp, city, season, df):
    seasonal_data = df[(df['city'] == city) & (df['season'] == season)]
    mean_temp = seasonal_data['temperature'].mean()
    std_temp = seasonal_data['temperature'].std()
    lower_bound = mean_temp - 2 * std_temp
    upper_bound = mean_temp + 2 * std_temp
    return "Температура в пределах нормы" if lower_bound <= current_temp <= upper_bound else "Аномальная температура!"

st.title("📊 Анализ температурных данных и мониторинг текущей температуры")

uploaded_file = st.file_uploader("📂 Загрузите CSV-файл с историческими данными", type=["csv"])
api_key = st.text_input("🔑 Введите API-ключ OpenWeatherMap", type="password")
sync_async = st.radio("🌐 Выберите метод запроса к API", ["Синхронный", "Асинхронный"])

if uploaded_file:
    df = load_data(uploaded_file)
    cities = df['city'].unique()
    city = st.selectbox("🏙 Выберите город", cities)
    
    start_time = time.time()
    df_city = parallel_analysis(df[df['city'] == city])
    temp_extremes = calculate_temperature_extremes(df_city)
    summary_stats = calculate_summary_statistics(df_city)
    seasonal_profile_fig = plot_seasonal_profiles(df_city)
    elapsed_time = time.time() - start_time
    
    st.write(f"⏳ Время выполнения анализа (с параллелизацией): **{elapsed_time:.2f} сек**")
    
    st.write("### 📊 Описательная статистика по городу")
    st.table(summary_stats)
    
    fig = px.line(df_city, x='timestamp', y='temperature', title=f'🌡 Температурный анализ - {city}')
    fig.add_trace(go.Scatter(x=df_city[df_city['is_anomaly']]['timestamp'], y=df_city[df_city['is_anomaly']]['temperature'], mode='markers', marker=dict(color='red', size=8), name='Аномалии'))
    st.plotly_chart(fig)
    
    st.write("### 🔥❄ Экстремальные температуры по сезонам")
    st.table(temp_extremes)
    
    st.plotly_chart(seasonal_profile_fig)
    
    if api_key:
        current_temp = asyncio.run(fetch_temperature_async(city, api_key)) if sync_async == "Асинхронный" else fetch_temperature_sync(city, api_key)
        
        if 'cod' in current_temp and current_temp['cod'] == 401:
            st.error("❌ Ошибка API: Неверный API-ключ. Проверьте ключ и попробуйте снова.")
        elif 'main' in current_temp:
            current_temp_value = current_temp['main']['temp']
            season = df_city.iloc[-1]['season']
            result = check_temperature_anomaly(current_temp_value, city, season, df)
            st.write(f'🌡 Текущая температура в **{city}**: **{current_temp_value}°C**')
            st.write(f'🔍 Анализ аномальности: **{result}**')
        else:
            st.error("⚠ Ошибка API: Не удалось получить данные. Попробуйте позже.")
