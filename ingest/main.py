import time
import schedule
import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- Configuration ---
LAT = float(os.getenv("WEATHER_LAT", 48.1351)) 
LON = float(os.getenv("WEATHER_LON", 11.5820))

# InfluxDB Settings (Loaded from .env by Docker)
URL = os.getenv("INFLUX_URL", "http://influxdb:8086")
TOKEN = os.getenv("INFLUX_TOKEN")
ORG = os.getenv("INFLUX_ORG")
BUCKET = os.getenv("INFLUX_BUCKET")


def get_influx_client():
    return InfluxDBClient(url=URL, token=TOKEN, org=ORG)

def fetch_open_meteo(days_past=0, days_future=1):
    """
    Fetches hourly temperature, pressure, and wind speed.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": ["temperature_2m", "pressure_msl", "wind_speed_10m"],
        "timezone": "auto",
        "past_days": days_past,
        "forecast_days": days_future
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Open-Meteo returns 'SoA' (Struct of Arrays), we want a DataFrame
        hourly = data['hourly']
        df = pd.DataFrame({
            'time': pd.to_datetime(hourly['time']),
            'temperature': hourly['temperature_2m'],
            'pressure': hourly['pressure_msl'],
            'wind_speed': hourly['wind_speed_10m']
        })
        return df
    except Exception as e:
        print(f"Error fetching Open-Meteo: {e}")
        return None

def write_to_influx(df):
    """
    Writes a Pandas DataFrame to InfluxDB
    """
    if df is None or df.empty:
        return

    client = get_influx_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    points = []
    for _, row in df.iterrows():
        # Create a "Point" for InfluxDB
        # Measurement: "weather"
        # Tags: "source=open-meteo" (Useful if you add your own sensors later)
        # Fields: temp, press, wind
        p = Point("weather") \
            .tag("source", "open_meteo") \
            .field("temperature", float(row['temperature'])) \
            .field("pressure", float(row['pressure'])) \
            .field("wind_speed", float(row['wind_speed'])) \
            .time(row['time'])
        points.append(p)
    
    try:
        write_api.write(bucket=BUCKET, org=ORG, record=points)
        print(f"Written {len(points)} data points to InfluxDB.")
    except Exception as e:
        print(f"InfluxDB Write Error: {e}")
    finally:
        client.close()


def initial_backfill():
    """Run once on container start to populate history."""
    client = get_influx_client()
    query_api = client.query_api()
    
    query = f'from(bucket: "{BUCKET}") |> range(start: -1y) |> count()'
    result = query_api.query(query)
    
    if len(result) == 0:
        print("Database empty! Starting initial backfill (Last 90 days)...")
        # Fetch 720 days of history
        df = fetch_open_meteo(days_past=730, days_future=1)
        write_to_influx(df)
        print("Backfill complete.")
    else:
        print("Database already contains data. Skipping backfill.")
    client.close()

def hourly_job():
    print("Running hourly update...")
    df = fetch_open_meteo(days_past=1, days_future=3)
    write_to_influx(df)


if __name__ == "__main__":
    print("Weather Ingest Service Started...")
    
    # Wait for InfluxDB to wake up (Retry logic)
    for _ in range(10):
        try:
            get_influx_client().health()
            break
        except:
            print("Waiting for InfluxDB...")
            time.sleep(5)
            
    initial_backfill()
    
    schedule.every().hour.do(hourly_job)
    
    while True:
        schedule.run_pending()
        time.sleep(1)