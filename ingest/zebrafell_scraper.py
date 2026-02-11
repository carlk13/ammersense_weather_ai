import requests
import pandas as pd
import time
import datetime
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

# --- Configuration ---
BASE_URL = "https://zebrafell-server.onrender.com/api/charts"
OUTPUT_DIR = "data_lake"
# Metric mapping based on report analysis
METRICS = {
    "temp": "temp",  # Temperature
    "wind": "wind",  # Wind speed/direction
    "rain": "rain",  # Precipitation (inferred)
    "press": "press",  # Pressure (inferred)
    "sun": "sun",  # Sunshine duration (inferred)
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("zebrafell_scraper.log"), logging.StreamHandler()],
)


class ZebrafellScraper:
    def __init__(self):
        self.session = self._create_session()
        self.ensure_directories()

    def _create_session(self):
        """
        Creates a requests session with exponential backoff.
        Essential for Render-hosted apps that may have cold starts or 5xx errors.
        """
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,  # Wait 1s, 2s, 4s...
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": "ZebrafellResearchBot/1.0 (Scientific Data Extraction)",
                "Referer": "https://zebrafell.onrender.com/",
            }
        )
        return session

    def ensure_directories(self):
        """Creates storage directory for Parquet/CSV files."""
        Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    def get_timestamp_ms(self, dt_object):
        """Converts datetime to Unix timestamp in milliseconds (required by API)."""
        return int(dt_object.timestamp() * 1000)

    def calculate_windchill(self, row):
        """
        Applies the Klima-Michel-Modell as described in the report.
        Only applies if Temp < 10C and Wind > 4.8 km/h (approx 1.3 m/s).
        """
        ta = row.get("v", 0)  # Ambient Temp
        v = row.get("wind_speed", 0)  # Wind Speed

        # Check conditions (simplified for demonstration)
        if ta < 10 and v > 4.8:
            return 13.12 + 0.6215 * ta - 11.37 * (v**0.16) + 0.3965 * ta * (v**0.16)
        return ta  # Return ambient if chill not applicable

    def fetch_chunk(self, metric, bis_ts, days=7):
        """
        Fetches a specific chunk of data from the API.
        URL pattern: /api/charts/{metric}?bis={timestamp}&tage={days}
        """
        url = f"{BASE_URL}/{metric}"
        params = {"bis": bis_ts, "tage": days, "dark": 0}

        try:
            logging.debug(f"Requesting {metric} ending at {bis_ts}...")
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch {metric} at {bis_ts}: {e}")
            return None

    def process_data(self, raw_data, metric):
        """
        Normalizes raw JSON into a Pandas DataFrame.
        Handles the specific dictionary structure: {t: timestamp, v: value, ...}
        """
        if not raw_data:
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)

        # Convert ms timestamp to datetime
        if "t" in df.columns:
            df["datetime_utc"] = pd.to_datetime(df["t"], unit="ms", origin="unix")
            df.drop(columns=["t"], inplace=True)
            df.set_index("datetime_utc", inplace=True)

        # Rename 'v' column to metric name for clarity
        if "v" in df.columns:
            df.rename(columns={"v": metric}, inplace=True)

        return df

    def backfill_history(self, start_year=2007):
        """
        Phase 2: Historical Backfill Algorithm (The 'Time-Seeker').
        Iterates backwards from NOW to start_year in 7-day chunks.
        """
        current_date = datetime.datetime.utcnow()
        end_date = datetime.datetime(start_year, 1, 1)

        logging.info(f"Starting historical backfill from {current_date} to {end_date}")

        # We will store data in monthly buffers to save to disk periodically
        monthly_buffer = {m: [] for m in METRICS}

        while current_date > end_date:
            bis_ts = self.get_timestamp_ms(current_date)

            for metric_key, metric_endpoint in METRICS.items():
                raw_data = self.fetch_chunk(metric_endpoint, bis_ts, days=7)

                if raw_data:
                    df = self.process_data(raw_data, metric_key)
                    if not df.empty:
                        monthly_buffer[metric_key].append(df)

            # Move pointer back 7 days
            current_date -= datetime.timedelta(days=7)

            # Save and clear buffer if we crossed a month boundary (optimization)
            # (Logic simplified here: saving every 10 iterations or check date change)
            if len(monthly_buffer["temp"]) >= 4:
                self.flush_buffer(monthly_buffer)
                monthly_buffer = {m: [] for m in METRICS}  # Reset

            # Politeness sleep to avoid DoS/Rate Limiting
            time.sleep(1.5)

        # Final flush
        self.flush_buffer(monthly_buffer)
        logging.info("Historical backfill complete.")

    def flush_buffer(self, buffer):
        """Saves buffered DataFrames to CSV/Parquet."""
        for metric, data_list in buffer.items():
            if not data_list:
                continue

            combined_df = pd.concat(data_list)
            # Remove duplicates caused by window overlaps
            combined_df = combined_df[~combined_df.index.duplicated(keep="first")]

            # Sort chronologically
            combined_df.sort_index(inplace=True)

            # Save to disk (Append mode logic would be needed for production)
            # For this script, we save distinct chunks based on the first timestamp
            timestamp_str = combined_df.index[-1].strftime("%Y-%m-%d")
            filename = f"{OUTPUT_DIR}/{metric}_{timestamp_str}.csv"

            combined_df.to_csv(filename)
            logging.info(f"Saved {len(combined_df)} rows to {filename}")

    def live_ingest(self):
        """
        Phase 3: Live Ingestion.
        Polls the API every 10 minutes for the latest data (tage=1).
        """
        logging.info("Switching to Live Ingestion Mode...")
        while True:
            try:
                now = datetime.datetime.utcnow()
                bis_ts = self.get_timestamp_ms(now)

                for metric_key, metric_endpoint in METRICS.items():
                    # Request only the last 1 day to be safe, logic extracts newest
                    raw_data = self.fetch_chunk(metric_endpoint, bis_ts, days=1)
                    if raw_data:
                        df = self.process_data(raw_data, metric_key)
                        # Here you would typically Upsert to a database (InfluxDB/SQL)
                        # For demo, we just log the latest value
                        latest = df.iloc[-1]
                        logging.info(
                            f"LIVE [{metric_key}]: {latest[metric_key]} at {df.index[-1]}"
                        )

                # Sleep for 10 minutes (600 seconds)
                logging.info("Sleeping for 10 minutes...")
                time.sleep(600)

            except KeyboardInterrupt:
                logging.info("Stopping live ingestion.")
                break
            except Exception as e:
                logging.error(f"Live ingestion error: {e}")
                time.sleep(60)  # Short sleep on error


# --- Execution Entry Point ---
if __name__ == "__main__":
    scraper = ZebrafellScraper()

    # 1. Run Historical Backfill (Comment out if only running live)
    # Note: Running full history from 2007 will take time.
    # Adjust start_year to 2025 for a quick test.
    scraper.backfill_history(start_year=2025)

    # 2. Switch to Live Mode
    # scraper.live_ingest()
