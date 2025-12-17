import pandas as pd
import numpy as np
import datetime
import logging
import io

# Setup logging (Engineers love logs)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OddsMonitorPipeline:
    def __init__(self, bucket_name="angstrom-odds-monitor-dev"):
        self.bucket_name = bucket_name
        # self.s3_client = boto3.client('s3') # Uncomment when AWS credentials are set

    def fetch_market_data(self):
        """
        Simulates fetching live odds data from an API.
        In a production env, this would hit a requests.get() endpoint.
        """
        logging.info("Fetching live market data...")
        
        # Simulated raw data (Nested JSON structure similar to real betting APIs)
        raw_data = [
            {"match_id": "LIV_ARS", "timestamp": "2025-12-18 14:00:00", "market": "1x2", "outcomes": [{"selection": "Home", "price": 2.10}, {"selection": "Draw", "price": 3.50}, {"selection": "Away", "price": 3.20}]},
            {"match_id": "LIV_ARS", "timestamp": "2025-12-18 14:05:00", "market": "1x2", "outcomes": [{"selection": "Home", "price": 2.15}, {"selection": "Draw", "price": 3.50}, {"selection": "Away", "price": 3.10}]},
            {"match_id": "CHE_MUN", "timestamp": "2025-12-18 14:00:00", "market": "1x2", "outcomes": [{"selection": "Home", "price": 1.95}, {"selection": "Draw", "price": 3.60}, {"selection": "Away", "price": 4.00}]}
        ]
        return raw_data

    def transform_data(self, raw_data):
        """
        Flattens JSON and calculates implied probability.
        """
        logging.info("Transforming and flattening data...")
        
        # 1. Flatten the nested JSON (The 'Analytics Engineer' skill)
        rows = []
        for entry in raw_data:
            for outcome in entry['outcomes']:
                rows.append({
                    'match_id': entry['match_id'],
                    'timestamp': entry['timestamp'],
                    'market': entry['market'],
                    'selection': outcome['selection'],
                    'price': outcome['price']
                })
        
        df = pd.DataFrame(rows)
        
        # 2. Feature Engineering: Calculate Implied Probability (1/Odds)
        df['implied_prob'] = round(1 / df['price'], 4)
        
        return df

    def detect_anomalies(self, df, threshold=0.05):
        """
        Detects significant price movements (Logic similar to Risk Monitoring).
        """
        logging.info("Running anomaly detection...")
        
        # Sort by match and time to use Window Functions
        df = df.sort_values(by=['match_id', 'selection', 'timestamp'])
        
        # Calculate price shift from previous timestamp (Using Shift/Lag logic)
        df['prev_price'] = df.groupby(['match_id', 'selection'])['price'].shift(1)
        df['price_delta'] = df['price'] - df['prev_price']
        
        # Filter for significant movements
        anomalies = df[abs(df['price_delta']) >= threshold]
        
        if not anomalies.empty:
            logging.warning(f"Found {len(anomalies)} pricing anomalies!")
            # In production, this would trigger an SNS alert or Slack webhook
        
        return df

    def load_to_s3(self, df, filename):
        """
        Simulates uploading the clean Parquet file to S3.
        """
        logging.info(f"Converting to Parquet and uploading to s3://{self.bucket_name}/{filename}...")
        
        # Convert to Parquet buffer (Efficient storage)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        
        # Mock Upload
        # self.s3_client.put_object(Bucket=self.bucket_name, Key=filename, Body=parquet_buffer.getvalue())
        logging.info("Upload successful (Simulated).")

if __name__ == "__main__":
    pipeline = OddsMonitorPipeline()
    
    # 1. Extract
    data = pipeline.fetch_market_data()
    
    # 2. Transform
    clean_df = pipeline.transform_data(data)
    
    # 3. Analyze
    final_df = pipeline.detect_anomalies(clean_df)
    
    # 4. Load
    pipeline.load_to_s3(final_df, f"odds_data_{datetime.date.today()}.parquet")
    
    print("\n--- SAMPLE OUTPUT DATA ---")
    print(final_df.head())
