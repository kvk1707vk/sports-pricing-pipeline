# Sports Pricing & Anomaly Detection Pipeline

## Project Overview
This project is an end-to-end data pipeline designed to ingest sports betting odds, normalize the data structure, and detect pricing anomalies (significant market drifts) in near real-time.

It mimics the architecture required for high-frequency betting environments, utilizing **Pandas** for transformation and simulating an **AWS S3 / Parquet** storage layer for downstream analytics.

## Key Features
* **ETL Architecture:** Extracts nested JSON data (simulating API response), Transforms it into a tabular structure, and Loads it into optimized storage.
* **Window Functions:** Utilizes shifting/lagging logic to calculate price deltas over time.
* **Storage Optimization:** Converts data to **Parquet** format to minimize storage footprint and maximize query speed in Athena.

## Tech Stack
* Python 3.9+
* Pandas / NumPy
* AWS SDK (Boto3) - *Architecture ready*
