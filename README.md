# Stock Market Data Pipeline

## Overview
This project implements an automated data pipeline that collects historical stock price data for multiple companies, processes it, and loads it into a Snowflake data warehouse. The pipeline is orchestrated using Apache Airflow and includes a Power BI dashboard for data visualization.

## Architecture
- **Data Collection**: Uses `yfinance` to fetch historical stock data
- **Data Processing**: Python/Pandas for data transformation
- **Data Storage**: Snowflake Data Warehouse
- **Orchestration**: Apache Airflow
- **Visualization**: Power BI Dashboard

## Data Coverage
- **Time Range**: 2000-01-01 to 2025-01-30
- **Companies**: 96 major publicly traded companies
- **Records**: 650,000 rows of historical stock data
- **Sectors**: 
  - Information Technology
  - Consumer Discretionary
  - Communication Services
  - Financials
  - Health Care
  - Consumer Staples
  - Industrials
  - Energy
  - Materials

## Technical Components

### 1. Airflow DAG
- Schedule: Daily at 12 AM UTC
- Tasks:
  - `get_stock_data_task`: Fetches and processes stock data
  - `load_to_snowflake_task`: Loads processed data into Snowflake

### 2. Data Model
The pipeline collects and stores the following data points:
- Trading Date
- Opening Price
- High Price
- Low Price
- Closing Price
- Trading Volume
- Company Name
- Stock Ticker
- Sector

## Setup Instructions

### Prerequisites
```bash
pip install apache-airflow
pip install yfinance
pip install snowflake-snowpark-python
pip install pandas
```

## Power BI Dashboard
The interactive dashboard provides:
- Stock price trends analysis
- Sector-wise performance comparison
- Volume analysis
- Company-specific detailed views
- Historical price movement patterns

## Future Enhancements
1. Add real-time data streaming capabilities
2. Implement data quality checks
3. Add more technical indicators
4. Expand company coverage
5. Implement automated alerting system
