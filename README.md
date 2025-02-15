# Stock Market Data Pipeline

## Overview
This project implements an automated data pipeline that collects historical stock price data for multiple companies, processes it, and loads it into a Snowflake database. The data is then modeled in a star schema as a data warehouse. The pipeline is orchestrated using Apache Airflow, and a Power BI dashboard is used for data visualization.

## Architecture
- **Data Collection**: Uses `yfinance` to fetch historical stock data
- **Data Processing**: Python/Pandas for data transformation
- **Data Storage**: Snowflake Data Warehouse
- **Orchestration**: Apache Airflow
- **Visualization**: Power BI Dashboard
## Workflow Diagram
Data Collection → Data Processing → Snowflake Loading → Star Schema Modeling → Power BI Dashboard.

## Data Coverage
- **Time Range**: 2000-01-03 to Now
- **Companies**: 100 major publicly traded companies
- **Records**: 631K rows of historical stock data
- **Sectors**: 
  - Technology
  - Consumer Discretionary
  - Communication Services
  - Financials
  - Health Care
  - Consumer Staples
  - Industrials
  - Energy
  - Materials
  - Utilities

## Technical Components

### 1. Airflow DAGS
- Schedule: Daily at 10 PM UTC For Dag1 for get data and transformed then loaded in snowflake database
- Schedule: Daily at 10.30 PM UTC For Dag2 for load new daily data in fact and dimension tables
  
## Data Loading
- **Target**: Snowflake database.
- **Tables**:
    -  Raw Data Table: Stores raw data collected from the API.
    -  Processed Data Tables: Stores cleaned and transformed data.

- **Star Schema Tables**:
    -  **Fact Table: FACT_STOCK_PRICES**:
         - Contains metrics like open, high, low, close, adj_close prices, and volume.
         - Linked to dimension tables via foreign keys.

    -  **Dimension Tables**:
         - DIM_DATE: Date-related attributes (e.g., date_sk , trade_data , trade_day, trade_month, trade_year , trade_Quarter).
         - DIM_COMPANY: Company-related attributes (e.g., company_sk, sector_sk , company_name).
         - DIM_SECTOR : Sector-related attributes (e.g., sector_sk, sector_name).

## Setup Instructions

### Prerequisites
```bash
pip install apache-airflow
pip install yfinance==0.2.31
pip install pandas==2.1.4
pip install apache-airflow-providers-snowflake
```

## Power BI Dashboard
The interactive dashboard provides:
- Market Overview
- Stock Details
- Volume analysis
- Company-specific detailed views
- Historical price movement patterns

## Future Enhancements
1. Add real-time data streaming capabilities
2. Implement data quality checks
3. Add more technical indicators
4. Expand company coverage
5. Implement automated alerting system
