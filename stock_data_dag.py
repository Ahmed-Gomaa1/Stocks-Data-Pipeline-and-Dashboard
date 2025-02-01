from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import
import yfinance as yf
import pandas as pd
from snowflake.snowpark import Session

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Move configurations to separate functions for better organization
def get_snowflake_connection():
    return {
        "account": "",
        "user": "",
        "password": "",
        "role": "",       
        "warehouse": "",  
        "database": "STOCKDATA",  
        "schema": "STOCKSTAGING"      
    }

def get_stock_data():
    tickers = [
        "AAPL", "MSFT", "AMZN", "GOOGL", "TSLA", "META", "NVDA", "BRK-B", "V", "JNJ",
        "WMT", "PG", "MA", "UNH", "HD", "DIS", "BAC", "PYPL", "CMCSA", "ADBE",
        "PFE", "CSCO", "PEP", "ABT", "TMO", "NFLX", "INTC", "ABBV", "COST", "AVGO",
        "QCOM", "TXN", "CRM", "AMD", "MDT", "NKE", "ORCL", "HON", "LIN", "SBUX",
        "INTU", "AMGN", "LOW", "BKNG", "CHTR", "GS", "UNP", "CAT", "UPS", "RTX",
        "BA", "DE", "MMM", "IBM", "GE", "F", "GM", "T", "VZ", "CVX", "XOM",
        "COP", "SLB", "EOG", "PSX", "MPC", "DVN", "HAL", "KMI", "OXY",
        "CL", "PG", "KO", "PEP", "PM", "MO", "MDLZ", "STZ", "KHC", "GIS",
        "SYY", "CAG", "HSY", "CPB", "K", "MCD", "YUM", "SBUX", "DPZ", "CMG",
        "DRI", "BLMN", "EAT", "DENN", "JACK", "NDLS", "PZZA", "RRGB", "THI"
    ]

    ticker_to_name = {
        "AAPL": "Apple Inc.",
        "MSFT": "Microsoft Corporation",
        "AMZN": "Amazon.com Inc.",
        "GOOGL": "Alphabet Inc. (Class A)",
        "TSLA": "Tesla Inc.",
        "META": "Meta Platforms Inc.",
        "NVDA": "NVIDIA Corporation",
        "BRK-B": "Berkshire Hathaway Inc. (Class B)",
        "V": "Visa Inc.",
        "JNJ": "Johnson & Johnson",
        "WMT": "Walmart Inc.",
        "PG": "Procter & Gamble Co.",
        "MA": "Mastercard Inc.",
        "UNH": "UnitedHealth Group Inc.",
        "HD": "The Home Depot Inc.",
        "DIS": "The Walt Disney Company",
        "BAC": "Bank of America Corporation",
        "PYPL": "PayPal Holdings Inc.",
        "CMCSA": "Comcast Corporation",
        "ADBE": "Adobe Inc.",
        "PFE": "Pfizer Inc.",
        "CSCO": "Cisco Systems Inc.",
        "PEP": "PepsiCo Inc.",
        "ABT": "Abbott Laboratories",
        "TMO": "Thermo Fisher Scientific Inc.",
        "NFLX": "Netflix Inc.",
        "INTC": "Intel Corporation",
        "ABBV": "AbbVie Inc.",
        "COST": "Costco Wholesale Corporation",
        "AVGO": "Broadcom Inc.",
        "QCOM": "Qualcomm Inc.",
        "TXN": "Texas Instruments Inc.",
        "CRM": "Salesforce Inc.",
        "AMD": "Advanced Micro Devices Inc.",
        "MDT": "Medtronic plc",
        "NKE": "Nike Inc.",
        "ORCL": "Oracle Corporation",
        "HON": "Honeywell International Inc.",
        "LIN": "Linde plc",
        "SBUX": "Starbucks Corporation",
        "INTU": "Intuit Inc.",
        "AMGN": "Amgen Inc.",
        "LOW": "Lowe's Companies Inc.",
        "BKNG": "Booking Holdings Inc.",
        "CHTR": "Charter Communications Inc.",
        "GS": "Goldman Sachs Group Inc.",
        "UNP": "Union Pacific Corporation",
        "CAT": "Caterpillar Inc.",
        "UPS": "United Parcel Service Inc.",
        "RTX": "Raytheon Technologies Corporation",
        "BA": "Boeing Company",
        "DE": "Deere & Company",
        "MMM": "3M Company",
        "IBM": "International Business Machines Corporation",
        "GE": "General Electric Company",
        "F": "Ford Motor Company",
        "GM": "General Motors Company",
        "T": "AT&T Inc.",
        "VZ": "Verizon Communications Inc.",
        "CVX": "Chevron Corporation",
        "XOM": "Exxon Mobil Corporation",
        "COP": "ConocoPhillips",
        "SLB": "Schlumberger Limited",
        "EOG": "EOG Resources Inc.",
        "PSX": "Phillips 66",
        "MPC": "Marathon Petroleum Corporation",
        "DVN": "Devon Energy Corporation",
        "HAL": "Halliburton Company",
        "KMI": "Kinder Morgan Inc.",
        "OXY": "Occidental Petroleum Corporation",
        "CL": "Colgate-Palmolive Company",
        "KO": "Coca-Cola Company",
        "PM": "Philip Morris International Inc.",
        "MO": "Altria Group Inc.",
        "MDLZ": "Mondelez International Inc.",
        "STZ": "Constellation Brands Inc.",
        "KHC": "Kraft Heinz Company",
        "GIS": "General Mills Inc.",
        "SYY": "Sysco Corporation",
        "CAG": "Conagra Brands Inc.",
        "HSY": "Hershey Company",
        "CPB": "Campbell Soup Company",
        "K": "Kellogg Company",
        "MCD": "McDonald's Corporation",
        "YUM": "Yum! Brands Inc.",
        "DPZ": "Domino's Pizza Inc.",
        "CMG": "Chipotle Mexican Grill Inc.",
        "DRI": "Darden Restaurants Inc.",
        "BLMN": "Bloomin' Brands Inc.",
        "EAT": "Brinker International Inc.",
        "DENN": "Denny's Corporation",
        "JACK": "Jack in the Box Inc.",
        "NDLS": "Noodles & Company",
        "PZZA": "Papa John's International Inc.",
        "RRGB": "Red Robin Gourmet Burgers Inc.",
        "THI": "Tim Hortons Inc."
    }

    sector_mapping = {
        "AAPL": "Information Technology",
        "MSFT": "Information Technology",
        "AMZN": "Consumer Discretionary",
        "GOOGL": "Communication Services",
        "TSLA": "Consumer Discretionary",
        "META": "Communication Services",
        "NVDA": "Information Technology",
        "BRK-B": "Financials",
        "V": "Financials",
        "JNJ": "Health Care",
        "WMT": "Consumer Staples",
        "PG": "Consumer Staples",
        "MA": "Financials",
        "UNH": "Health Care",
        "HD": "Consumer Discretionary",
        "DIS": "Communication Services",
        "BAC": "Financials",
        "PYPL": "Financials",
        "CMCSA": "Communication Services",
        "ADBE": "Information Technology",
        "PFE": "Health Care",
        "CSCO": "Information Technology",
        "PEP": "Consumer Staples",
        "ABT": "Health Care",
        "TMO": "Health Care",
        "NFLX": "Communication Services",
        "INTC": "Information Technology",
        "ABBV": "Health Care",
        "COST": "Consumer Staples",
        "AVGO": "Information Technology",
        "QCOM": "Information Technology",
        "TXN": "Information Technology",
        "CRM": "Information Technology",
        "AMD": "Information Technology",
        "MDT": "Health Care",
        "NKE": "Consumer Discretionary",
        "ORCL": "Information Technology",
        "HON": "Industrials",
        "LIN": "Materials",
        "SBUX": "Consumer Discretionary",
        "INTU": "Information Technology",
        "AMGN": "Health Care",
        "LOW": "Consumer Discretionary",
        "BKNG": "Consumer Discretionary",
        "CHTR": "Communication Services",
        "GS": "Financials",
        "UNP": "Industrials",
        "CAT": "Industrials",
        "UPS": "Industrials",
        "RTX": "Industrials",
        "BA": "Industrials",
        "DE": "Industrials",
        "MMM": "Industrials",
        "IBM": "Information Technology",
        "GE": "Industrials",
        "F": "Consumer Discretionary",
        "GM": "Consumer Discretionary",
        "T": "Communication Services",
        "VZ": "Communication Services",
        "CVX": "Energy",
        "XOM": "Energy",
        "COP": "Energy",
        "SLB": "Energy",
        "EOG": "Energy",
        "PSX": "Energy",
        "MPC": "Energy",
        "DVN": "Energy",
        "HAL": "Energy",
        "KMI": "Energy",
        "OXY": "Energy",
        "CL": "Consumer Staples",
        "KO": "Consumer Staples",
        "PM": "Consumer Staples",
        "MO": "Consumer Staples",
        "MDLZ": "Consumer Staples",
        "STZ": "Consumer Staples",
        "KHC": "Consumer Staples",
        "GIS": "Consumer Staples",
        "SYY": "Consumer Staples",
        "CAG": "Consumer Staples",
        "HSY": "Consumer Staples",
        "CPB": "Consumer Staples",
        "K": "Consumer Staples",
        "MCD": "Consumer Discretionary",
        "YUM": "Consumer Discretionary",
        "DPZ": "Consumer Discretionary",
        "CMG": "Consumer Discretionary",
        "DRI": "Consumer Discretionary",
        "BLMN": "Consumer Discretionary",
        "EAT": "Consumer Discretionary",
        "DENN": "Consumer Discretionary",
        "JACK": "Consumer Discretionary",
        "NDLS": "Consumer Discretionary",
        "PZZA": "Consumer Discretionary",
        "RRGB": "Consumer Discretionary",
        "THI": "Consumer Discretionary"
    }

    # Get yesterday's date
    end_date = datetime.today()
    start_date = end_date - timedelta(days=1)
    
    # Download data
    data = yf.download(tickers, 
                      start=start_date.strftime('2000-01-01'),
                      end=end_date.strftime('2025-01-30'), 
                      group_by="ticker")
    
    # Process data
    flattened_data = pd.DataFrame()
    for ticker in tickers:
        if ticker in data:
            ticker_data = data[ticker].reset_index()
            ticker_data['Ticker'] = ticker 
            flattened_data = pd.concat([flattened_data, ticker_data])
    
    # Rename columns
    flattened_data.rename(columns={
        'Date': 'tdate',
        'Open': 'open',
        'High': 'high',
        'Low': 'low',
        'Close': 'close',
        'Volume': 'volume',
    }, inplace=True)
    
    # Add company name and sector
    flattened_data['company_name'] = flattened_data['Ticker'].map(ticker_to_name)
    flattened_data['sector'] = flattened_data['Ticker'].map(sector_mapping)
    flattened_data = flattened_data.reset_index(drop=True)
    
    return flattened_data

def load_to_snowflake(**context):
    # Get the data from the previous task
    flattened_data = context['task_instance'].xcom_pull(task_ids='get_stock_data_task')
    
    # Create Snowflake session
    session = Session.builder.configs(get_snowflake_connection()).create()
    
    try:
        snowpark_df = session.create_dataframe(flattened_data)
        snowpark_df.write.mode("overwrite").save_as_table("HISTORICAL_STOCKS_PRICES")
        print("Data successfully saved to Snowflake table HISTORICAL_STOCKS_PRICES.")
    except Exception as e:
        print(f"Error saving data to Snowflake: {e}")
        raise
    finally:
        session.close()

# Create the DAG
with DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Daily stock data pipeline',
    schedule='0 0 * * *',  # Use `schedule` instead of `schedule_interval`
    start_date=datetime(2025, 1, 31),
    catchup=False,
    tags=['stocks', 'finance'],
) as dag:
    
    # Task 1: Get stock data
    get_data = PythonOperator(
        task_id='get_stock_data_task',
        python_callable=get_stock_data,  # Remove `provide_context=True`
    )
    
    # Task 2: Load data to Snowflake
    load_data = PythonOperator(
        task_id='load_to_snowflake_task',
        python_callable=load_to_snowflake,  # Remove `provide_context=True`
    )
    
    # Set task dependencies
    get_data >> load_data
