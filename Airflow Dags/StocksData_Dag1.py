from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import yfinance as yf
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('stock_pipeline')

def get_stock_data(**context) -> None:
    """Extract stock data from Yahoo Finance"""
    try:
        logger.info("Starting stock data extraction")
        
        snowflake_hook = SnowflakeHook(snowflake_conn_id="Snowflake_conn")
        
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT MAX("STOCK_DATA"."Tdate") FROM stockdata.stockstaging."STOCK_DATA"')
            last_date = cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error querying last date: {e}")
            last_date = None
        finally:
            cursor.close()
            conn.close()
            
            
        logger.info(f"MAX STOCKS_DATA {last_date}")
        
        if last_date:
            start_date = last_date + timedelta(days=1)
        else:
            start_date = datetime(2000, 1, 1)
        
        end_date = datetime.now()
        
        logger.info(f"Fetching data from {start_date} to {end_date}")
        
        tickers = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "TSLA", "META", "NVDA", "BRK-B", "V", "JNJ",
    "WMT", "MA", "UNH", "HD", "DIS", "BAC", "CMCSA", "ADBE",
    "PFE", "CSCO", "PEP", "ABT", "TMO", "NFLX", "INTC", "ABBV", "COST",
    "QCOM", "TXN", "CRM", "AMD", "NKE", "ORCL", "HON", "LIN", "SBUX",
    "INTU", "AMGN", "LOW", "BKNG", "GS", "UNP", "CAT", "UPS", "RTX",
    "BA", "DE", "MMM", "IBM", "GE", "F", "GM", "T", "VZ", "CVX", "XOM",
    "COP", "SLB", "EOG", "PSX", "MPC", "DVN", "HAL", "OXY",
    "CL", "KO", "PEP", "PM", "MO", "MDLZ", "STZ", "KHC", "GIS",
    "SYY", "CAG", "HSY", "CPB", "K", "MCD", "YUM", "SBUX", "DPZ",
    "DRI", "BLMN", "EAT", "DENN", "JACK", "RRGB", "BB", "MAN", "MBI",
    "OC", "PNC", "RCL", "RRC", "SCHW", "SIRI", "SJM", "SNA", "SO", "T", "M","NEE"]
        
        data = yf.download(tickers, 
                        start=start_date.strftime('%Y-%m-%d'),
                        end=end_date.strftime('%Y-%m-%d'), 
                        group_by="ticker")
        
        context['task_instance'].xcom_push(key='raw_data', value=data)
        context['task_instance'].xcom_push(key='tickers', value=tickers)
        
        logger.info("Successfully extracted stock data")
        
    except Exception as e:
        logger.error(f"Failed to extract stock data: {str(e)}", exc_info=True)
        raise

def transform_stock_data(**context) -> None:
    """Transform the raw stock data"""
    try:
        logger.info("Starting data transformation")
        
        raw_data = context['task_instance'].xcom_pull(task_ids='get_stock_data_task', key='raw_data')
        tickers = context['task_instance'].xcom_pull(task_ids='get_stock_data_task', key='tickers')
        
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
        "MA": "Mastercard Inc.",
        "UNH": "UnitedHealth Group Inc.",
        "HD": "The Home Depot Inc.",
        "DIS": "The Walt Disney Company",
        "BAC": "Bank of America Corporation",
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
        "QCOM": "Qualcomm Inc.",
        "TXN": "Texas Instruments Inc.",
        "CRM": "Salesforce Inc.",
        "AMD": "Advanced Micro Devices Inc.",
        "NKE": "Nike Inc.",
        "ORCL": "Oracle Corporation",
        "HON": "Honeywell International Inc.",
        "LIN": "Linde plc",
        "SBUX": "Starbucks Corporation",
        "INTU": "Intuit Inc.",
        "AMGN": "Amgen Inc.",
        "LOW": "Lowe's Companies Inc.",
        "BKNG": "Booking Holdings Inc.",
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
        "DRI": "Darden Restaurants Inc.",
        "BLMN": "Bloomin' Brands Inc.",
        "EAT": "Brinker International Inc.",
        "DENN": "Denny's Corporation",
        "JACK": "Jack in the Box Inc.",
        "RRGB": "Red Robin Gourmet Burgers Inc.",
        'BB': 'BlackBerry Limited',
        "MAN": "ManpowerGroup Inc.",
        "MBI": "MBIA Inc.",
        "OC": "Owens Corning",
        "PNC": "The PNC Financial Services Group, Inc.",
        "RCL": "Royal Caribbean Cruises Ltd.",
        "RRC": "Range Resources Corporation",
        "SCHW": "The Charles Schwab Corporation",
        "SIRI": "Sirius XM Holdings Inc.",
        "SJM": "The J.M. Smucker Company",
        "SNA": "Snap-on Incorporated",
        "SO": "Southern Company",
        "T": "AT&T Inc.",
        "M": "Macy's, Inc.",
        "NEE": "NextEra Energy Inc."
    }
        
        sector_mapping = {
    'AAPL': 'Technology',
    'MSFT': 'Technology',
    'AMZN': 'Consumer Discretionary',
    'GOOGL': 'Communication Services',
    'TSLA': 'Consumer Discretionary',
    'META': 'Communication Services',
    'NVDA': 'Technology',
    'BRK-B': 'Financials',
    'V': 'Financials',
    'JNJ': 'Healthcare',
    'WMT': 'Consumer Staples',
    'MA': 'Financials',
    'UNH': 'Healthcare',
    'HD': 'Consumer Discretionary',
    'DIS': 'Communication Services',
    'BAC': 'Financials',
    'CMCSA': 'Communication Services',
    'ADBE': 'Technology',
    'PFE': 'Healthcare',
    'CSCO': 'Technology',
    'PEP': 'Consumer Staples',
    'ABT': 'Healthcare',
    'TMO': 'Healthcare',
    'NFLX': 'Communication Services',
    'INTC': 'Technology',
    'ABBV': 'Healthcare',
    'COST': 'Consumer Staples',
    'QCOM': 'Technology',
    'TXN': 'Technology',
    'CRM': 'Technology',
    'AMD': 'Technology',
    'NKE': 'Consumer Discretionary',
    'ORCL': 'Technology',
    'HON': 'Industrials',
    'LIN': 'Materials',
    'SBUX': 'Consumer Discretionary',
    'INTU': 'Technology',
    'AMGN': 'Healthcare',
    'LOW': 'Consumer Discretionary',
    'BKNG': 'Consumer Discretionary',
    'GS': 'Financials',
    'UNP': 'Industrials',
    'CAT': 'Industrials',
    'UPS': 'Industrials',
    'RTX': 'Industrials',
    'BA': 'Industrials',
    'DE': 'Industrials',
    'MMM': 'Industrials',
    'IBM': 'Technology',
    'GE': 'Industrials',
    'F': 'Consumer Discretionary',
    'GM': 'Consumer Discretionary',
    'T': 'Communication Services',
    'VZ': 'Communication Services',
    'CVX': 'Energy',
    'XOM': 'Energy',
    'COP': 'Energy',
    'SLB': 'Energy',
    'EOG': 'Energy',
    'PSX': 'Energy',
    'MPC': 'Energy',
    'DVN': 'Energy',
    'HAL': 'Energy',
    'OXY': 'Energy',
    'CL': 'Consumer Staples',
    'KO': 'Consumer Staples',
    'PEP': 'Consumer Staples',
    'PM': 'Consumer Staples',
    'MO': 'Consumer Staples',
    'MDLZ': 'Consumer Staples',
    'STZ': 'Consumer Staples',
    'KHC': 'Consumer Staples',
    'GIS': 'Consumer Staples',
    'SYY': 'Consumer Staples',
    'CAG': 'Consumer Staples',
    'HSY': 'Consumer Staples',
    'CPB': 'Consumer Staples',
    'K': 'Consumer Staples',
    'MCD': 'Consumer Discretionary',
    'YUM': 'Consumer Discretionary',
    'SBUX': 'Consumer Discretionary',
    'DPZ': 'Consumer Discretionary',
    'DRI': 'Consumer Discretionary',
    'BLMN': 'Consumer Discretionary',
    'EAT': 'Consumer Discretionary',
    'DENN': 'Consumer Discretionary',
    'JACK': 'Consumer Discretionary',
    'RRGB': 'Consumer Discretionary',
    'BB': 'Technology',
    'MAN': 'Industrials',
    'MBI': 'Financials',
    'OC': 'Industrials',
    'PNC': 'Financials',
    'RCL': 'Consumer Discretionary',
    'RRC': 'Energy',
    'SCHW': 'Financials',
    'SIRI': 'Communication Services',
    'SJM': 'Consumer Staples',
    'SNA': 'Industrials',
    'SO': 'Utilities',
    'T': 'Communication Services',
    'M': 'Consumer Discretionary',
    'NEE': 'Utilities'}

        
        flattened_data = pd.DataFrame()
        for ticker in tickers:
            if ticker in raw_data:
                ticker_data = raw_data[ticker].reset_index()
                ticker_data['Ticker'] = ticker 
                flattened_data = pd.concat([flattened_data, ticker_data])
        
        flattened_data.rename(columns={
            'Date': 'Tdate',
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price',
            'Volume': 'volume',
        }, inplace=True)
        
        flattened_data['Tdate'] = pd.to_datetime(flattened_data['Tdate'], errors='coerce').dt.date    
        flattened_data['company_name'] = flattened_data['Ticker'].map(ticker_to_name)
        flattened_data['sector'] = flattened_data['Ticker'].map(sector_mapping)
        
        context['task_instance'].xcom_push(key='transformed_data', value=flattened_data)
        logger.info("Data transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}", exc_info=True)
        raise

def load_to_snowflake(**context) -> None:
    """Load transformed data to Snowflake staging"""
    try:
        logger.info("Starting data load to Snowflake staging")
        
        transformed_data = context['task_instance'].xcom_pull(task_ids='transform_stock_data_task', key='transformed_data')
        snowflake_hook = SnowflakeHook(snowflake_conn_id="Snowflake_conn")
        
        conn = snowflake_hook.get_conn()
        database = "STOCKDATA"
        schema = "STOCKSTAGING"
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=transformed_data,
            table_name="STOCK_DATA",
            database=database,
            schema=schema,
            auto_create_table=True
        )
        
        logger.info(f"Data loaded successfully: {nrows} rows in {nchunks} chunks")
        
    except Exception as e:
        logger.error(f"Data load failed: {str(e)}", exc_info=True)
        raise

with DAG(
    'stock_data_pipeline1',
    default_args=default_args,
    description='Daily stock data pipeline',
    schedule='0 22 * * * ',  
    start_date=datetime(2025, 2, 13), 
    catchup=False,
    tags=['stocks', 'finance'],
) as dag:
    
    get_data = PythonOperator(
        task_id='get_stock_data_task',
        python_callable=get_stock_data,
    )
    
    transform_data = PythonOperator(
        task_id='transform_stock_data_task',
        python_callable=transform_stock_data,
    )
    
    load_data = PythonOperator(
        task_id='load_to_snowflake_task',
        python_callable=load_to_snowflake,
    )
    
    get_data >> transform_data >> load_data  
