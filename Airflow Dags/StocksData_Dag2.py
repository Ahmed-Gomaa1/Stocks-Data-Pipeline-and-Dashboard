from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
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

def update_dim_sector(**context) -> None:
    """Update the dimension table for sectors"""
    try:
        logger.info("Starting sector dimension update")
        
        snowflake_hook = SnowflakeHook(snowflake_conn_id="Snowflake_conn")
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()
        
        merge_query = """
        MERGE INTO stockmodeling.dim_sector AS target
        USING (
            SELECT DISTINCT 
                stock_data."sector" as Sector_Name
            FROM STOCKSTAGING.STOCK_DATA
            WHERE stock_data."sector" IS NOT NULL
        ) AS source
        ON target.Sector_Name = source.Sector_Name
        WHEN NOT MATCHED THEN
            INSERT (Sector_Name)
            VALUES (source.Sector_Name);

        """
        
        cursor.execute(merge_query)
        affected_rows = cursor.rowcount
        conn.commit()
        
        logger.info(f"Sector dimension updated successfully. Affected rows: {affected_rows}")
        
    except Exception as e:
        logger.error(f"Sector dimension update failed: {str(e)}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()

def update_dim_company(**context) -> None:
    """Update the dimension table for companies"""
    try:
        logger.info("Starting company dimension update")
        
        snowflake_hook = SnowflakeHook(snowflake_conn_id="Snowflake_conn")
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()
        
        merge_query = """
        MERGE INTO stockmodeling.dim_company AS target
        USING (
            SELECT DISTINCT 
                sd."company_name" as Company_Name,
                ds.Sector_SK
            FROM STOCKSTAGING.STOCK_DATA sd
            JOIN stockmodeling.dim_sector ds ON sd."sector" = ds.Sector_Name
            WHERE sd."company_name" IS NOT NULL
        ) AS source
        ON target.Company_Name = source.Company_Name
        WHEN NOT MATCHED THEN
            INSERT (Company_Name, Sector_SK)
            VALUES (source.Company_Name, source.Sector_SK);

        """
        
        cursor.execute(merge_query)
        affected_rows = cursor.rowcount
        conn.commit()
        
        logger.info(f"Company dimension updated successfully. Affected rows: {affected_rows}")
        
    except Exception as e:
        logger.error(f"Company dimension update failed: {str(e)}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()

def insert_dim_date(**context) -> None:
    """Update the dimension table for dates"""
    try:
        logger.info("Starting date dimension update")
        
        snowflake_hook = SnowflakeHook(snowflake_conn_id="Snowflake_conn")
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()
        
        merge_query = """
        MERGE INTO stockmodeling.dim_date AS target
        USING (
            SELECT DISTINCT 
                "Tdate" AS Trade_Date,
                EXTRACT(YEAR FROM "Tdate") AS Trading_Year,
                EXTRACT(QUARTER FROM "Tdate") AS Trading_Quarter,
                EXTRACT(MONTH FROM "Tdate") AS Trading_Month,
                EXTRACT(DAY FROM "Tdate") AS Trading_Day,
                EXTRACT(DAYOFWEEK FROM "Tdate") AS Day_of_Week,
                CASE WHEN EXTRACT(DAYOFWEEK FROM "Tdate") IN (6, 7) THEN TRUE ELSE FALSE END AS Is_Weekend
            FROM STOCKSTAGING.STOCK_DATA
            WHERE "Tdate" IS NOT NULL
        ) AS source
        ON target.Trade_Date = source.Trade_Date
        WHEN NOT MATCHED THEN
            INSERT (
                Trade_Date, Trading_Year, Trading_Quarter, Trading_Month,
                Trading_Day, Day_of_Week, Is_Weekend
            )
            VALUES (
                source.Trade_Date, source.Trading_Year, source.Trading_Quarter,
                source.Trading_Month, source.Trading_Day, source.Day_of_Week,
                source.Is_Weekend
            );
        """
        
        cursor.execute(merge_query)
        affected_rows = cursor.rowcount
        conn.commit()
        
        logger.info(f"Date dimension updated successfully. Affected rows: {affected_rows}")
        
    except Exception as e:
        logger.error(f"Date dimension update failed: {str(e)}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()

def insert_fact_stocks_prices(**context) -> None:
    """Update the fact table for stock prices"""
    try:
        logger.info("Starting fact table update")
        
        snowflake_hook = SnowflakeHook(snowflake_conn_id="Snowflake_conn")
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()
        
        merge_query = """
        MERGE INTO stockmodeling.fact_stocks_prices AS target
        USING (
            SELECT DISTINCT
                d.Date_SK,
                c.Company_SK,
                FIRST_VALUE(s."open_price") OVER (
                    PARTITION BY d.Date_SK, c.Company_SK ORDER BY s."Tdate"
                ) as Open_Price,
                FIRST_VALUE(s."high_price") OVER (
                    PARTITION BY d.Date_SK, c.Company_SK ORDER BY s."Tdate"
                ) as High_Price,
                FIRST_VALUE(s."low_price") OVER (
                    PARTITION BY d.Date_SK, c.Company_SK ORDER BY s."Tdate"
                ) as Low_Price,
                FIRST_VALUE(s."close_price") OVER (
                    PARTITION BY d.Date_SK, c.Company_SK ORDER BY s."Tdate"
                ) as Close_Price,
                FIRST_VALUE(s."Adj Close") OVER (
                    PARTITION BY d.Date_SK, c.Company_SK ORDER BY s."Tdate"
                ) as Adj_Close,
                FIRST_VALUE(s."volume") OVER (
                    PARTITION BY d.Date_SK, c.Company_SK ORDER BY s."Tdate"
                ) as NumberofShares_Traded
            FROM STOCKSTAGING.STOCK_DATA s
            INNER JOIN stockmodeling.dim_date d ON s."Tdate" = d.Trade_Date
            INNER JOIN stockmodeling.dim_company c ON s."company_name" = c.Company_Name
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY d.Date_SK, c.Company_SK ORDER BY s."Tdate"
            ) = 1
        ) AS source
        ON target.Date_SK = source.Date_SK 
        AND target.Company_SK = source.Company_SK
        WHEN NOT MATCHED THEN
            INSERT (
                Date_SK, Company_SK, Open_Price, High_Price, Low_Price,
                Close_Price, Adj_Close, NumberofShares_Traded
            )
            VALUES (
                source.Date_SK, source.Company_SK, source.Open_Price,
                source.High_Price, source.Low_Price, source.Close_Price,
                source.Adj_Close, source.NumberofShares_Traded
            );
        """
        
        cursor.execute(merge_query)
        affected_rows = cursor.rowcount
        conn.commit()
        
        logger.info(f"Fact updated successfully. Affected rows: {affected_rows}")
        
    except Exception as e:
        logger.error(f"Fact update failed: {str(e)}", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()
        
with DAG(
    'stock_data_pipeline2',
    default_args=default_args,
    description='Daily stock data pipeline',
    schedule='30 22 * * *',  
    start_date=datetime(2025, 2, 13), 
    catchup=False,
    tags=['stocks', 'finance'],
) as dag:
        
    update_dim_sector = PythonOperator(
        task_id='update_dim_sector_task',
        python_callable=update_dim_sector,
    )
    update_dim_company = PythonOperator(  
        task_id='update_dim_company_task',
        python_callable=update_dim_company,
    )
    insert_dim_date = PythonOperator(  
        task_id='insert_dim_date_task',
        python_callable=insert_dim_date,
    )
    insert_fact_stocks_prices =PythonOperator(
        task_id='insert_fact_stocks_prices_task',
        python_callable=insert_fact_stocks_prices,
    )
    
    update_dim_sector >> update_dim_company >> insert_dim_date >> insert_fact_stocks_prices
