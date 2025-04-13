import random
import time
from prefect import flow, task
from datetime import datetime, timedelta
import sys

sys.path.append(".")
from configs.stock_config import STOCK_LIST

from configs.database_config import DB_CONFIG

from prefect import flow, task
from src.etl.extract import StockDataExtractor
from src.etl.transform import StockDataTransformer
from src.etl.load import StockDataLoader
import pandas as pd


@task
def extract_data_past_year_qfq(stock_code) -> pd.DataFrame:
    """从AKShare获取股票数据"""
    today = datetime.today().strftime("%Y%m%d")
    one_year_ago = (datetime.today() - timedelta(days=365)).strftime("%Y%m%d")
    return StockDataExtractor.extract_stock_data(
        stock_code, one_year_ago, today, adjust="qfq"
    )


@task
def transform_data(df):
    """数据转换/清洗"""
    return StockDataTransformer.transform_data(df)


@task
def load_data(df):
    """将数据写入MySQL"""
    StockDataLoader.load_data(df, DB_CONFIG)


@flow(name="stock_etl_flow")
def stock_etl_flow():
    """Prefect工作流：ETL主流程"""
    time.sleep(random.uniform(10 - 3600))
    for stock_code in STOCK_LIST:
        raw_data = extract_data_past_year_qfq(stock_code)
        transformed_data = transform_data(raw_data)
        load_data(transformed_data)


if __name__ == "__main__":
    stock_etl_flow.server(name="daily_stock_data")
