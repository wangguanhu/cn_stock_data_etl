import pandas as pd
import akshare as ak
import sys

sys.path.append(".")
from datetime import datetime
from configs.database_config import DB_CONFIG
from src.etl.transform import StockDataTransformer
from src.etl.load import StockDataLoader

def get_industry_data():
    """获取并处理板块数据"""
    raw_data = ak.stock_board_industry_name_em()
    transformed_data = StockDataTransformer.transform_data(raw_data).rename(columns={
        '板块名称': 'industry_name',
        '板块代码': 'industry_code'
    })
    transformed_data.to_csv(f'data/industry_{datetime.today().strftime("%Y%m%d")}.csv', index=False)
    # StockDataLoader.load_data(transformed_data, DB_CONFIG, table_name='industry_data')
    # return transformed_data

if __name__ == "__main__":
    get_industry_data()
    # df = ak.stock_board_industry_name_em()
