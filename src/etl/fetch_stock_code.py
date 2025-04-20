import akshare as ak
import sys
sys.path.append('.')
from src.database.stock_code_operations import StockCodeOperations
from configs.database_config import DB_CONFIG
import pandas as pd

def fetch_and_load_stock_codes():
    """获取A股股票代码并加载到数据库"""
    
    # 从AKShare获取数据
    df = ak.stock_info_a_code_name()
    
    # 规范列名并筛选字段
    df = df[['code', 'name']].rename(columns={
        'code': 'stock_code',
        'name': 'stock_name'
    })
    
    # 初始化数据库操作类
    db_ops = StockCodeOperations(DB_CONFIG)
    
    # 创建股票代码表并插入数据
    db_ops.create_table()
    db_ops.bulk_load(df)

if __name__ == '__main__':
    fetch_and_load_stock_codes()