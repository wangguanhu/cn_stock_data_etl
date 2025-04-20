import pandas as pd
from sqlalchemy import create_engine
from configs.database_config import DB_CONFIG

class StockDataOperations:
    """股票数据库操作类"""
    
    def __init__(self, db_config):
        """初始化数据库操作"""
        self.db_config = db_config