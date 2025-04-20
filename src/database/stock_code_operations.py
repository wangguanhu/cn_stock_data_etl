import pandas as pd
from .connection import DatabaseConnection
from configs.database_config import DB_CONFIG

class StockCodeOperations:
    """股票代码表操作类"""
    
    def __init__(self, db_config):
        self.db_config = db_config

    def create_table(self):
        """创建股票代码表"""
        with DatabaseConnection(self.db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS cn_stock_code (
                    stock_code VARCHAR(10) PRIMARY KEY,
                    stock_name VARCHAR(100),
                    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""")
                conn.commit()

    def get_all_stock_codes(self):
        """获取所有股票代码列表"""
        with DatabaseConnection(self.db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT stock_code FROM cn_stock_code")
                result = cursor.fetchall()
                return [row[0] for row in result]

    def bulk_load(self, df):
        """批量加载股票代码数据"""
        self.create_table()
        with DatabaseConnection(self.db_config) as conn:
            with conn.cursor() as cursor:
                insert_sql = """
                INSERT INTO cn_stock_code (stock_code, stock_name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE stock_name = VALUES(stock_name)
                """
                for _, row in df.iterrows():
                    cursor.execute(insert_sql, (
                        row['stock_code'],
                        row['stock_name']
                    ))
                conn.commit()