import pandas as pd
from .connection import DatabaseConnection

class StockDataOperations:
    """股票行情数据操作类"""

    def __init__(self, db_config):
        self.db_config = db_config

    def create_table(self):
        """创建股票数据表"""
        with DatabaseConnection(self.db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(10),
                    date DATE,
                    open FLOAT,
                    close FLOAT,
                    high FLOAT,
                    low FLOAT,
                    volume BIGINT,
                    amount FLOAT,
                    swing FLOAT,
                    change_percent FLOAT,
                    change_amount FLOAT,
                    turnover_rate FLOAT,
                    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""")
                conn.commit()

    def bulk_load(self, df):
        """批量加载行情数据"""
        self.create_table()
        with DatabaseConnection(self.db_config) as conn:
            with conn.cursor() as cursor:
                insert_sql = """
                INSERT INTO stock_data 
                (stock_code, date, open, close, high, low, volume, amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                for _, row in df.iterrows():
                    cursor.execute(insert_sql, (
                        row['代码'],
                        row['日期'],
                        row['开盘'],
                        row['收盘'],
                        row['最高'],
                        row['最低'],
                        row['成交量'],
                        row['成交额']
                    ))
                conn.commit()

    def query_data(self, stock_code=None, start_date=None, end_date=None):
        """查询行情数据"""
        with DatabaseConnection(self.db_config) as conn:
            with conn.cursor() as cursor:
                query = "SELECT * FROM stock_data WHERE 1=1"
                params = []
                
                if stock_code:
                    query += " AND stock_code = %s"
                    params.append(stock_code)
                
                if start_date:
                    query += " AND date >= %s"
                    params.append(start_date)
                
                if end_date:
                    query += " AND date <= %s"
                    params.append(end_date)
                
                cursor.execute(query, params)
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]