import pandas as pd
from sqlalchemy import create_engine
from configs.database_config import DB_CONFIG

class StockDataOperations:
    """股票数据库操作类"""
    
    def __init__(self, db_config):
        """初始化数据库操作"""
        self.db_config = db_config
    
    def create_stock_table(self):
        """创建股票数据表（如果不存在）"""
        with DatabaseConnection(self.db_config) as conn:
            with conn.cursor() as cursor:
                create_table_sql = """
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
                )"""
                cursor.execute(create_table_sql)
                conn.commit()
    
    def load_stock_data(self, df):
        """将股票数据加载到数据库"""
        self.create_stock_table()
        
        with DatabaseConnection(self.db_config) as conn:
            with conn.cursor() as cursor:
                # 插入数据
                for _, row in df.iterrows():
                    insert_sql = """
                    INSERT INTO stock_data 
                    (stock_code, date, open, close, high, low, volume, amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
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
                
    def query_stock_data(self, stock_code=None, start_date=None, end_date=None):
        """查询股票数据"""
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