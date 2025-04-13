import pymysql
from datetime import datetime, timedelta
import pandas as pd

class StockDataLoader:
    """股票数据加载类"""
    
    @staticmethod
    def load_data(df: pd.DataFrame, db_config: dict) -> None:
        """将数据写入MySQL"""
        conn = pymysql.connect(**db_config)
        try:
            with conn.cursor() as cursor:
                # 检查并删除已存在的数据
                check_sql = """
                DELETE FROM stock_daily 
                WHERE stock_code = %s AND trade_date = %s
                """
                
                # 准备批量插入数据
                insert_sql = """
                INSERT INTO stock_daily 
                (stock_code, trade_date, open, close, high, low, volume, amount, swing, change_percent, change_amount, turnover_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                # 先删除可能存在的重复数据
                for _, row in df.iterrows():
                    cursor.execute(check_sql, (row['stock_code'], row['trade_date']))
                
                # 将DataFrame转换为适合批量插入的元组列表
                data_tuples = [(
                    row['stock_code'],
                    row['trade_date'],
                    row['open'],
                    row['close'],
                    row['high'],
                    row['low'],
                    row['volume'],
                    row['amount'],
                    row['swing'],
                    row['change_percent'],
                    row['change_amount'],
                    row['turnover_rate']
                ) for _, row in df.iterrows()]
                
                # 执行批量插入
                cursor.executemany(insert_sql, data_tuples)
                
                # 清理超过一年的旧数据
                one_year_ago = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')
                cleanup_sql = "DELETE FROM stock_daily WHERE trade_date < %s"
                cursor.execute(cleanup_sql, (one_year_ago,))
                
            conn.commit()
        finally:
            conn.close()