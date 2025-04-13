import akshare as ak
import pandas as pd

class StockDataExtractor:
    """股票数据提取类"""
    
    @staticmethod
    def extract_stock_data(stock_code: str, start_date: str, end_date: str, adjust = 'qfq') -> pd.DataFrame:
        """从AKShare获取股票数据
        
        Args:
            stock_code: 股票代码
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            
        Returns:
            pd.DataFrame: 股票历史数据
        """
        return ak.stock_zh_a_hist(
            symbol=stock_code, 
            start_date=start_date, 
            end_date=end_date, 
            adjust=adjust
        )

    @staticmethod
    def extract_stock_spot() -> pd.DataFrame:
        """从AKShare获取股票数据

        Args:
            

        Returns:
            pd.DataFrame: 股票历史数据
        """
        return ak.stock_zh_a_spot_em()