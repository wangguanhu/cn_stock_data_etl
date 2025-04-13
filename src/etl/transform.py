import pandas as pd

class StockDataTransformer:
    """股票数据转换类"""
    
    @staticmethod
    def transform_data(df: pd.DataFrame) -> pd.DataFrame:
        """数据转换/清洗"""
        # 列名映射
        column_mapping = {
            '股票代码': 'stock_code',
            '日期': 'trade_date',
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume',
            '成交额': 'amount',
            '振幅': 'swing',
            '涨跌幅': 'change_percent',
            '涨跌额': 'change_amount',
            '换手率': 'turnover_rate'
        }
        
        # 重命名列
        return df.rename(columns=column_mapping)