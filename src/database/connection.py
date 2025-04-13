import pymysql

class DatabaseConnection:
    """数据库连接管理类"""
    
    def __init__(self, db_config):
        """初始化数据库连接配置"""
        self.db_config = db_config
        self.connection = None
    
    def connect(self):
        """建立数据库连接"""
        if not self.connection:
            self.connection = pymysql.connect(**self.db_config)
        return self.connection
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def __enter__(self):
        """上下文管理器入口"""
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()