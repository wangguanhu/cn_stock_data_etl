import unittest
from src.database.connection import DatabaseConnection
from configs.database_config import DB_CONFIG

class TestDatabaseConnection(unittest.TestCase):
    """测试数据库连接类"""
    
    def setUp(self):
        """测试初始化"""
        self.db_config = DB_CONFIG
        
    def test_connect(self):
        """测试连接建立"""
        db_conn = DatabaseConnection(self.db_config)
        conn = db_conn.connect()
        self.assertIsNotNone(conn)
        db_conn.close()
        
    def test_close(self):
        """测试连接关闭"""
        db_conn = DatabaseConnection(self.db_config)
        conn = db_conn.connect()
        self.assertIsNotNone(conn)
        db_conn.close()
        
    def test_context_manager(self):
        """测试上下文管理器"""
        with DatabaseConnection(self.db_config) as conn:
            self.assertIsNotNone(conn)

if __name__ == '__main__':
    unittest.main()