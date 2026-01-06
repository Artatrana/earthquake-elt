# ============================================================================
# FILE: src/database.py
# ============================================================================
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_batch
from contextlib import contextmanager
from typing import Optional, List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class Database:
    """Database connection manager with connection pooling."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pool: Optional[SimpleConnectionPool] = None

    def initialize_pool(self):
        """Initialize connection pool."""
        db_config = self.config["database"]
        self.pool = SimpleConnectionPool(
            minconn=1,
            maxconn=db_config["pool_size"],
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
        )
        logger.info("Database connection pool initialized")

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        if not self.pool:
            self.initialize_pool()
        conn = self.pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self.pool.putconn(conn)

    def execute_sql_file(self, filepath: str) -> None:
        """Execute SQL from file."""
        with open(filepath, "r") as f:
            sql = f.read()
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        logger.info(f"Executed SQL file: {filepath}")

    def bulk_insert(
        self, table: str, records: List[Dict], page_size: int = 1000
    ) -> int:
        """Bulk insert records using execute_batch."""
        if not records:
            return 0
        columns = records[0].keys()
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        values = [[rec[col] for col in columns] for rec in records]
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_batch(cur, sql, values, page_size=page_size)
        return len(records)

    def close_pool(self):
        """Close all connections in pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")
