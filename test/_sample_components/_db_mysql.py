import time
from typing import Any, Tuple, Union

import mysql.connector
from multirunnable.persistence.database.operator import DatabaseOperator
from multirunnable.persistence.database.strategy import (
    BaseConnectionPool,
    BaseSingleConnection,
    get_connection_pool,
)
from mysql.connector.connection import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector.errors import PoolError
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection


class MySQLSingleConnection(BaseSingleConnection):
    def _connect_database(self, **kwargs) -> MySQLConnection:
        _connection = mysql.connector.connect(**kwargs)
        return _connection

    def _is_connected(self) -> bool:
        return self.current_connection.is_connected()

    def commit(self) -> None:
        self.current_connection.commit()

    def _close_connection(self) -> None:
        if self.current_connection is not None and self.current_connection.is_connected() is True:
            self.current_connection.close()


class MySQLDriverConnectionPool(BaseConnectionPool):
    def connect_database(self, **kwargs) -> MySQLConnectionPool:
        connection_pool = MySQLConnectionPool(**kwargs)
        return connection_pool

    def _get_one_connection(self, pool_name: str = "", **kwargs) -> PooledMySQLConnection:
        while True:
            try:
                connection = get_connection_pool(pool_name=pool_name).get_connection()
                return connection
            except PoolError as e:
                time.sleep(5)
                raise e
            except AttributeError as ae:
                if "'NoneType' object has no attribute 'get_connection'" in str(ae):
                    raise ConnectionError(
                        f"Cannot get the one connection instance from connection pool because it "
                        f"doesn't exist the connection pool with the name '{pool_name}'."
                    ) from ae
                else:
                    raise ae

    def _commit(self, conn: PooledMySQLConnection) -> None:
        conn.commit()

    def _close_connection(self, conn: PooledMySQLConnection) -> None:
        if conn is not None:
            conn.close()

    def close_pool(self, pool) -> None:
        # self.get_connection_pool(pool_name=pool_name).close()
        pass


class MySQLOperator(DatabaseOperator):
    def __init__(self, conn_strategy: Union[BaseSingleConnection, BaseConnectionPool], db_config={}):
        super().__init__(conn_strategy=conn_strategy, db_config=db_config)

    def initial_cursor(self, connection: Union[MySQLConnection, PooledMySQLConnection]) -> MySQLCursor:
        return connection.cursor(buffered=True)

    @property
    def column_names(self) -> MySQLCursor:
        return self._cursor.column_names

    @property
    def row_count(self) -> MySQLCursor:
        return self._cursor.rowcount

    def execute(self, operator: Any, params: Tuple = None, multi: bool = False) -> MySQLCursor:
        return self._cursor.execute(operation=operator, params=params, multi=multi)

    def execute_many(self, operator: Any, seq_params=None) -> MySQLCursor:
        return self._cursor.executemany(operation=operator, seq_params=seq_params)

    def fetch_one(self) -> list:
        return self._cursor.fetchone()

    def fetch_many(self, size: int = None) -> list:
        return self._cursor.fetchmany(size=size)

    def fetch_all(self) -> list:
        return self._cursor.fetchall()

    def reset(self) -> None:
        self._cursor.reset()

    def close_cursor(self) -> None:
        self._cursor.close()
