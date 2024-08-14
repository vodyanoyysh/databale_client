import os
import time
from sqlite3 import ProgrammingError, DatabaseError
from string import Template
from typing import Optional, List

from sqlalchemy import Engine, MetaData, Connection, text
from sqlalchemy.exc import ResourceClosedError


class DBClient:
    def __init__(self, engine: Engine, future=True):
        self.engine: Engine = engine
        self.connection: Optional[Connection] = None
        self.metadata: Optional[MetaData] = None
        self.future = future

    def __del__(self):
        self.close_connection()

    def create_connection(self) -> None:
        if not self.connection:
            self.connection = self.engine.connect()

    def close_connection(self) -> None:
        if self.connection:
            self.connection.close()
            self.engine.dispose()
            self.connection = None

    def create_metadata(self) -> None:
        if not self.metadata:
            self.create_connection()
            self.metadata = MetaData()
            self.metadata.reflect(bind=self.engine)

    def get_data(self, sql: str, encoding: str = "utf-8", print_query=False, max_attempts=5, **kwargs) -> List[dict]:
        if not self.connection:
            self.create_connection()

        if os.path.exists(sql):
            sql_query = Template(self.get_sql_query(sql, encoding))
        else:
            sql_query = Template(sql)
        sql_query = sql_query.substitute(**kwargs)

        if print_query:
            print(sql_query)

        result = self._execute(sql_query, max_attempts)
        return result

    @staticmethod
    def get_sql_query(sql_file: str, encoding: str = "utf-8") -> str:
        """
        Читает файл и возвращает его содержимое
        :param sql_file: путь до файла
        :param encoding: кодировка
        :return:
        """
        with open(sql_file, "r", encoding=encoding) as file:
            sql_query = file.read()
        return sql_query

    @staticmethod
    def parse_dsn(dsn: str) -> dict:
        result = dict()
        dsn_parts = dsn.split('@')
        for dsn_part in dsn_parts:
            if "/" in dsn_part:
                if ":" in dsn_part.split("/")[-1]:
                    result["user"] = dsn_part.split("/")[-1].split(":")[0]
                    result["password"] = dsn_part.split("/")[-1].split(":")[1]
                if "/" in dsn_part:
                    result["server"] = dsn_part.split("/")[-2].split(":")[0]
                    result["database"] = dsn_part.split("/")[-1]
        return result

    def _execute(self, script, max_attempts) -> List[dict]:
        result = []
        transaction = None
        for _ in range(max_attempts):
            try:
                if not self.future:
                    transaction = self.connection.begin()
                result = self.connection.execute(text(script))
                try:
                    result = list(result.mappings())
                    result = [dict(r) for r in result]
                except ResourceClosedError:
                    pass
                self.commit(transaction)
                break
            except ProgrammingError as ex:
                raise ex
            except DatabaseError:
                self.rollback(transaction)
                try:
                    self.close_connection()
                except Exception as err:
                    print(err)
                self.create_connection()
                time.sleep(10)
        return result

    def commit(self, transaction=None):
        if transaction:
            transaction.commit()
        else:
            self.connection.commit()

    def rollback(self, transaction=None):
        if transaction:
            transaction.rollback()
        else:
            self.connection.rollback()
