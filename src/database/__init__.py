import sqlalchemy
from abc import ABCMeta, abstractclassmethod
from dataclasses import dataclass
from typing import Union, List

from src.database.Queries import PostgresSelectQuery, PostgresDMLQuery

@dataclass
class SQLConnectionDetails:
    host: str
    port: int
    database: str
    username: str
    password: str
    pool_size: int = 5
    pool_timeout: int = 30

class SQLDatabase(metaclass=ABCMeta):
    def __init__(self, connection_details = SQLConnectionDetails):
        self._conn_details = connection_details

    @abstractclassmethod
    def _build_connection_string(self) -> str:
        pass

    @abstractclassmethod
    def get_engine(self) -> sqlalchemy.Engine:
        pass
    
    @abstractclassmethod
    def execute(self, query: str, args: dict = None):
        pass

    @abstractclassmethod
    def select(self, query: str, args: dict = None) -> List[dict]:
        pass

class PostgresEngineCreateError(Exception):
    """
    When the SQLAlchemy create_engine function fails to build
    an object.
    """
    def __init__(self, message: str, host: str, user: str):
        super().__init__(
            "Failed to create SQLAlchemy engine for [Host: {0} | User: {1}]. {2}".format(
                host, user, message
        ))

class PostgresDatabase(SQLDatabase):
    def __init__(self, connection_details: SQLConnectionDetails):
        super().__init__(connection_details = connection_details)

    def _build_connection_string(self) -> str:
        return "postgresql://{0}:{1}@{2}:{3}/{4}".format(
            self._conn_details.username,
            self._conn_details.password,
            self._conn_details.host,
            self._conn_details.port,
            self._conn_details.database
        )

    def get_engine(self) -> sqlalchemy.Engine:
        """
        Responds with a SQLAlchemy Engine object targeted at the injected
        database connection details.
        """
        url = self._build_connection_string()
        try:
            # Documentation Reference:
            # https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine
            engine = sqlalchemy.create_engine(
                url = url,
                pool_size = self._conn_details.pool_size,
                pool_timeout = self._conn_details.pool_timeout
            )
            return engine
        except Exception as create_engine_err:
            raise PostgresEngineCreateError(
                message = create_engine_err.args,
                host = self._conn_details.host,
                user = self._conn_details.username
            )

    def execute(self, query: Union[PostgresDMLQuery, str], args: dict = None):
        """
        Performs the provided INSERT/UPDATE/DELETE/STORED PROCEDURE query
        on the injected database connection detials.
        """
        engine: sqlalchemy.Engine = self.get_engine()
        with engine.connect() as conn:
            conn.execute(
                statement = sqlalchemy.sql.text(str(query)),
                parameters = args
            )
            conn.commit()

    def select(self, query: Union[PostgresSelectQuery, str], args: dict = None) -> List[dict]:
        """
        Performs the provide SELECT query on the injected database
        connection details.
        """
        engine: sqlalchemy.Engine = self.get_engine()
        with engine.connect() as conn:
            results: sqlalchemy.CursorResult = conn.execute(
                statement = sqlalchemy.sql.text(str(query)),
                parameters = args
            )
            return [row._mapping for row in results]