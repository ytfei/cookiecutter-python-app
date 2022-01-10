import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import inspect
from sqlalchemy.engine.base import Transaction
from sqlalchemy.sql import text
from .logger import logger
from .config import config
from enum import Enum

__all__ = "get_engine", "execute", "query_one", "query", "connect"


class DbType(Enum):
    """
    当前支持的数据库类型
    """
    MYSQL = "mysql+pymysql"
    SQLITE = "sqlite+pysqlite"


def connect():
    new_con = Conn()
    return new_con()


class Conn(object):
    _engine = None

    def __init__(self) -> None:
        super(Conn, self).__init__()

        if not Conn._engine:

            if config.db.connection_str:
                logger.info(
                    f"using direct connection:{config.db.connection_str}")
                Conn._engine = create_engine(config.db.connection_str)
            else:
                _prefix = DbType[config.db.type.upper()]

                logger.info(
                    f"initialize db engine to {_prefix}://{config.db.host}:{config.db.port}/{config.db.database}")
                Conn._engine = create_engine(
                    f"{_prefix}://{config.db.username}:{config.db.password}@{config.db.host}:{config.db.port}/{config.db.database}")

    def __call__(self):
        return Conn._engine.connect()


def get_engine():
    conn = Conn()  # make sure engine is init
    return Conn._engine


def execute(sql, params=None):
    statement = text(sql)

    with connect() as con:
        return con.execute(statement, params)


def query_one(sql, params=None):
    rs = execute(sql, params)
    row = rs.fetchone()
    return dict(row)


def query(sql, params=None):
    rs = execute(sql, params)
    return [dict(row) for row in rs.fetchall()]

# TODO: with in Transaction
