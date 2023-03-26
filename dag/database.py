from pathlib import Path
import os

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from dag.utils import read_json_file

ROOT_PATH: Path = Path(os.path.abspath(os.path.dirname(__file__))).parent
class Database:
    QUERY_REPO_PATH: Path = ROOT_PATH / Path("resources")

    def __init__(self, credentials_path: Path):
        self.credentials = read_json_file(credentials_path)

    def get_connection_string(self):
        user = self.credentials.get("user")
        password = self.credentials.get("password")
        host = self.credentials.get("host")
        port = self.credentials.get("port")
        database = self.credentials.get("database")
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def execute(self, query: str) -> pd.DataFrame:
        connection_str = self.get_connection_string()
        engine = create_engine(connection_str)
        result = pd.read_sql(query, engine)
        return result
    
    @classmethod
    def get_query(cls, query_name: str) -> str:
        query_path = cls.QUERY_REPO_PATH / f"{query_name}.sql"
        assert query_path.exists(), "Query does not exists"
        sql = []
        with open(query_path, "r") as fp:
            sql = fp.read()
        return sql
    