from pathlib import Path

import pandas as pd
import psycopg2



class Database:
    query_repo = Path("resources")

    def __init__(self):
        self.host = "localhost"
        self.port = 5432
        self.dataabse = "dataBrain"
        self.user = "doms"
        self.password = ""

    def execute(self, query: str) -> pd.DataFrame:
        results = []
        with psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.username,
            password=self.password
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()
        return results
    
    def get_query(self, query_name: str) -> str:
        query_path = self.query_repo / f"{query_name}.sql"
        sql = []
        with open(query_path, "r") as fp:
            sql = fp.readlines()
        return sql
    