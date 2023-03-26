import requests
from pathlib import Path
from string import Template
import sys
import os

import xml.etree.ElementTree as ET
import pandas as pd
from dagster import asset
from sqlalchemy import create_engine

from dag.database import Database, ROOT_PATH
from dag.entity.interest_rate import InterestRate
from dag.utils import df_difference

ROOT_PATH: Path = Path(os.path.abspath(os.path.dirname(__file__))).parent

@asset(
    description="Extract the XML data for the ECB deposit facility rate"
)
def extract_dfr() -> str:
    url = "https://sdw-wsrest.ecb.europa.eu/service/data/FM/D.U2.EUR.4F.KR.DFR.CHG?format=genericdata"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        raise requests.exceptions.RequestException(f"Error while accessing the resource. Request status code: {response.status_code}")

@asset(
    description="Transform the DFR data from XML to pandas dataframe"
)
def transform_dfr(extract_dfr: str) -> pd.DataFrame:
    root = ET.fromstring(extract_dfr)
    data_set = root.find('.//{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}DataSet')
    series = data_set.find('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Series')
    observations = series.findall('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Obs')
    obs_data = []
    for obs in observations:
        obs_date = obs.find('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}ObsDimension').attrib['value']
        obs_value = obs.find('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}ObsValue').attrib['value']
        d = {'date': obs_date, 'value': obs_value}
        obs_attrs = obs.find('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Attributes')
        for field in obs_attrs: d[field.get("id").lower()] = field.get("value")
        obs_data.append(d)
    obs_df = pd.DataFrame(obs_data)
    obs_df["name"] = "DFR"
    obs_df["date"] = pd.to_datetime(obs_df["date"])
    obs_df["value"] = obs_df["value"].astype(float)
    obs_df["name"] = obs_df["name"].astype(str)
    return obs_df

@asset(
    description="Creates new id column based on the hash of the combined columns"
)
def transform_dfr_primary_key(transform_dfr: pd.DataFrame) -> pd.DataFrame:
    transform_dfr["id"] = transform_dfr["date"].astype(str) + transform_dfr["name"]
    transform_dfr["id"] = transform_dfr["id"].apply(lambda x: hash(x) % sys.maxsize)
    return transform_dfr


@asset(
    description="Loads the DFR data into Postgres database"
)
def load_dfr(transform_dfr_primary_key: pd.DataFrame):
    result = None
    credential_path = ROOT_PATH / "credentials.json" 
    database = Database(credential_path)
    template = database.get_query(Path("relation_exist"))
    sql = Template(template).substitute(schema_name="public", 
                                        table_name="interest_rate")
    connection_str = database.get_connection_string()
    engine = create_engine(connection_str)
    does_table_exist = database.execute(sql)["does_exist"].item()
    if not does_table_exist:
        InterestRate.metadata.create_all(engine)
        result = transform_dfr_primary_key
    else:
        old_df = pd.read_sql_table(InterestRate.__tablename__, engine)
        new_df = transform_dfr_primary_key
        result = df_difference(old_df, new_df)
    transform_dfr_primary_key.to_sql(InterestRate.__tablename__, engine, if_exists='replace', index=False)
    engine.dispose()
    return result
    
