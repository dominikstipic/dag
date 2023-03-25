import requests
from pathlib import Path

import xml.etree.ElementTree as ET
import pandas as pd
from dagster import asset

from dag.database import Database


@asset(
    description="Extract the XML data for the ECB deposit facility rate"
)
def extract_dfr() -> Path:
    url = "https://sdw-wsrest.ecb.europa.eu/service/data/FM/D.U2.EUR.4F.KR.DFR.CHG?format=genericdata"
    file_path = Path("data.xml")
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, 'w') as f:
            f.write(response.text)
    else:
        raise requests.exceptions.RequestException(f"Error while accessing the resource. Request status code: {response.status_code}")
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist")
    return file_path



@asset(
    description="Transform the DFR data into pandas dataframe"
)
def transform_dfr(extract_dfr):
    tree = ET.parse(extract_dfr)
    root = tree.getroot()

    data_set = root.find('.//{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}DataSet')
    series = data_set.find('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Series')
    observations = series.findall('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Obs')

    obs_data = []
    for obs in observations:
            obs_date = obs.find('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}ObsDimension').attrib['value']
            obs_value = obs.find('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}ObsValue').attrib['value']
            d = {'date': obs_date, 'value': obs_value}
            obs_attrs = obs.find('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Attributes')
            for field in obs_attrs: d[field.get("id")] = field.get("value")
            obs_data.append(d)
    obs_df = pd.DataFrame(obs_data)
    obs_df["name"] = "DFR"
    obs_df["date"] = pd.to_datetime(obs_df["date"])
    obs_df["value"] = obs_df["value"].astype(float)
    obs_df["name"] = obs_df["name"].astype(str)
    return obs_df

@asset(
    description="Loads the DFR data into Postgres database"
)
def load_dfr(transform_dfr):
    pass