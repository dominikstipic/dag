import json
import pickle
from typing import Tuple, List
import base64
from io import BytesIO

import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

def read_json_file(file_path: Path) -> dict:
    assert file_path.exists(), f"Path {file_path} does not exist"
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def load_pickle(file_path: Path) -> pickle:
    assert file_path.exists(), "Path does not exist"
    with open(file_path, 'rb') as f:
        data = pickle.load(f)
    return data


def df_difference(old_df: pd.DataFrame, new_df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    assert (old_df.dtypes == new_df.dtypes).all(), "Dataframes don't have equal schema!"
    merged = pd.merge(old_df, new_df, how='outer', indicator=True)
    diff = merged.loc[merged['_merge'] != 'both']
    containing_set = list(diff["_merge"])
    del diff["_merge"]
    diff = diff.reset_index(drop=True)
    return diff, containing_set


def dataframe_2d_plot_to_markdown(df: pd.DataFrame, x_name: str, y_name: str, title: str=None) -> str:
    plt.plot(df["date"], df["value"])
    plt.grid()
    if title:
        plt.title(title) 
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"
    return md_content