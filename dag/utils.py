import json
import pickle
from typing import Tuple, List

import pandas as pd
from pathlib import Path


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