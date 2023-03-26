import pandas as pd
import numpy as np
import datetime

from dag.utils import df_difference

def _dataframe_factory():
    df = pd.DataFrame({
        'id': range(1, 11),
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Emma', 'Frank', 'Grace', 'Henry', 'Isabel', 'Jack'],
        'age': np.random.randint(18, 65, 10),
        'salary': np.random.randint(30000, 100000, 10),
        "hire_date": pd.date_range(start=datetime.date.today(), periods=10)
    })
    return df

def _get_additional_row() -> pd.DataFrame:
    new_row = pd.DataFrame({
        'id': 11,
        'name': 'Kate',
        'age': 28,
        'salary': 50000,
        'hire_date': pd.date_range(start=datetime.date.today(), periods=1)
    }, index=[0])
    return new_row

#######################################

def test_df_difference():
    old_df = _dataframe_factory()
    additional_row = _get_additional_row()
    new_df = pd.concat([old_df, additional_row]).reset_index(drop=True)
    row, containing_df = df_difference(old_df, new_df)
    assert row.equals(additional_row) and containing_df[0] == "right_only"