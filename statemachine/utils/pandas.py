from typing import List, Union

import pandas as pd


def left_anti(left_df: pd.DataFrame, right_df: pd.DataFrame, on: Union[str, List[str]]):
    """Performs a left anti-join on left df"""
    outer = left_df.merge(right_df[on], on=on, how="outer", indicator=True)
    return outer[(outer._merge == "left_only")].drop("_merge", axis=1)
