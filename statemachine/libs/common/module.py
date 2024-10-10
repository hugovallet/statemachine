import logging
import pathlib
from abc import ABC, abstractmethod

import pandas as pd
from statemachine.constants import ROOT_DIR
from statemachine.utils.funcs import protected
from statemachine.utils.string import camel_to_snake


log = logging.getLogger(__name__)


class Module(ABC):
    """Base module oject. Defines standard methods that are common to all modules (e.g. module naming)"""

    def __str__(self) -> str:
        return self.get_module_name()

    def get_output_dir_path(self) -> pathlib.Path:
        """Standard module output directory name"""
        return ROOT_DIR / "data" / "outputs"

    @protected
    def read_module_output(self, folder_suffix: str = "", **kwargs) -> pd.DataFrame:
        """Returns latest module output dataframe"""
        module_output_path = (
            self.get_output_dir_path()
            / folder_suffix
            / f"{self.get_module_name()}.parquet"
        )
        print(f"Reading module output from {module_output_path}")
        df = pd.read_parquet(module_output_path, engine="fastparquet", **kwargs)
        return df

    @protected
    def save_module_output(
        self,
        df: pd.DataFrame,
        folder_suffix: str = "",
        **kwargs,
    ):
        """Standard method to save module output

        Write module output to parquet or csv. PySpark/Pandas will be used to
        write the data based on the instance type of the passed dataframe
        """
        module_output_path = (
            self.get_output_dir_path()
            / folder_suffix
            / f"{self.get_module_name()}.parquet"
        )
        if not module_output_path.parent.exists():
            module_output_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Writing module output to {module_output_path}")
        df.to_parquet(module_output_path, engine="fastparquet", **kwargs)

    def get_module_name(self) -> str:
        """Standard module name (= class name in snake notation)"""
        return camel_to_snake(self.__class__.__name__)

    @abstractmethod
    def run_module(self, *args, **kwargs) -> pd.DataFrame:
        """Base method to run module logic"""
        raise NotImplementedError
