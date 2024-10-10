from airflow.exceptions import AirflowSkipException
from omegaconf import DictConfig


def request_lpro_allocation(sm_config: DictConfig, k: int):
    raise AirflowSkipException("Not implemented for now")


def send_communication(sm_config: DictConfig, k: int):
    raise AirflowSkipException("Not implemented for now")
