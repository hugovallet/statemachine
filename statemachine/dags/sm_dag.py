# pylint: disable=pointless-statement, consider-using-enumerate
import sys


sys.path.insert(0, "/")
sys.path.insert(0, "/Users/vallethugo/Desktop/statemachine")

import logging

from airflow import DAG
from omegaconf import OmegaConf
from statemachine.constants import SRC_DIR
from statemachine.utils.dags import generate_state_machine_dag


log = logging.getLogger(__name__)

import datetime
from glob import glob


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2019, 7, 29, 7),
    "retries": 2,
    "retry_delay": datetime.timedelta(seconds=5),
}

# load all state machine configs (except the unit-test one)
confs = []
for conf_file in glob(str(SRC_DIR / "configs/*[!_unit_test].yml")):
    print(f"Loading conf: {conf_file}")
    confs.append(OmegaConf.load(conf_file))
state_machine_configs = OmegaConf.merge(*confs)

# create dynamically all required DAGs
for state_machine_config in state_machine_configs.values():
    sm_name = state_machine_config["name"]
    dag = DAG(
        dag_id=sm_name,
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        schedule_interval=None,
        render_template_as_native_obj=True,
    )
    globals()[sm_name] = generate_state_machine_dag(
        dag=dag,
        state_machine_config=state_machine_config,
    )
