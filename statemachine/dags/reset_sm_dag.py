# pylint: disable=pointless-statement, consider-using-enumerate
import sys


sys.path.insert(0, "/")
sys.path.insert(0, "/Users/vallethugo/Desktop/statemachine")

import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from statemachine.tasks.other import reset_state_db


log = logging.getLogger(__name__)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2019, 7, 29, 7),
    "retries": 2,
    "retry_delay": datetime.timedelta(seconds=5),
}

with DAG(
    "reset_state_sb", default_args=default_args, catchup=False, schedule_interval=None
) as dag:
    reset = PythonOperator(task_id="reset_state_db", python_callable=reset_state_db)
