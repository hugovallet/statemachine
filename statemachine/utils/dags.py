from typing import Callable

import statemachine.tasks.actions as actions_module
import statemachine.tasks.transitions as transitions_module
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from omegaconf import DictConfig
from statemachine.tasks.other import get_data, update_state


def _get_transition_callable(type: str) -> Callable:
    try:
        return getattr(transitions_module, type)
    except AttributeError:
        raise Exception(
            f"Cannot find transition type '{type}' in state machine transition bank"
        )
    except Exception as e:
        raise e


def _get_action_callable(type: str) -> Callable:
    try:
        return getattr(actions_module, type)
    except AttributeError:
        raise Exception(
            f"Cannot find action type '{type}' in state machine action bank"
        )
    except Exception as e:
        raise e


def generate_state_machine_dag(
    dag: DAG,
    state_machine_config: DictConfig,
):
    with dag:
        data_getter = PythonOperator(
            task_id="data_getter",
            python_callable=get_data,
        )

        prev_task_group = data_getter

        for k, transition_conf in enumerate(state_machine_config.transitions):
            from_state = transition_conf["from_state"]
            to_state = transition_conf["to_state"]
            condition = transition_conf["condition"]
            action = transition_conf.get("action", None)

            with TaskGroup(
                f"Transition-{k}", tooltip=f"FROM {from_state}\nTO '{to_state}'"
            ) as task_group:
                evaluate_transition_task = PythonOperator(
                    task_id="transition",
                    python_callable=_get_transition_callable(type=condition["type"]),
                    op_kwargs={"sm_config": state_machine_config, "k": k},
                    trigger_rule="none_failed",
                )
                update_state_task = PythonOperator(
                    task_id="update_state",
                    python_callable=update_state,
                    op_kwargs={"sm_config": state_machine_config, "k": k},
                )
                if action:
                    action_task = PythonOperator(
                        task_id="action",
                        python_callable=_get_action_callable(type=action["type"]),
                        op_kwargs={"sm_config": state_machine_config, "k": k},
                    )
                    (evaluate_transition_task >> update_state_task >> action_task)

                else:
                    (evaluate_transition_task >> update_state_task)

            prev_task_group >> task_group
            prev_task_group = task_group

    return dag
