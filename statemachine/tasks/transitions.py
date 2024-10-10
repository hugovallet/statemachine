import pandas as pd
import statemachine.libs.engine.transitions as transitions_modules
from airflow.exceptions import AirflowSkipException
from omegaconf import DictConfig, OmegaConf
from statemachine.data.getters import (
    _filter_to_parent_states,
    _get_lpro_answers_df,
    _get_redeems_df,
    _get_states_df,
)
from statemachine.libs.engine.data_classes import StateMachine


def _skip_if_prev_state_empty(
    prev_state_df: pd.DataFrame, sm_config: StateMachine, k: int
):
    if len(prev_state_df) == 0:
        raise AirflowSkipException(
            f"No {sm_config.granularity} in parent states of transition '{k}'. Skipping"
        )
    else:
        print(
            f"Found {len(prev_state_df)} couples in upstream states, executing transitions"
        )


def relative_time(sm_config: DictConfig, k: int):
    # 0 read transition config
    sm_config = StateMachine(**OmegaConf.to_container(sm_config, resolve=True))
    config = sm_config.transitions[k]
    print(
        f"Executing transition '{config.condition.type.name}' for step {k} with params {config.condition.params}"
    )

    # 1 read prev state data
    states_df = _get_states_df()
    prev_state_df = _filter_to_parent_states(
        states_df=states_df, sm_config=sm_config, transition_config=config
    )
    _skip_if_prev_state_empty(prev_state_df=prev_state_df, sm_config=sm_config, k=k)

    # 2 find (customer, offer) that meet the transition condition
    module = transitions_modules.RelativeTime()
    updates_df = module.run_module(prev_state_df=prev_state_df, config=config)

    # 3 save updates
    module.save_module_output(updates_df, folder_suffix=f"{sm_config.id}/{k}")


def lpro_ok(sm_config: DictConfig, k: int):
    # 0 read transition config
    sm_config = StateMachine(**OmegaConf.to_container(sm_config, resolve=True))
    config = sm_config.transitions[k]
    print(
        f"Executing transition '{config.condition.type.name}' for step {k} with params {config.condition.params}"
    )

    # 1 read prev state data
    states_df = _get_states_df()
    prev_state_df = _filter_to_parent_states(
        states_df=states_df, sm_config=sm_config, transition_config=config
    )
    _skip_if_prev_state_empty(prev_state_df=prev_state_df, sm_config=sm_config, k=k)
    lpro_answer_df = _get_lpro_answers_df()

    # 2 find (customer, offer) that meet the transition condition
    module = transitions_modules.LproOk()
    updates_df = module.run_module(
        prev_state_df=prev_state_df, lpro_answer_df=lpro_answer_df, config=config
    )

    # 3 save updates
    module.save_module_output(updates_df, folder_suffix=f"{sm_config.id}/{k}")


def redemption(sm_config: DictConfig, k: int):
    # 0 read transition config
    sm_config = StateMachine(**OmegaConf.to_container(sm_config, resolve=True))
    config = sm_config.transitions[k]
    print(
        f"Executing transition '{config.condition.type.name}' for step {k} with params {config.condition.params}"
    )

    # 1 read prev state data
    states_df = _get_states_df()
    prev_state_df = _filter_to_parent_states(
        states_df=states_df, sm_config=sm_config, transition_config=config
    )
    _skip_if_prev_state_empty(prev_state_df=prev_state_df, sm_config=sm_config, k=k)
    redeems_df = _get_redeems_df()

    # 2 find (customer, offer) that meet the transition condition
    module = transitions_modules.Redeem()
    updates_df = module.run_module(
        prev_state_df=prev_state_df, redeems_df=redeems_df, config=config
    )

    # 3 save updates
    module.save_module_output(updates_df, folder_suffix=f"{sm_config.id}/{k}")
