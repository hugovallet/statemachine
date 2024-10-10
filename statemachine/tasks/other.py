from itertools import product

import numpy as np
import pandas as pd
from airflow.exceptions import AirflowSkipException
from omegaconf import DictConfig, OmegaConf
from statemachine.constants import ROOT_DIR
from statemachine.constants import ColumnNames as CN
from statemachine.data.getters import _get_states_df, _get_transitions_df
from statemachine.libs.engine.data_classes import StateMachine
from statemachine.libs.engine.data_classes import SupportedConditions as SC
from statemachine.libs.engine.data_classes import SupportedStates, Transition
from statemachine.libs.engine.transitions import LproOk, Redeem, RelativeTime
from statemachine.utils.pandas import left_anti


N_CUSTOMERS = 100000
N_OFFERS = 1000
CUSTOMER_IDS = [f"C{i}" for i in range(N_CUSTOMERS)]
OFFER_IDS = [f"O{i}" for i in range(N_OFFERS)]
COMBOS = list(product(CUSTOMER_IDS, OFFER_IDS))


TRANSITION_MODULES = {
    SC.relative_time.name: RelativeTime(),
    SC.lpro_ok.name: LproOk(),
    SC.redemption.name: Redeem(),
}


def get_data():
    reset_state_db()
    # pass


def reset_state_db():
    """Generates a toy set of (customer, offer) allocations and reset the state machine db"""
    # generate toy data (everybody reset to pre_allocated state)
    print("Resetting states db")
    state_df = pd.DataFrame(COMBOS, columns=[CN.customer_id, CN.offer_id])
    state_df[CN.state_machine_id] = 0
    state_df[CN.current_state] = SupportedStates.pre_allocated.name
    state_df[CN.state_arrival_datetime] = pd.Timestamp.now()
    state_df[CN.active_state_flag] = False
    # save data
    state_df.to_parquet(
        ROOT_DIR / "data" / "outputs" / "states_db.parquet", engine="fastparquet"
    )
    print(f"Done, state db shape={len(state_df)}")

    # generate the corresponding transition data
    print("Resetting transitions db")
    transition_df = state_df[[CN.customer_id, CN.offer_id, CN.state_machine_id]]
    transition_df[CN.from_state] = None
    transition_df[CN.to_state] = state_df[CN.current_state]
    transition_df[CN.transition_datetime] = state_df[CN.state_arrival_datetime]
    transition_df[CN.transition_rank] = -1
    # save data
    transition_df.to_parquet(
        ROOT_DIR / "data" / "outputs" / "transitions_db.parquet", engine="fastparquet"
    )
    print(f"Done, transitions db shape={len(transition_df)}")


def simulate_lpro_answer():
    """Randomly accept to allocate certain % of customers"""
    lpro_answer_df = pd.DataFrame(COMBOS, columns=[CN.customer_id, CN.offer_id])
    # generate random outcomes
    percent_success = 0.9
    lpro_answer_df[CN.lpro_answer] = np.random.choice(
        ["SUCCESS", "FAILURE"],
        size=len(COMBOS),
        p=(percent_success, 1 - percent_success),
    ).tolist()
    now = pd.Timestamp.now()
    lpro_answer_df[CN.lpro_allocation_start] = np.where(
        lpro_answer_df[CN.lpro_answer] == "SUCCESS", now, None
    )
    lpro_answer_df[CN.lpro_allocation_end] = np.where(
        lpro_answer_df[CN.lpro_answer] == "SUCCESS", now + pd.Timedelta(days=7), None
    )
    # save data
    lpro_answer_df.to_parquet(
        ROOT_DIR / "data" / "inputs" / "lpro_answers_db.parquet", engine="fastparquet"
    )


def simulate_redeems():
    """Randomly accept to redeem certain % of customers"""
    redeems_df = pd.DataFrame(COMBOS, columns=[CN.customer_id, CN.offer_id])
    # generate random outcomes
    percent_success = 0.2
    redeems_df[CN.has_redeemed] = np.random.choice(
        [True, False],
        size=len(COMBOS),
        p=(percent_success, 1 - percent_success),
    ).tolist()
    redeems_df[CN.redemption_datetime] = np.where(
        redeems_df[CN.has_redeemed], pd.Timestamp.now(), None
    )
    # save data
    redeems_df.to_parquet(
        ROOT_DIR / "data" / "inputs" / "redeems_db.parquet", engine="fastparquet"
    )


def _update_transitions_db(transitions_df, update_df, config: Transition, k: int):
    print("Updating transitions...")
    updates_df = update_df.rename(columns={CN.current_state: CN.from_state})
    updates_df[CN.to_state] = config.to_state.name
    updates_df[CN.transition_datetime] = pd.Timestamp.now()
    updates_df[CN.transition_rank] = k
    # new_transition_df = pd.concat([transitions_df, updates_df])
    updates_df.to_parquet(
        ROOT_DIR / "data" / "outputs" / "transitions_db.parquet",
        engine="fastparquet",
        append=True,
    )
    # print(f"Done, shape={new_transition_df.shape}")
    print(f"Done. Appended {len(updates_df)} rows")


def _update_states_db(
    states_df, update_df, sm_config: StateMachine, config: Transition
):
    print("Updating states...")
    updates_states_df = update_df.copy()
    updates_states_df[CN.current_state] = config.to_state.name
    updates_states_df[CN.state_arrival_datetime] = pd.Timestamp.now()
    state_dct = {s.name: s.is_active for s in sm_config.states}
    updates_states_df[CN.active_state_flag] = updates_states_df[CN.current_state].map(
        state_dct
    )
    keys = [CN.customer_id, CN.offer_id, CN.state_machine_id]
    unchanged_states_df = left_anti(states_df, updates_states_df, on=keys)
    new_state_df = pd.concat([unchanged_states_df, updates_states_df])
    new_state_df.to_parquet(
        ROOT_DIR / "data" / "outputs" / "states_db.parquet", engine="fastparquet"
    )
    print(f"Done, shape={new_state_df.shape}")


def update_state(sm_config: DictConfig, k: int):
    """Updates the states and transitions dbs, if necessary"""
    # 0 read transition config
    sm_config = StateMachine(**OmegaConf.to_container(sm_config, resolve=True))
    config = sm_config.transitions[k]

    # 1 read updates
    prev_module = TRANSITION_MODULES[config.condition.type.name]
    updates_df = prev_module.read_module_output(folder_suffix=f"{sm_config.id}/{k}")

    # 2 if updates found, updates state/transition
    if len(updates_df) > 0:
        states_df = _get_states_df()
        transitions_df = _get_transitions_df()
        _update_states_db(
            states_df=states_df,
            update_df=updates_df,
            sm_config=sm_config,
            config=config,
        )
        _update_transitions_db(
            transitions_df=transitions_df, update_df=updates_df, config=config, k=k
        )
    else:
        raise AirflowSkipException("No updates found, skipping db updates")
