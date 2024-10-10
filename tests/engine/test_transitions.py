from unittest.mock import MagicMock

import data.getters
import numpy as np
import pandas as pd
import pytest
import tasks.other
import tasks.transitions as transitions
from libs.engine.data_classes import SupportedStates as SS
from mockito import unstub
from omegaconf import OmegaConf
from statemachine.constants import ColumnNames as CN


@pytest.fixture()
def mock_sm_config_dct():
    sm_dct = {
        "id": 1,
        "name": "test_state_machine_1",
        "desc": "a state machine for unit-tests",
        "granularity": [CN.customer_id, CN.offer_id],
        "states": [
            {"name": SS.pre_allocated.name, "is_active": False},
            {"name": SS.allocated.name, "is_active": True},
        ],
        "transitions": [
            {
                "from_state": SS.pre_allocated.name,
                "to_state": SS.allocated,
                "condition": {"type": "relative_time", "params": {"seconds": 1}},
            }
        ],
    }
    return OmegaConf.create(sm_dct)


@pytest.fixture
def mock_states_df():
    return pd.DataFrame(
        {
            CN.customer_id: ["C1"] * 3,
            CN.offer_id: ["O1", "O2", "O3"],
            CN.state_machine_id: [1] * 3,
            CN.current_state: [SS.pre_allocated.name] * 3,
            CN.state_arrival_datetime: [pd.Timestamp("2022-01-01")] * 3,
            CN.active_state_flag: [False] * 3,
        }
    )


@pytest.fixture
def mock_transitions_df():
    return pd.DataFrame(
        {
            CN.customer_id: ["C1"] * 3,
            CN.offer_id: ["O1", "O2", "O3"],
            CN.state_machine_id: [1] * 3,
            CN.from_state: [np.nan] * 3,
            CN.to_state: [SS.pre_allocated.name] * 3,
            CN.transition_datetime: [pd.Timestamp("2022-01-01")] * 3,
            CN.transition_rank: [-1] * 3,
        }
    )


@pytest.fixture
def stub_get_states(
    mock_states_df,
):
    data.getters._get_states_df(...).thenReturn(mock_states_df)

    yield

    unstub()


@pytest.fixture
def stub_get_transitions(
    mock_transitions_df,
):
    data.getters._get_transitions_df(...).thenReturn(mock_transitions_df)

    yield

    unstub()


@pytest.fixture
def stub_updaters():
    tasks.other._update_transitions_db(...).thenReturn(MagicMock())
    tasks.other._update_states_db(...).thenReturn(MagicMock())

    yield

    unstub()


def test_relative_time(
    mock_sm_config_dct, stub_get_states, stub_get_transitions, stub_updaters
):
    transitions.relative_time(sm_config=mock_sm_config_dct, k=0)
