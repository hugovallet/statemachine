from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

import matplotlib
import numpy as np
import pandas as pd
from graphviz import Digraph
from pydantic import BaseModel, validator
from statemachine.constants import ColumnNames as CN
from statemachine.utils.string import prettify


class LproStatuses(Enum):
    success = "SUCCESS"
    failure = "FAILURE"


class SupportedStates(Enum):
    pre_allocated = "pre_allocated"
    lpro_msg_sent = "lpro_msg_sent"
    allocated = "allocated"
    redeemed = "redeemed"
    expired = "expired"
    reminder = "reminder"
    reminder1 = "reminder1"
    reminder2 = "reminder2"
    hurdle_completed = "hurdle_completed"


class SupportedConditions(Enum):
    relative_time = "relative_time"
    lpro_ok = "lpro_ok"
    redemption = "redemption"


class SupportedActions(Enum):
    relative_time = "request_lpro_allocation"
    redemption = "send_communication"


class Condition(BaseModel):
    """Class to store the transition condition"""

    # The type of transition
    type: SupportedConditions
    # Parameters for the transition
    params: Optional[Dict]


class Action(BaseModel):
    """Class to store the action triggered by the transition"""

    # The type of action
    type: SupportedActions
    # Parameters for the action
    params: Optional[Dict]


class Transition(BaseModel):
    # The list of incoming states, if one it will be converted to list of one
    from_state: List[SupportedStates]
    # The state to which to transition
    to_state: SupportedStates
    # The condition to allow transition
    condition: Condition
    # Action to trigger when transition happens
    action: Optional[Action]

    @validator("from_state", pre=True)
    def normalise_to_list(cls, v):
        if isinstance(v, str):
            return [v]
        return v


class State(BaseModel):
    name: SupportedStates
    is_active: bool
    action: Optional[Action]


class StateMachine(BaseModel):
    id: int
    name: str
    desc: str
    granularity: List[str]
    transitions: List[Transition]
    states: List[State]

    @property
    def name_to_state_map(self) -> Dict[str, State]:
        return {s.name: s for s in self.states}

    @property
    def nodes(self) -> Set[SupportedStates]:
        """List of nodes in state machine"""
        nodes = set()
        for from_state, to_state in self.edges:
            nodes |= {from_state}
            nodes |= {to_state}

        return nodes

    @property
    def edges(self) -> Set[Tuple[SupportedStates, SupportedStates]]:
        """list of edges in state machine"""
        edges = set()
        for t in self.transitions:
            for s in t.from_state:
                edges |= {(s, t.to_state)}

        return edges

    def to_graph(
        self,
        condition_labels: bool = False,
        states_df: pd.DataFrame = None,
        excluded_transitions: Tuple[int] = (),
    ) -> Digraph:
        """Generates the graphviz repr of the State Machine, for easier debugging"""
        dot = Digraph(self.name, comment=self.desc, format="svg")
        dot.attr(rankdir="LR", splines="ortho")

        n_colors = len(self.transitions)
        thresh = np.linspace(0, 1, n_colors)
        cmap = matplotlib.colormaps["viridis"]
        colors = [matplotlib.colors.to_hex(cmap(t)) for t in thresh]

        dot.attr("node", shape="circle", fixedsize="true", width="1.5")
        dot.attr("edge", penwidth="3")

        if states_df is not None:
            count_dct = (
                states_df[states_df[CN.state_machine_id] == self.id]
                .groupby(CN.current_state)[CN.state_arrival_datetime]
                .count()
                .to_dict()
            )
        else:
            count_dct = dict()

        nodes_names = dict()
        for n in self.states:
            count = count_dct.get(n.name.name, 0)
            count_label = f"\nN={prettify(count)}"
            node_name = n.name.name + count_label
            nodes_names[n.name] = node_name
            if n.is_active:
                node_shape = "doublecircle"
            else:
                node_shape = "circle"
            dot.node(node_name, shape=node_shape)

        for i, t in enumerate(self.transitions):
            if i in excluded_transitions:
                continue
            from_states = t.from_state
            for child in from_states:
                label = f"{i}"
                if condition_labels:
                    label += f" | Cond:'{t.condition.type.name}'"
                dot.edge(
                    nodes_names[child],
                    nodes_names[t.to_state],
                    xlabel=label,
                    color=colors[i],
                    fontcolor=colors[i],
                )

        return dot
