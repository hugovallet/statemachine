from datetime import timedelta

import pandas as pd
from statemachine.constants import ColumnNames as CN
from statemachine.libs.common.module import Module
from statemachine.libs.engine.data_classes import LproStatuses, Transition


class RelativeTime(Module):
    def run_module(
        self,
        prev_state_df: pd.DataFrame,
        config: Transition,
    ) -> pd.DataFrame:
        now = pd.Timestamp.now()
        condition = (now - prev_state_df[CN.state_arrival_datetime]) / timedelta(
            **config.condition.params
        ) >= 1
        updates_df = prev_state_df.loc[
            condition,
            [CN.customer_id, CN.offer_id, CN.state_machine_id, CN.current_state],
        ]

        print(f"Found {len(updates_df)} updates")

        return updates_df


class LproOk(Module):
    def run_module(
        self,
        prev_state_df: pd.DataFrame,
        lpro_answer_df: pd.DataFrame,
        config: Transition,
    ) -> pd.DataFrame:
        # keep the most recent lpro answer only
        lpro_answer_df = (
            lpro_answer_df.sort_values(CN.lpro_allocation_start)
            .groupby([CN.customer_id, CN.offer_id])
            .tail()
        )
        prev_state_df = pd.merge(
            prev_state_df, lpro_answer_df, on=[CN.customer_id, CN.offer_id], how="left"
        )
        condition = (prev_state_df[CN.lpro_answer] == LproStatuses.success.value) & (
            prev_state_df[CN.lpro_allocation_start]
            >= prev_state_df[CN.state_arrival_datetime]
        )
        updates_df = prev_state_df.loc[
            condition,
            [CN.customer_id, CN.offer_id, CN.state_machine_id, CN.current_state],
        ]

        print(f"Found {len(updates_df)} updates")

        return updates_df


class Redeem(Module):
    def run_module(
        self,
        prev_state_df: pd.DataFrame,
        redeems_df: pd.DataFrame,
        config: Transition,
    ) -> pd.DataFrame:
        # keep the most recent redeem answer only
        redeems_df = (
            redeems_df.sort_values(CN.redemption_datetime)
            .groupby([CN.customer_id, CN.offer_id])
            .tail()
        )
        prev_state_df = pd.merge(
            prev_state_df, redeems_df, on=[CN.customer_id, CN.offer_id], how="left"
        )
        condition = prev_state_df[CN.has_redeemed] & (
            prev_state_df[CN.redemption_datetime]
            >= prev_state_df[CN.state_arrival_datetime]
        )
        updates_df = prev_state_df.loc[
            condition,
            [CN.customer_id, CN.offer_id, CN.state_machine_id, CN.current_state],
        ]

        print(f"Found {len(updates_df)} updates")

        return updates_df
