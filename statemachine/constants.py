from pathlib import Path


class ColumnNames:
    """
    Column names used in the code.
    """

    # states
    customer_id = "customer_id"
    offer_id = "offer_id"
    state_machine_id = "state_machine_id"
    current_state = "current_state"
    state_arrival_datetime = "state_arrival_datetime"
    state_arrival_date = "state_arrival_date"
    active_state_flag = "active_state_flag"

    # transitions
    transition_rank = "transition_rank"
    transition_datetime = "transition_datetime"
    transition_flag = "transition_flag"
    from_state = "from_state"
    to_state = "to_state"
    run_date = "run_date"

    # lpro answers
    lpro_answer = "lpro_answer"
    lpro_allocation_start = "lpro_allocation_start"
    lpro_allocation_end = "lpro_allocation_end"

    # redeems
    redemption_datetime = "redemption_datetime"
    has_redeemed = "has_redeemed"


SRC_DIR = Path(__file__).parent
ROOT_DIR = SRC_DIR.parent
