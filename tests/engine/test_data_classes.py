from constants import SRC_DIR
from libs.engine.data_classes import StateMachine, SupportedStates, Transition
from omegaconf import OmegaConf


def test_transition():
    t = Transition(
        from_state="allocated", to_state="expired", condition={"type": "relative_time"}
    )
    assert t.from_state == [SupportedStates.allocated]


def test_sm_init_from_conf():
    conf = OmegaConf.load(SRC_DIR / "configs/sm_unit_test.yml")
    conf_dct = OmegaConf.to_container(conf, resolve=True)["sm1"]
    sm = StateMachine(**conf_dct)
    assert sm.states == {
        SupportedStates.pre_allocated,
        SupportedStates.lpro_msg_sent,
        SupportedStates.allocated,
        SupportedStates.redeemed,
        SupportedStates.expired,
    }
    assert sm.edges == {
        (SupportedStates.pre_allocated, SupportedStates.lpro_msg_sent),
        (SupportedStates.lpro_msg_sent, SupportedStates.allocated),
        (SupportedStates.allocated, SupportedStates.redeemed),
        (SupportedStates.pre_allocated, SupportedStates.expired),
        (SupportedStates.lpro_msg_sent, SupportedStates.expired),
        (SupportedStates.allocated, SupportedStates.expired),
        (SupportedStates.redeemed, SupportedStates.expired),
    }
