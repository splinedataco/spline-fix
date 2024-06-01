import pytest
from joshua.fix.fixp.sequence import (
    Sequence,
    UnsequencedHeartbeat,
)
import logging


def test_sequence_serialize_deserialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_sequence_serialize_deserialize -o log_cli=true
    """
    with pytest.raises(ValueError) as excinfo:
        seq = Sequence(next_sequence_number=0)

    assert str(excinfo.value) == "Next Sequence number must be positive. Got: 0"

    for i in range(1, 100):
        seq = Sequence(next_sequence_number=i)
        logging.debug(f"Sequence(seq={seq}")
        seq_bytes = seq.serialize()
        logging.debug("serialized: {!r}".format(seq_bytes.hex(":")))
        (seq_deser, remaining_bytes) = Sequence.deserialize(data=seq_bytes)
        assert remaining_bytes == bytes()
        logging.debug("deserialized: {}".format(seq_deser))
        assert seq_deser == seq


def test_unsequenced_heartbeat_serde() -> None:
    useq_hb = UnsequencedHeartbeat()
    useq_bytes = useq_hb.serialize()
    (useq_deser, remaining_bytes) = UnsequencedHeartbeat.deserialize(data=useq_bytes)
    assert useq_hb == useq_deser
    assert remaining_bytes == bytes()
