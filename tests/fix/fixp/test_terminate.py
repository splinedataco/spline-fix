from joshua.fix.fixp.messages import (
    Uuid,
)
from joshua.fix.fixp.terminate import (
    Terminate,
    TerminationCodeEnum,
    TERMINATE_REASON_STRINGS,
)
import logging


def test_terminate_serialize_deserialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_terminate_serialize_deserialize -o log_cli=true
    """
    for tc in range(0, 4):
        u = Uuid()
        term = Terminate(
            u,
            termination_code=TerminationCodeEnum(tc),
            termination_str=TERMINATE_REASON_STRINGS[tc],
        )
        logging.debug(
            f"Terminate(uuid={u}, termination_code={TerminationCodeEnum(tc)}, termination_str={TERMINATE_REASON_STRINGS[tc]})"
        )
        logging.debug(f"from __str__: {term}")
        term_bytes = term.serialize()
        logging.debug("serialized: {!r}".format(term_bytes.hex(":")))
        (term_deser, remaining_bytes) = Terminate.deserialize(data=term_bytes)
        assert remaining_bytes == bytes()
        logging.debug("deserialized: {}".format(term_deser))
        assert term_deser == term
