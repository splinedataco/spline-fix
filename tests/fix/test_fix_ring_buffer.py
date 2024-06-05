import logging
from pprint import pprint as pp

from joshua.fix.fix_ring_buffer import FixReadonlyRingBuffer

# will test with curves because smaller


def test_open_independent_buffers(integration_test_data_dir, helpers) -> None:  # type: ignore[no-untyped-def]
    with helpers.idependent_ring_buffers(integration_test_data_dir) as buff_paths:
        curves_rb = FixReadonlyRingBuffer(filename=buff_paths["curves"])
        preds_rb = FixReadonlyRingBuffer(filename=buff_paths["predictions"])
        logging.debug(f"curves: {curves_rb}")
        logging.debug(f"preds: {preds_rb}")

        """
        for sofh, head, b in curves_rb.messages():
            logging.debug(f"curves: curves_rb: {sofh}, {head}, {b!r}")
        """
        curve_metrics = curves_rb.analyze()
        logging.debug(f"{curve_metrics}")
        pp(curve_metrics)

        preds_metrics = preds_rb.analyze()
        logging.debug(f"{preds_metrics}")
        pp(preds_metrics)
