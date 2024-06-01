from enum import Enum
from typing import Union
import ctypes
import logging
import datetime as dt
import time


class FixpMessageType(Enum):
    Negotiate = 1
    NegotiationResponse = 2
    NegotiationReject = 3
    Establish = 4
    EstablishmentAck = 5
    EstablishmentReject = 6
    Terminate = 7
    Sequence = 8
    UnsequencedHeartbeat = 9


class SplineMuniFixType(Enum):
    Curve = 1
    Prediction = 2


class FlowTypeEnum(Enum):
    NONE = 0
    IDEMPOTENT = 1
    UNSEQUENCED = 2
    RECOVERABLE = 3


def align_by(numbytes: int, alignment_by: int = 4) -> int:
    """Takes a number of bytes and the preferred alignment and return
    number of additional bytes needed to achive alignment.
    """
    additional_bytes = alignment_by - numbytes % alignment_by
    if additional_bytes == alignment_by:
        return 0
    else:
        return additional_bytes


def yield_float_to_fixed_decimal(
    value: float, mantissa_size: int = 16, fixed_exp: int = -3
) -> int:
    """Used to convert the yield in curves from a float with positions behind the decimal
    to a mantissa * 10^exp representation, where the fixed_exp an assumed constant.
    mantissa_size must be signed to support negative numbers. Currently only i16 is tested
    though easily extended.

    example: value=2.697, mantissa_size = i16, fixed_exp=-3
    mantissa = value * 10 * (-1*fixed_exp)
    mantissa == 2697
    if abs(mantissa) > 2**(16-1):
       raise Error
    return mantissa
    """
    num_signed_bits = mantissa_size
    # conversion from float to int truncates
    # 2.518*10000 = 25179.9999999996. If we truncate we get 251700 instead of 251800
    mantissa = round(value * 10 ** (-1 * fixed_exp))
    if abs(mantissa) > 2 ** (num_signed_bits - 1):
        raise ValueError(
            "Resulting mantissa of {} is too large to fit into {} bits.".format(
                mantissa, num_signed_bits
            )
        )
    return mantissa


def yield_fixed_decimal_to_float(
    mantissa: Union[int, ctypes.c_short], fixed_exp: int = -3
) -> float:
    """The inverse of yield_float_to_fixed_decimal, takes a signed integer
    and converts to float using the fixed_exp
    """
    if isinstance(mantissa, ctypes.c_short):
        logging.debug(
            "yield_fixed_decimal_to_float got c_short mantissa of: '{}'".format(
                mantissa
            )
        )
        mantissa = int(mantissa.value)
    return round(mantissa * (10.0**fixed_exp), -1 * fixed_exp)


def get_timestamp() -> float:
    timestamp: float
    while True:
        timestamp = dt.datetime.now().timestamp()
        timestamp_i = int(timestamp * 1_000_000_000)
        timestamp_f = timestamp_i / 1_000_000_000.0
        if timestamp == timestamp_f:
            break
        logging.debug(
            "timestamp={} != {} = reconstituted timestamp".format(
                timestamp, timestamp_f
            )
        )
        time.sleep(1)
    return timestamp
