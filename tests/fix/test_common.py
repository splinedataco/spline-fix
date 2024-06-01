import pytest
from joshua.fix.common import (
    yield_float_to_fixed_decimal,
    yield_fixed_decimal_to_float,
)


def test_yield_float_to_decimal_conversions_success() -> None:
    # exp is default of -3
    test_inputs = [
        2.631,
        2.518,
        2.403,
        2.286,
        2.265,
        3.058,
        3.095,
        3.132,
        3.17,
        3.207,
        -3.207,
    ]
    test_answers = [
        2631,
        2518,
        2403,
        2286,
        2265,
        3058,
        3095,
        3132,
        3170,
        3207,
        -3207,
    ]

    for f, m in zip(test_inputs, test_answers):
        mantissa = yield_float_to_fixed_decimal(value=f)
        assert mantissa == m
        as_float = yield_fixed_decimal_to_float(mantissa=mantissa)
        # probably should make this a >= f+sigma <=
        # but is passing tests
        assert as_float == f

    # exp is -5
    test_inputs.append(3.20712)
    test_answers = [x * 100 for x in test_answers]
    test_answers.append(320712)
    for f, m in zip(test_inputs, test_answers):
        mantissa = yield_float_to_fixed_decimal(value=f, mantissa_size=32, fixed_exp=-5)
        assert mantissa == m
        as_float = yield_fixed_decimal_to_float(mantissa=mantissa, fixed_exp=-5)
        # probably should make this a >= f+sigma <=
        # but is passing tests
        assert as_float == f


def test_yield_float_to_decimal_conversions_fail() -> None:
    # the one with many trailing digits will fill when converting back
    # as the first conversion is lossy
    test_inputs = [33.631, 40.123, 150.234]
    test_answers = [33631, 40123, 150234]
    for f, m in zip(test_inputs, test_answers):
        with pytest.raises(ValueError):
            mantissa = yield_float_to_fixed_decimal(value=f)
            assert mantissa == m
            as_float = yield_fixed_decimal_to_float(mantissa=mantissa)
            # shouldn't get here
            assert as_float != f

    # we are only doing 10^-3 so when we convert back we
    # will have lost that precision
    orig_val = -10.246789
    mantissa = yield_float_to_fixed_decimal(value=orig_val)
    test_ans = yield_fixed_decimal_to_float(mantissa=mantissa)
    assert test_ans != orig_val
