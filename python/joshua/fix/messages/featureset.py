try:
    from typing import Self  # type: ignore[reportAttributeAccessIssue, unused-ignore]
except ImportError:
    from typing_extensions import Self
from typing import Any
import struct
import ctypes
import logging
import regex
from joshua.fix.messages import (
    MuniLiquidity,
    MuniPayment,
    MuniSize,
    SplineFixType,
)


class FeatureSet(SplineFixType):
    _rating: str
    payment: MuniPayment
    _coupon: ctypes.c_uint8
    muni_size: MuniSize
    liquidity: MuniLiquidity
    NUM_BYTES: int = 8

    def __init__(
        self,
        rating: str,
        payment: MuniPayment,
        coupon: int,
        muni_size: MuniSize,
        liquidity: MuniLiquidity,
    ):
        self.rating = rating
        self.payment = payment
        self.coupon = coupon
        self.muni_size = muni_size
        self.liquidity = liquidity

    def serialize(self) -> bytes:
        """Turn ourselves into a little_endian array of bytes"""
        # NOTE: Should turn this into an enum and then need only 2.
        # basically, 1-21 mapping to the spline ratings we have internally
        # we have BBB+, BBB-, so we need 4
        ser_ret = struct.pack("<4s", self.rating.encode())
        ser_ret = ser_ret + struct.pack("<B", self.payment.value)
        ser_ret = ser_ret + struct.pack("<B", self._coupon.value)
        ser_ret = ser_ret + struct.pack("<B", self.muni_size.value)
        ser_ret = ser_ret + struct.pack("<B", self.liquidity.value)
        return ser_ret

    @property
    def rating(self) -> str:
        return self._rating

    @rating.setter
    def rating(self, rat: str) -> Self:
        rat_len = len(rat)
        if rat_len == 0 or rat_len > 4:
            raise ValueError("Rating must be between 1 and 4 characters.")
        self._rating = rat
        return self

    @property
    def coupon(self) -> int:
        return self._coupon.value

    @coupon.setter
    def coupon(self, c: int) -> Self:
        if not isinstance(c, int) or c < 0 or c > 2**8:
            logging.debug("coupon passed type={}".format(type(c)))
            logging.debug("coupon passed value={}".format(c))
            raise ValueError("Coupon must be positive and less than {}".format(2**8))
        self._coupon = ctypes.c_uint8(c)
        return self

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Self, bytes]:
        """group_type and align_to are unused"""
        logging.debug(
            "deserialize bytes in len: {} bytes: '{}'".format(len(data), data.hex())
        )
        rating = struct.unpack("<4s", data[0:4])[0]
        # rating may have padding, but if it doesn't have a null
        first_null = rating.find(b"\x00")
        if first_null < 0 or first_null > 4:
            logging.debug("first null returned was {} seting to 4.".format(first_null))
            first_null = 4
        if first_null == 0:
            raise ValueError("Tried to decode 0 length rating string.")

        rating = rating[0:first_null].decode("latin1")
        payment = struct.unpack("<B", data[4:5])[0]
        coupon = struct.unpack("<B", data[5:6])[0]
        muni_size = struct.unpack("<B", data[6:7])[0]
        liquidity = struct.unpack("<B", data[7:8])[0]
        return (
            cls(
                str(rating),
                MuniPayment(payment),
                coupon,
                MuniSize(muni_size),
                MuniLiquidity(liquidity),
            ),
            data[8:],
        )

    def __str__(self) -> str:
        strvals = [self.rating.lower()]
        strvals.append(self.payment.name.lower())
        strvals.append(str(self.coupon))
        strvals.append(self.muni_size.name.lower())
        strvals.append(self.liquidity.name.lower())
        return "_".join(strvals)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FeatureSet):
            return NotImplemented
        # make a tuple of values and then compare them
        stup = (self.rating, self.payment, self.coupon, self.muni_size, self.liquidity)
        otup = (
            other.rating,
            other.payment,
            other.coupon,
            other.muni_size,
            other.liquidity,
        )
        return stup == otup

    def __repr__(self) -> str:
        return str(self)

    def length_in_bytes(self) -> int:
        return self.struct_size_bytes()

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls.NUM_BYTES

    @property
    def block_length(self) -> int:
        return self.struct_size_bytes()

    @classmethod
    def from_str(cls, fs_str: str) -> Self:
        fs_re = regex.compile(
            r"^.*?([abc]{1,3})_(go|rev)_([[:digit:]]{1,2})_(lrg|med|sml)_(lqd|ilqd).*$"
        )

        group = fs_re.match(fs_str.lower())
        if group is None:
            raise ValueError(
                "Expecting rrr_pp_c_sss_lll format. Got: {}".format(fs_str)
            )
        groups = group.groups()
        logging.debug("groups: {} str={}".format(groups, fs_str))

        fs = cls(
            groups[0].lower(),
            MuniPayment[groups[1].lower()],
            int(groups[2]),
            MuniSize[groups[3].lower()],
            MuniLiquidity[groups[4].lower()],
        )
        logging.debug("FeatureSet.from_str() returning {}".format(fs))
        return fs
