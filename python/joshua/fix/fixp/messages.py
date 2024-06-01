try:
    from typing import Self  # type: ignore[reportAttributeAccessIssue, unused-ignore]
except ImportError:
    from typing_extensions import Self
from typing import Any, cast, Dict, List, Optional, Union
from io import BytesIO, StringIO
import struct
import logging
import datetime as dt
import regex
import jwt
import os
import secrets
import base64
from joshua.fix.types import (
    SplineByteData,
    SplineDateTime,
    SplineNanoTime,
)
from joshua.fix.messages import SplineFixType
from joshua.fix.data_dictionary import (
    SPLINE_MUNI_DATA_SCHEMA_VERSION,
    SPLINE_FIXP_SESSION_SCHEMA_ID,
)
from joshua.fix.common import (
    align_by,
    FixpMessageType,
    FlowTypeEnum,
)

from enum import Enum
import uuid


"""template_id == FixpMessageType
   schema_id = 0, spline_fixp
   version = 0
"""


class Jwt(SplineFixType):
    # NOTE: this causes the cls.__secret_key to be set to this value.
    # In this way, the test_client may generate a token and the server
    # may decode the token without having a proper environment set.
    __secret_key = base64.urlsafe_b64decode(
        os.getenv("SPLINE_SECRET_KEY", default=secrets.token_urlsafe(32) + "==")
    )

    """For decoding, takes a series of bytes from a data field.
    token = bytes(from_data_field)
    header = jwt.get_unverified_header(token)
    assert header["typ"] == "JWT"
    assert header["alg"] == "HS256"

    # need the password, get it from env or config, will be base64 encoded
    secret_key = base64.urlsafe_b64decode(secret_b64)
    Then decode the body.
    try:
        output = jwt.decode(token, secret_key, algorithms=["HS256"])
    except as ex:
        various depending on the failure.


    generate test token:
    secrets.token_urlsafe(32)

    This generates 32 bytes of random characters and base64 encodes
    them. Each byte encoded is about 1.3 bytes in length.

    """
    prediction_perms = [
        "AllYieldFeatures",
        "RealTimeData",
        "AclDataApi",
        "PricePredictions",
    ]
    curve_perms = ["TodaysCurves", "RealTimeData", "AclDataApi"]
    realtime_perms = ["RealTimeData"]
    data_api_perms = ["AclDataApi"]
    _curves_permitted: bool
    _predictions_permitted: bool
    _realtime_data_permitted: bool
    _payload: Dict[str, Any]

    @property
    def curves_permitted(self) -> bool:
        logging.debug(
            f"curves_permitted: curves_permitted={self._curves_permitted} and data_api_permitted={self._data_api_permitted}"
        )
        return self._curves_permitted and self._data_api_permitted

    @curves_permitted.setter
    def curves_permitted(self, permitted: bool) -> None:
        self._curves_permitted = permitted

    @property
    def predictions_permitted(self) -> bool:
        logging.debug(
            f"predictions_permitted: predictions_permitted={self._predictions_permitted} and data_api_permitted={self._data_api_permitted}"
        )

        return self._predictions_permitted and self._data_api_permitted

    @predictions_permitted.setter
    def predictions_permitted(self, permitted: bool) -> None:
        self._predictions_permitted = permitted

    @property
    def realtime_data_permitted(self) -> bool:
        return self._realtime_data_permitted

    @realtime_data_permitted.setter
    def realtime_data_permitted(self, permitted: bool) -> None:
        self._realtime_data_permitted = permitted

    @property
    def data_api_permitted(self) -> bool:
        return self._data_api_permitted

    @data_api_permitted.setter
    def data_api_permitted(self, permitted: bool) -> None:
        self._data_api_permitted = permitted

    def __init__(self, token: Optional[bytes] = None):
        # this would be used when deserializing,
        # since we're not issuing tokens.
        # however, for testing it would be useful
        # so we'll add a method generate() for that
        # NOTE: This object is intended specifically for
        # it's task, not as a generic JWT wrapper.
        if token is None:
            # maybe we need a blank for the prototype pattern
            # in the generate function
            self._payload = {}
            self._userid = None
            self._username = None
            self._predictions_permitted = False
            self._curves_permitted = False
            self._realtime_data_permitted = False
            self._data_api_permitted = False
            return
        # verify this is a jwt and get the enc algorithm.
        header = jwt.get_unverified_header(token)
        if header["typ"] != "JWT":
            raise ValueError("Provided token is not a JWT")
        if header["alg"] != "HS256":
            raise ValueError("Only algorithm HS256 is supported")
        # now we need to validate the token
        output: Dict[str, Any]
        try:
            output = jwt.decode(
                jwt=token,
                key=self.__secret_key,
                issuer="Lambda Auth API",
                algorithms=["HS256"],
            )
        # TODO: When we change the error signature we lose what happened, though
        #       we've changed the message. Move the errors into a common error table
        #       and just log here. Let the error propigate upward so that the NegotiationReject
        #       can use the information to send back a more informational reject_code.
        except jwt.ExpiredSignatureError as ex:
            raise ValueError("Signature has expired on token. Exception: {}".format(ex))
        except jwt.InvalidIssuerError as ex:
            raise ValueError("Invalid issuer. Exception: {}".format(ex))
        except Exception as ex:
            raise ValueError("Invalid token. Exception: {}".format(ex))

        # we should have the info we need now
        logging.debug("after decoding jwt successfully: {}".format(output))
        self._payload = output
        capabilities = output["capabilities"]
        self._userid = output["sub"]
        self._username = output["data"]["name"]
        predictions_allowed = True
        for req_cap in self.prediction_perms:
            predictions_allowed &= req_cap in capabilities
        self._predictions_permitted = predictions_allowed

        curves_allowed = True
        for req_cap in self.curve_perms:
            curves_allowed &= req_cap in capabilities
        self._curves_permitted = curves_allowed

        realtime_allowed = True
        for req_cap in self.realtime_perms:
            realtime_allowed &= req_cap in capabilities
        self._realtime_data_permitted = realtime_allowed

        data_api_allowed = True
        for req_cap in self.data_api_perms:
            data_api_allowed &= req_cap in capabilities
        self._data_api_permitted = data_api_allowed

    def __str__(self) -> str:
        buf = StringIO()
        buf.write("JWT:\n")
        buf.write("    contents: {}\n".format(self._payload))
        buf.write("    predictions permitted: {}\n".format(self._predictions_permitted))
        buf.write("    curves permitted: {}\n".format(self._curves_permitted))
        if os.getenv("SPLINE_SECRET_KEY") is None:
            buf.write(
                "    random_secret_key: {}\n".format(
                    base64.urlsafe_b64encode(self.__secret_key).decode("latin1")
                )
            )
        return buf.getvalue()

    @classmethod
    def generate(
        cls,
        roles: List[str],
        capabilities: List[str],
        issuer: str,
        userid: str,
        username: str,
        secret_key_b64: Optional[str] = None,
        algo: Optional[str] = "HS256",
        # just for testing
        minutes_in_future: Optional[int] = 5,
    ) -> Self:

        key: bytes
        if secret_key_b64 is None:
            key = cls.__secret_key
        else:
            # this will fail if the length isn't correct,
            # since python will remove padding to make the length
            # correct, we can just add extra padding to make sure
            # it's long enough
            base64.urlsafe_b64decode(secret_key_b64 + "==")

        timestamp_seconds = int(dt.datetime.now().timestamp())
        payload = {
            "roles": roles,
            "capabilities": capabilities,
            "iss": issuer,
            "sub": userid,
            "data": {"name": username},
            "iat": timestamp_seconds,
            "exp": timestamp_seconds + 60 * cast(int, minutes_in_future),
        }
        encoded = jwt.encode(payload, key, algorithm=algo)
        logging.debug("generate: {!r}".format(encoded))
        # encoded_b64 = base64.urlsafe_b64encode(encoded.encode("utf-8"))
        # logging.debug("generate_b64: {!r}".format(encoded_b64.hex(":")))

        return cls(token=encoded.encode("utf-8"))

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, type(self)):
            raise NotImplementedError
        return (
            self.__secret_key,
            self._curves_permitted,
            self._predictions_permitted,
            self._payload,
        ) == (
            value.__secret_key,
            value._curves_permitted,
            value._predictions_permitted,
            value._payload,
        )

    def __len__(self) -> int:
        # 32 characters, but each is 1 hex, which is 1/2 an octet
        return 16

    def serialize(self) -> bytes:
        ser_ret = BytesIO()
        encoded = jwt.encode(self._payload, self.__secret_key, algorithm="HS256")
        logging.debug("generate: {}".format(encoded))
        # why do we do it this way? Because when a jwt is obtained from
        # the Spline api/login it will be in this form.
        # By having all of them arrive this way we have uniformity and
        # clients don't have to figure out how to decode.
        # encoded_b64 = base64.urlsafe_b64encode(encoded.encode("utf-8"))
        # encoded_b64 = encoded
        # logging.debug("generate_b64: {!r}".format(encoded_b64.hex(":")))

        # we need to put this into a bytes group to handle the variable
        # length
        byte_data = SplineByteData(data=encoded.encode("latin1"))
        ser_ret.write(byte_data.serialize())
        logging.debug("Negotiate.serialize: {!r}".format(ser_ret.getvalue().hex(":")))
        return ser_ret.getvalue()

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Self, bytes]:
        """Neither group_type nor align_to are used"""
        """Assuming we're at the start of where we should be
        in a bytestream, deserialize ourselves using ByteData.
        """
        (val, remaining_bytes) = SplineByteData.deserialize(data=data, group_type=bytes)
        logging.debug("deserialized from SplineByteData:\n{!r}".format(val))

        val = val.decode("latin1", errors="ignore")
        logging.debug("after byte decode utf-8: type(): {}\n{}".format(type(val), val))

        return (cls(token=cast(bytes, val)), remaining_bytes)

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized"""
        # 32 characters, but each is 1 hex, which is 1/2 an octet
        return self.struct_size_bytes()

    @classmethod
    def struct_size_bytes(cls) -> int:
        return 16

    @property
    def block_length(self) -> int:
        return self.struct_size_bytes()


class ClientJwt(Jwt):
    """This will take a jwt and just store it, assuming that the secret
    key is not available.
    """

    _jwt_encoded: bytes

    def __init__(self, token: bytes):
        """Intended use is for when there is a TOKEN in the environment (on a client)
        and the SPLINE_SECRET_KEY is unavailable to decode the message.
        """
        if token is None:
            self._jwt_encoded = bytes()
            return
        else:
            self._jwt_encoded = token
            logging.debug(f"clientjwt set _jwt_encoded = {token!r}")
        # verify this is a jwt and get the enc algorithm.
        header = jwt.get_unverified_header(token)
        if header["typ"] != "JWT":
            raise ValueError("Provided token is not a JWT")
        if header["alg"] != "HS256":
            raise ValueError("Only algorithm HS256 is supported")
        logging.debug(f"got a token: {token!r} unverified header is: {header}")

        self._predictions_permitted = False

        self._curves_permitted = False

        self._realtime_data_permitted = False

        self._data_api_permitted = False

    def __str__(self) -> str:
        buf = StringIO()
        buf.write(f"ClientJWT.token: {self._jwt_encoded!r}\n")
        return buf.getvalue()

    def serialize(self) -> bytes:
        byte_data = SplineByteData(data=self._jwt_encoded, align_to=1)
        ser_ret = BytesIO()
        ser_ret.write(byte_data.serialize())
        logging.debug("Negotiate.serialize: {!r}".format(ser_ret.getvalue().hex(":")))
        return ser_ret.getvalue()


class Uuid(SplineFixType):
    """UUID version 4
    32 hexidecimal digits in a patern of:
    8-4-4-4-12
    where the version is indicated by a leading 4 in the fourth group.
    x's are to be replaced with a random number from 0 to 15, represented
    in hexidecimal.
    xxxxxxxx-xxxx-xxxx-4xxx-xxxxxxxxxxxx
    """

    _uuid: uuid.UUID

    _valid_char_re_len4 = regex.compile(r"[[:digit:]abcdefABCDEF]{4,4}")
    _valid_char_re_len8 = regex.compile(r"[[:digit:]abcdefABCDEF]{8,8}")
    _valid_char_re_len12 = regex.compile(r"[[:digit:]abcdefABCDEF]{12,12}")
    _uuid_part_lengths: List[int] = [8, 4, 4, 4, 12]

    def __init__(self, reuse: Optional[Union[str, uuid.UUID]] = None) -> None:
        if reuse is not None:
            # will throw if not enough parts
            p0, p1, p2, p3, p4 = str(reuse).split("-")
            if p2[0] != "4":
                raise ValueError("Uuid provided for reuse must be version 4.")
            m = self._valid_char_re_len8.match(p0)
            if m is None:
                raise ValueError("Uuid may only contain hexidecimal values. 0-9A-F")
            for part4 in [p1, p2, p3]:
                m = self._valid_char_re_len4.match(part4)
                if m is None:
                    raise ValueError("Uuid may only contain hexidecimal values. 0-9A-F")
            m = self._valid_char_re_len12.match(p4)
            if m is None:
                raise ValueError("Uuid may only contain hexidecimal values. 0-9A-F")
            # if here we can assign
            self._uuid = uuid.UUID(str(reuse))
        else:
            self._uuid = uuid.uuid4()

    def __str__(self) -> str:
        return str(self._uuid)

    def __eq__(self, value: Any) -> bool:
        self_str = str(self)
        other_str = str(value)
        return self_str == other_str

    def __len__(self) -> int:
        # 32 characters, but each is 1 hex, which is 1/2 an octet
        return 16

    def serialize(self) -> bytes:
        ser_ret = BytesIO()
        self._uuid
        ser_ret.write(self._uuid.bytes_le)
        return ser_ret.getvalue()

    @classmethod
    def deserialize(
        cls, ba: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Self, bytes]:
        """Neither group_type nor align_to are used"""
        new_uuid = uuid.UUID(bytes_le=ba[:16])
        return (cls(reuse=new_uuid), ba[16:])

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized"""
        # 32 characters, but each is 1 hex, which is 1/2 an octet
        return self.struct_size_bytes()

    @classmethod
    def struct_size_bytes(cls) -> int:
        return 16

    @property
    def block_length(self) -> int:
        return self.struct_size_bytes()


class Negotiate(SplineFixType):
    _messageType: FixpMessageType = FixpMessageType.Negotiate
    _sessionId: Uuid
    _schemaId: int = SPLINE_FIXP_SESSION_SCHEMA_ID
    _version: int = SPLINE_MUNI_DATA_SCHEMA_VERSION
    _timestamp_nano: SplineNanoTime
    _clientFlow: FlowTypeEnum
    _credentials: Jwt
    # fixed size fields only, no groups
    _block_length: int
    # 16 bytes
    _block_length = Uuid.struct_size_bytes()
    # u64, 8 bytes
    _block_length += SplineNanoTime.struct_size_bytes()
    # FlowTypeEnum
    # 1 byte
    _block_length += 1
    # will want padding of 3 before group
    _group_pad = align_by(_block_length)
    # we add it to the blockLength so that the BytesData doesn't
    # try to align it again when using struct_size_bytes()
    _block_length += _group_pad

    # credentials are a group, and aren't included in blockLength

    # schema_id, version, and template_id are all needed only when static
    # but after >=3.11 no longer supports both @classmethod and @property
    # so making them all functions
    @property
    def template_id(self) -> int:
        return Negotiate._messageType.value

    @property
    def schema_id(self) -> int:
        return Negotiate._schemaId

    @property
    def version(self) -> int:
        return Negotiate._version

    @property
    def timestamp(self) -> SplineNanoTime:
        return self._timestamp_nano

    @property
    def blockLength(self) -> int:
        return self._block_length

    @property
    def num_groups(self) -> int:
        # the credentials, even if empty
        return 1

    def __init__(
        self,
        credentials: Union[Jwt, bytes],
        uuid: Optional[Uuid] = None,
        timestamp: Optional[
            Union[dt.datetime, SplineDateTime, SplineNanoTime, float]
        ] = None,
        client_flow: Optional[FlowTypeEnum] = FlowTypeEnum.NONE,
    ) -> None:
        logging.debug(
            "Negotiate(credentials: {!r}, uuid: {}, timestamp: {}, client_flow: {})".format(
                credentials, uuid, timestamp, client_flow
            )
        )
        # force validation
        logging.debug("type(uuid)={}".format(type(uuid)))
        self.sessionId = uuid
        if isinstance(credentials, bytes):
            credentials = Jwt(token=credentials)
        if isinstance(credentials, Jwt):
            logging.debug(
                "Negotiate.__init__ got Jwt. Assigning to credentials. {}".format(
                    credentials
                )
            )
            self._credentials = credentials
        else:
            raise NotImplementedError

        self.timestamp_nano = timestamp

        if not isinstance(client_flow, FlowTypeEnum):
            raise NotImplementedError
        self._clientFlow = client_flow

    @property
    def credentials(self) -> Jwt:
        logging.debug(
            f"Negotiate: Flow={self._clientFlow} returning credentials={self._credentials}"
        )
        return self._credentials

    @credentials.setter
    def credentials(self, cred: Union[Jwt, bytes]) -> None:
        if isinstance(cred, bytes):
            cred = Jwt(token=cred)
        if isinstance(cred, Jwt):
            logging.debug(
                "Negotiate.__init__ flow={} got Jwt. Assigning to credentials. {}".format(
                    self._clientFlow, cred
                )
            )
            self._credentials = cred
        else:
            raise NotImplementedError

    @property
    def sessionId(self) -> Uuid:
        return self._sessionId

    @sessionId.setter
    def sessionId(self, value: Optional[Uuid] = None) -> None:
        logging.debug("sessionId.setter({})".format(value))
        if value is None:
            value = Uuid()
        if isinstance(value, Uuid):
            self._sessionId = value
        else:
            raise NotImplementedError

    @property
    def timestamp_nano(self) -> SplineNanoTime:
        return self._timestamp_nano

    @timestamp_nano.setter
    def timestamp_nano(
        self,
        tmstamp: Optional[
            Union[SplineNanoTime, SplineDateTime, dt.datetime, float]
        ] = None,
    ) -> None:
        logging.debug(
            "timestamp_nano: type(tmstmp)={} value={}".format(type(tmstamp), tmstamp)
        )
        value: SplineNanoTime
        if tmstamp is None:
            tmstamp = dt.datetime.now()
        if isinstance(tmstamp, float):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, dt.datetime):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, SplineDateTime):
            value = SplineNanoTime(other=tmstamp.timestamp)
        elif isinstance(tmstamp, SplineNanoTime):
            value = tmstamp
        else:
            raise NotImplementedError

        self._timestamp_nano = value

    def serialize(self) -> bytes:
        # jwt needs to come last because it's a "group"
        # which has varying length
        ret_buf = self._sessionId.serialize()
        ret_buf += self._timestamp_nano.serialize()
        ret_buf += struct.pack("<B", self._clientFlow.value)
        padding = self._group_pad
        logging.debug("serialize: padding is {}".format(padding))
        ret_buf += b"\x00" * padding
        ret_buf += self._credentials.serialize()
        logging.debug(
            "serialize just credentials:\n{!r}".format(
                ret_buf[self.struct_size_bytes() :].hex(":")
            )
        )
        return ret_buf

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Self, bytes]:
        """Neither group_type nor align_to are used"""
        logging.debug(
            "Negotiate.deserialize(): len(): {} data: {}".format(
                len(data), data.hex(":")
            )
        )

        # 16 bytes
        (sessionId, remaining_bytes) = Uuid.deserialize(ba=data)
        # 8 bytes
        (timestamp, remaining_bytes) = SplineNanoTime.deserialize(remaining_bytes)
        # 1 byte
        (client_flow_int,) = struct.unpack("<B", remaining_bytes[0:1])
        client_flow = FlowTypeEnum(client_flow_int)
        remaining_bytes = remaining_bytes[1:]
        logging.debug("serialize: padding is {}".format(cls._group_pad))
        remaining_bytes = remaining_bytes[cls._group_pad :]
        # now our group
        (credentials, remaining_bytes) = Jwt.deserialize(remaining_bytes)

        new_self = cls(
            credentials=cast(Jwt, credentials),
            uuid=cast(Uuid, sessionId),
            timestamp=cast(SplineNanoTime, timestamp),
            client_flow=client_flow,
        )
        return (new_self, remaining_bytes)

    @classmethod
    def deserialize_to_dict(cls, data: bytes) -> tuple[Dict[str, Any], bytes]:
        """For error handling situations when a proper message can't
        be constructed.
        """
        logging.debug(
            "Negotiate.deserialize(): len(): {} data: {}".format(
                len(data), data.hex(":")
            )
        )

        parts: Dict[str, Any] = {}
        remaining_bytes = data
        # 16 bytes
        try:
            (sessionId, remaining_bytes) = Uuid.deserialize(ba=remaining_bytes)
            parts.update({"sessionId": sessionId})
        except Exception as ex:
            logging.warning("unable to deserialize Uuid. {}".format(ex))
            # consume the bytes
            remaining_bytes = remaining_bytes[Uuid.struct_size_bytes() :]

        # 8 bytes
        try:
            (timestamp, remaining_bytes) = SplineNanoTime.deserialize(remaining_bytes)
            parts.update({"timestamp": timestamp})
        except Exception as ex:
            logging.warning("unable to deserialize timestamp. {}".format(ex))
            # consume the bytes
            remaining_bytes = remaining_bytes[SplineDateTime.struct_size_bytes() :]

        # 1 byte
        try:
            (client_flow_int,) = struct.unpack("<B", remaining_bytes[0:1])
            client_flow = FlowTypeEnum(client_flow_int)
            parts.update({"clientFlow": client_flow})
            remaining_bytes = remaining_bytes[1:]
        except Exception as ex:
            logging.warning("unable to deserialize clientflow. {}".format(ex))
            # consume the bytes
            remaining_bytes = remaining_bytes[1:]

        logging.debug("serialize: padding is {}".format(cls._group_pad))
        remaining_bytes = remaining_bytes[cls._group_pad :]
        # now our group
        try:
            (credentials, remaining_bytes) = Jwt.deserialize(remaining_bytes)
            parts.update({"credentials": credentials})
        except Exception as ex:
            logging.warning("unable to deserialize Jwt. {}".format(ex))
            # consume the bytes
            remaining_bytes = remaining_bytes[Jwt.struct_size_bytes() :]

        return (parts, remaining_bytes)

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized."""
        serialized = self.serialize()
        return len(serialized)

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls._block_length

    def __str__(self) -> str:
        str_buf = StringIO()
        str_buf.write("FIXP:\n")
        str_buf.write("  messageType: {}\n".format(str(self._messageType)))
        str_buf.write("  sessionId: {}".format(self._sessionId))
        str_buf.write("  timestamp_nano: {}".format(self._timestamp_nano))
        str_buf.write("  clientFlow: {}".format(self._clientFlow))
        str_buf.write("  credentials: {}".format(self._credentials))
        str_buf.write("  blockLength: {}".format(self._block_length))
        str_buf.write("  groupPad: {}".format(self._group_pad))
        return str_buf.getvalue()

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, type(self)):
            raise NotImplementedError

        if self.sessionId != value.sessionId:
            logging.debug("{} != {}".format(self.sessionId, value.sessionId))
            return False
        if self.timestamp_nano != value.timestamp_nano:
            logging.debug("{} != {}".format(self.timestamp_nano, value.timestamp_nano))
            return False
        if self._clientFlow != value._clientFlow:
            logging.debug("{} != {}".format(self._clientFlow, value._clientFlow))
            return False
        if self._credentials != value._credentials:
            logging.debug("{} != {}".format(self._credentials, value._credentials))
            return False
        return True


class NegotiationResponse(SplineFixType):
    _messageType: FixpMessageType = FixpMessageType.NegotiationResponse
    _sessionId: Uuid
    _schema_id: int = SPLINE_FIXP_SESSION_SCHEMA_ID
    _version: int = SPLINE_MUNI_DATA_SCHEMA_VERSION
    _request_timestamp_nano: SplineNanoTime
    _serverFlow: FlowTypeEnum
    # servers credentials
    _credentials: SplineByteData
    # fixed size fields only, no groups
    _block_length: int
    # 16 bytes
    _block_length = Uuid.struct_size_bytes()
    # u64, 8 bytes
    _block_length += SplineNanoTime.struct_size_bytes()
    # FlowTypeEnum
    # 1 byte
    _block_length += 1
    # will want padding ? before group
    _group_pad = align_by(_block_length)
    # we add it to the blockLength so that the BytesData doesn't
    # try to align it again when using struct_size_bytes()
    _block_length += _group_pad

    @property
    def session_id(self) -> Uuid:
        return self._sessionId

    @session_id.setter
    def session_id(self, value: Optional[Uuid]) -> None:
        self.sessionId = value

    # credentials are a group, and aren't included in blockLength
    def __init__(
        self,
        session_id: Uuid,
        request_timestamp: Union[dt.datetime, SplineDateTime, SplineNanoTime, float],
        server_flow: FlowTypeEnum = FlowTypeEnum.IDEMPOTENT,
        credentials: Optional[bytes] = None,
    ) -> None:
        logging.debug(
            "NegotiateResponse(credentials: {!r}, session_id: {}, request_timestamp: {}, server_flow: {})".format(
                credentials, session_id, request_timestamp, server_flow
            )
        )
        # force validation
        logging.debug("type(session_id)={}".format(type(session_id)))
        self.sessionId = session_id
        if credentials is None:
            self._credentials = SplineByteData(bytes())
        elif isinstance(credentials, bytes):
            self._credentials = cast(SplineByteData, SplineByteData(credentials))
        elif isinstance(credentials, Jwt):
            logging.debug(
                "NegotiateResponse.__init__ got Jwt. Assigning to credentials. {}".format(
                    credentials
                )
            )
            self._credentials = cast(SplineByteData, credentials)
        else:
            raise NotImplementedError

        self.request_timestamp_nano = request_timestamp

        if not isinstance(server_flow, FlowTypeEnum):
            raise NotImplementedError
        self._serverFlow = server_flow

    @property
    def num_groups(self) -> int:
        return 1

    @property
    def template_id(self) -> int:
        return NegotiationResponse._messageType.value

    @property
    def schema_id(self) -> int:
        return NegotiationResponse._schema_id

    @property
    def version(self) -> int:
        return NegotiationResponse._version

    @property
    def blockLength(self) -> int:
        return self._block_length

    @classmethod
    def from_negotiate(cls, neg: Negotiate) -> Self:
        if not isinstance(neg, Negotiate):
            raise NotImplementedError
        return cls(
            session_id=neg.sessionId,
            request_timestamp=neg.timestamp_nano,
        )

    @property
    def sessionId(self) -> Uuid:
        return self._sessionId

    @sessionId.setter
    def sessionId(self, value: Optional[Uuid] = None) -> None:
        logging.debug("sessionId.setter({})".format(value))
        if value is None:
            value = Uuid()
        if isinstance(value, Uuid):
            self._sessionId = value
        else:
            raise NotImplementedError

    @property
    def request_timestamp_nano(self) -> SplineNanoTime:
        return self._request_timestamp_nano

    @request_timestamp_nano.setter
    def request_timestamp_nano(
        self,
        tmstamp: Optional[
            Union[SplineNanoTime, SplineDateTime, dt.datetime, float]
        ] = None,
    ) -> None:
        logging.debug(
            "request_timestamp_nano: type(tmstmp)={} value={}".format(
                type(tmstamp), tmstamp
            )
        )
        value: SplineNanoTime
        if tmstamp is None:
            tmstamp = dt.datetime.now()
        if isinstance(tmstamp, float):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, dt.datetime):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, SplineDateTime):
            value = SplineNanoTime(other=tmstamp.timestamp)
        elif isinstance(tmstamp, SplineNanoTime):
            value = tmstamp
        else:
            raise NotImplementedError

        self._request_timestamp_nano = value

    def serialize(self) -> bytes:
        # jwt/data needs to come last because it's a "group"
        # which has varying length
        ret_buf = self._sessionId.serialize()
        ret_buf += self._request_timestamp_nano.serialize()
        ret_buf += struct.pack("<B", self._serverFlow.value)
        padding = self._group_pad
        logging.debug("serialize: padding is {}".format(padding))
        ret_buf += b"\x00" * padding
        ret_buf += self._credentials.serialize()
        logging.debug(
            "serialize just credentials:\n{!r}".format(
                ret_buf[self.struct_size_bytes() :].hex(":")
            )
        )
        return ret_buf

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Self, bytes]:
        """Neither group_type nor align_to are used"""
        logging.debug(
            "NegotiateResponse.deserialize(): len(): {} data: {}".format(
                len(data), data.hex(":")
            )
        )
        # 16 bytes
        (sessionId, remaining_bytes) = Uuid.deserialize(ba=data)
        # 8 bytes
        (timestamp, remaining_bytes) = SplineNanoTime.deserialize(remaining_bytes)
        # 1 byte
        (server_flow_int,) = struct.unpack("<B", remaining_bytes[0:1])
        server_flow = FlowTypeEnum(server_flow_int)
        remaining_bytes = remaining_bytes[1:]
        logging.debug("serialize: padding is {}".format(cls._group_pad))
        remaining_bytes = remaining_bytes[cls._group_pad :]
        # now our group
        (credentials, remaining_bytes) = SplineByteData.deserialize(
            remaining_bytes, group_type=bytes
        )

        new_self = cls(
            session_id=cast(Uuid, sessionId),
            request_timestamp=cast(SplineNanoTime, timestamp),
            server_flow=server_flow,
            credentials=cast(bytes, credentials),
        )
        return (new_self, remaining_bytes)

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized."""
        serialized = self.serialize()
        return len(serialized)

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls._block_length

    def __str__(self) -> str:
        str_buf = StringIO()
        str_buf.write("FIXP:\n")
        str_buf.write("  messageType: {}\n".format(str(self._messageType)))
        str_buf.write("  sessionId: {}".format(self._sessionId))
        str_buf.write(
            "  request_timestamp_nano: {}".format(self._request_timestamp_nano)
        )
        str_buf.write("  clientFlow: {}".format(self._serverFlow))
        str_buf.write("  credentials: {}".format(self._credentials))
        str_buf.write("  blockLength: {}".format(self._block_length))
        str_buf.write("  groupPad: {}".format(self._group_pad))
        return str_buf.getvalue()

    def __eq__(self, value: Any) -> bool:
        logging.debug("NegotiationResponse: self={}\nother={}".format(self, value))
        if not isinstance(value, type(self)):
            raise NotImplementedError

        if self.sessionId != value.sessionId:
            logging.debug("{} != {}".format(self.sessionId, value.sessionId))
            return False
        if self.request_timestamp_nano != value.request_timestamp_nano:
            logging.debug(
                "{} != {}".format(
                    self.request_timestamp_nano, value.request_timestamp_nano
                )
            )
            return False
        if self._serverFlow != value._serverFlow:
            logging.debug("{} != {}".format(self._serverFlow, value._serverFlow))
            return False
        if self._credentials != value._credentials:
            logging.debug("{} != {}".format(self._credentials, value._credentials))
            return False
        return True


class NegotiationRejectCodeEnum(Enum):
    Unspecified = 0
    Credentials = 1
    FlowTypeNotSupported = 2
    DuplicateId = 3


REJECTION_REASON_STRINGS = [
    "Unspecified:",
    "Credentials: failed authentication because identity is not recognized, or the user is not authorized.",
    "FlowTypeNotSupported: server does not support requested flow type.",
    "DuplicateId: session ID is non-unique.",
]


class NegotiationReject(SplineFixType):
    _messageType: FixpMessageType = FixpMessageType.NegotiationReject
    _schema_id: int = SPLINE_FIXP_SESSION_SCHEMA_ID
    _version: int = SPLINE_MUNI_DATA_SCHEMA_VERSION
    _sessionId: Uuid
    _request_timestamp_nano: SplineNanoTime
    _reject_code: NegotiationRejectCodeEnum
    _reject_str: SplineByteData
    # fixed size fields only, no groups
    _block_length: int
    # 16 bytes
    _block_length = Uuid.struct_size_bytes()
    # u64, 8 bytes
    _block_length += SplineNanoTime.struct_size_bytes()
    # NegotiationRejectCodeEnum
    # 1 byte
    _block_length += 1
    # will want padding before group
    _group_pad = align_by(_block_length)
    # we add it to the blockLength so that the BytesData doesn't
    # try to align it again when using struct_size_bytes()
    _block_length += _group_pad
    _num_groups = 1

    @property
    def num_groups(self) -> int:
        return self._num_groups

    # Reason string is a group, and groups aren't included in blockLength
    def __init__(
        self,
        session_id: Uuid,
        request_timestamp: Union[dt.datetime, SplineDateTime, SplineNanoTime, float],
        reject_code: NegotiationRejectCodeEnum = NegotiationRejectCodeEnum.Unspecified,
        reject_str: Optional[str] = None,
    ) -> None:
        logging.debug(
            "NegotiationReject(session_id: {}, request_timestamp: {}, reject_code: {}, reject_string: {})".format(
                session_id,
                request_timestamp,
                reject_code,
                REJECTION_REASON_STRINGS[reject_code.value],
            )
        )
        # force validation
        logging.debug("type(uuid)={}".format(type(uuid)))
        self.sessionId = session_id

        self.request_timestamp_nano = request_timestamp

        if not isinstance(reject_code, NegotiationRejectCodeEnum):
            raise NotImplementedError
        self._reject_code = reject_code

        if reject_str is None:
            reject_str = REJECTION_REASON_STRINGS[reject_code.value]
            self._reject_str = SplineByteData(data=reject_str.encode("latin-1"))
        elif isinstance(reject_str, str):
            # prepend - Unspecified:
            if not reject_str.startswith(REJECTION_REASON_STRINGS[reject_code.value]):
                reject_str = REJECTION_REASON_STRINGS[reject_code.value] + reject_str

            # turn str into bytes
            reject_bytes = reject_str.encode("latin-1")
            self._reject_str = SplineByteData(data=reject_bytes)

    @property
    def template_id(self) -> int:
        return NegotiationReject._messageType.value

    @property
    def schema_id(self) -> int:
        return NegotiationReject._schema_id

    @property
    def version(self) -> int:
        return NegotiationReject._version

    @classmethod
    def from_negotiate(
        cls,
        neg: Negotiate,
        reject_code: Optional[
            NegotiationRejectCodeEnum
        ] = NegotiationRejectCodeEnum.Unspecified,
        reject_str: Optional[str] = None,
    ) -> Self:
        if not isinstance(neg, Negotiate):
            raise NotImplementedError
        return cls(
            session_id=neg.sessionId,
            request_timestamp=neg.timestamp_nano,
            reject_code=cast(NegotiationRejectCodeEnum, reject_code),
            reject_str=reject_str,
        )

    @property
    def sessionId(self) -> Uuid:
        return self._sessionId

    @sessionId.setter
    def sessionId(self, value: Optional[Uuid] = None) -> None:
        logging.debug("sessionId.setter({})".format(value))
        if value is None:
            value = Uuid()
        if isinstance(value, Uuid):
            self._sessionId = value
        else:
            raise NotImplementedError

    @property
    def request_timestamp_nano(self) -> SplineNanoTime:
        return self._request_timestamp_nano

    @request_timestamp_nano.setter
    def request_timestamp_nano(
        self,
        tmstamp: Optional[
            Union[SplineNanoTime, SplineDateTime, dt.datetime, float]
        ] = None,
    ) -> None:
        logging.debug(
            "request_timestamp_nano: type(tmstmp)={} value={}".format(
                type(tmstamp), tmstamp
            )
        )
        value: SplineNanoTime
        if tmstamp is None:
            tmstamp = dt.datetime.now()
        if isinstance(tmstamp, float):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, dt.datetime):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, SplineDateTime):
            value = SplineNanoTime(other=tmstamp.timestamp)
        elif isinstance(tmstamp, SplineNanoTime):
            value = tmstamp
        else:
            raise NotImplementedError

        self._request_timestamp_nano = value

    def serialize(self) -> bytes:
        # jwt/data needs to come last because it's a "group"
        # which has varying length
        ret_buf = self._sessionId.serialize()
        ret_buf += self._request_timestamp_nano.serialize()
        ret_buf += struct.pack("<B", self._reject_code.value)
        padding = self._group_pad
        logging.debug("serialize: padding is {}".format(padding))
        ret_buf += b"\x00" * padding
        ret_buf += self._reject_str.serialize()
        logging.debug(
            "serialize just reject reason str:\n{!r} : {}".format(
                ret_buf[self.struct_size_bytes() :].hex(":"),
                self._reject_str.data.decode("latin-1"),
            )
        )
        return ret_buf

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Any, bytes]:
        """Neither group_type nor align_to are used"""
        logging.debug(
            "NegotiationReject.deserialize(): len(): {} data: {}".format(
                len(data), data.hex(":")
            )
        )
        # 16 bytes
        (sessionId, remaining_bytes) = Uuid.deserialize(ba=data)
        # 8 bytes
        (timestamp, remaining_bytes) = SplineNanoTime.deserialize(remaining_bytes)
        # 1 byte
        (reject_code_int,) = struct.unpack("<B", remaining_bytes[0:1])
        reject_code = NegotiationRejectCodeEnum(reject_code_int)
        remaining_bytes = remaining_bytes[1:]
        logging.debug("serialize: padding is {}".format(cls._group_pad))
        remaining_bytes = remaining_bytes[cls._group_pad :]
        # now our group
        (reject_str, remaining_bytes) = SplineByteData.deserialize(
            remaining_bytes, group_type=bytes
        )

        new_self = cls(
            session_id=cast(Uuid, sessionId),
            request_timestamp=cast(SplineNanoTime, timestamp),
            reject_code=reject_code,
            reject_str=cast(bytes, reject_str).decode("latin-1"),
        )
        return (new_self, remaining_bytes)

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized."""
        serialized = self.serialize()
        return len(serialized)

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls._block_length

    def __str__(self) -> str:
        str_buf = StringIO()
        str_buf.write("FIXP:\n")
        str_buf.write("  messageType: {}\n".format(str(self._messageType)))
        str_buf.write("  sessionId: {}".format(self._sessionId))
        str_buf.write(
            "  request_timestamp_nano: {}".format(self._request_timestamp_nano)
        )
        str_buf.write("  reject_code: {}".format(self._reject_code))
        str_buf.write("  Reason: {}".format(self._reject_str))
        str_buf.write("  blockLength: {}".format(self._block_length))
        str_buf.write("  groupPad: {}".format(self._group_pad))
        return str_buf.getvalue()

    def __eq__(self, value: Any) -> bool:
        logging.debug("NegotiationResponse: self={}\nother={}".format(self, value))
        if not isinstance(value, type(self)):
            raise NotImplementedError

        if self.sessionId != value.sessionId:
            logging.debug("{} != {}".format(self.sessionId, value.sessionId))
            return False
        if self.request_timestamp_nano != value.request_timestamp_nano:
            logging.debug(
                "{} != {}".format(
                    self.request_timestamp_nano, value.request_timestamp_nano
                )
            )
            return False
        if self._reject_code != value._reject_code:
            logging.debug("{} != {}".format(self._reject_code, value._reject_code))
            return False
        if self._reject_str != value._reject_str:
            logging.debug("{} != {}".format(self._reject_str, value._reject_str))
            return False
        return True
