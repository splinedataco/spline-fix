from typing import Optional
from io import BytesIO
from joshua.fix.sofh import Sofh
from joshua.fix.messages import SplineMessageHeader
from joshua.fix.common import align_by
from joshua.fix.types import SplineFixType


def generate_message_with_header(
    # 2 bytes
    block_length: int,
    # 1 byte
    template_id: int,
    # 1 byte
    schema_id: int,
    # 1 byte
    version: int,
    # 1 byte
    num_groups: int,
    data: Optional[bytes] = None,
) -> bytes:
    head_buffer = BytesIO()
    head_buffer.write(Sofh().serialize())
    # message header includes the root block size of the message
    # which here will just be the datetime
    curve_mh_bytes = SplineMessageHeader(
        block_length=block_length,
        template_id=template_id,
        schema_id=schema_id,
        version=version,
        num_groups=num_groups,
    ).serialize()
    head_buffer.write(curve_mh_bytes)
    # we'll want to align the next block to 4
    # mh_pad = align_by(message_head_length, 4)
    # message_head_length += mh_pad
    mh_pad = align_by(head_buffer.tell(), 4)
    head_buffer.write(mh_pad * b"\x00")

    if data is not None:
        head_buffer.write(data)
        sofh = Sofh(size=head_buffer.tell(), size_includes_self=True)
        head_buffer.seek(0, 0)
        head_buffer.write(sofh.serialize())
    return head_buffer.getvalue()


def generate_message_from_type(
    msg: SplineFixType, num_groups: Optional[int] = None
) -> bytes:
    """Given the type of the to the server object (needs the following
    cls.blockLength, cls.template_id, cls.to the server schema_id, cls.versioni
    TODO: add num_groups to session objects
    )
    the data to append
    """
    buffer = BytesIO()
    numg: int
    if num_groups is None:
        try:
            numg = msg.num_groups
        except AttributeError:
            numg = 0
    else:
        numg = int(num_groups)
    neg_msg_bytes = generate_message_with_header(
        block_length=msg.blockLength,
        template_id=msg.template_id,
        schema_id=msg.schema_id,
        version=msg.version,
        # empty credentials group
        num_groups=numg,
        data=msg.serialize(),
    )
    buffer.write(neg_msg_bytes)
    return buffer.getvalue()
