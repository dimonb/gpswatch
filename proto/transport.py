import binascii, struct
import logging

from .message import message

log = logging.getLogger('gpstrack.proto.transport')

class ProtocolException(Exception): pass

class stream:
    def __init__(self):
        self.buffer = b''
    
    def send(self, data):
        log.debug('data received: %r', data)
        self.buffer += data
        p = self.buffer.split(b'*', 3)
        if len(p) < 4:
            if len(self.buffer) > 1024*1024:
                raise ProtocolException('message size limit exceeded')
            return

        company, device_id, length, msg = p
        prefix_size = sum(len(i)+1 for i in p[:3])
        length = struct.unpack('>H', binascii.unhexlify(length))[0]

        if company[:1] != b'[':
            raise ProtocolException('invalid message start, expected "[", got "%r"'%company[:1])
        company = company[1:]

        if len(msg) <= length:
            return #need to wait remain message

        raw = self.buffer[:prefix_size+length+1]
        self.buffer = msg[length+1:]
        if msg[length:length+1] != b']':
            raise ProtocolException('invalid message end, expected "]", got "%r"'%msg[length])

        self.message_received(message(company, device_id, msg[:length], raw))

    def message_received(self, msg):
        return None
