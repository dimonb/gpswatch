import binascii
import json
import requests
import ast
import arrow
import struct
import six
import os
from cached_property import cached_property


class tstate:
    ALBITS = {
            16: 'SOS',
            17: 'low battery',
            18: 'out fence',
            19: 'into fence',
            20: 'remove watch',
    }
    def __init__(self, st):
        self.st = st

    @property
    def alarm(self):
        tstate = struct.unpack('>L', binascii.unhexlify(self.st))[0]
        al = []
        for b in self.ALBITS:
            if (tstate & (1 << b)):
                al.append(self.ALBITS[b])
        return ''.join(al)

    @property
    def on_wrist(self):
        return not (struct.unpack('>L', binascii.unhexlify(self.st))[0] & (1 << 3))

class ld:
    def __init__(self, params):
        params = params.decode().split(',')
        self.date, self.time = params[:2]
        self.position = params[2:11]
        self.gsm = params[11]
        self.batt = params[12]
        self.steps, self.roll, self.tstate = params[13:16]
        self.gsm_info = params[16:]
    
        # tstate: struct.unpack('>L', binascii.unhexlify(tstate))
        # check bit: & (1 << N)
        #  0 - low battery
        #  1 - out of fence state
        #  2 - Into the fence state
        #  3 - watch state
        # 16 - SOS alarm
        # 17 - low battery alarm
        # 18 - out fence alarm
        # 19 - into fence alarm
        # 20 - remove watch alarm
   
    @property
    def alarm(self):
        return tstate(self.tstate).alarm

    @property
    def on_wrist(self):
        return tstate(self.tstate).on_wrist

    @staticmethod
    def gsm_location_from_info(info):
        if not 'GOOGLE_KEY' in os.environ: return None

        sts = []
        bsn, bst, mcc, mnc = info[:4]
        for i in range(int(bsn)):
            lac, cellid, ss = info[4+i*3: 4+3+i*3]
            sts.append(
				{
					  "cellId": cellid,
					  "locationAreaCode": lac,
					  "mobileCountryCode": mcc,
					  "mobileNetworkCode": mnc,
					  "age": 0,
					  "signalStrength": ss,
				}
			)
        data = {'cellTowers': sts}

        res = requests.post(
            'https://www.googleapis.com/geolocation/v1/geolocate', 
            params={'key': os.environ['GOOGLE_KEY']},
            headers={'Content-Type': 'application/json'},
            data = json.dumps(data)
        ) 

        return res.json()


    @cached_property
    def gsm_location(self):
        return self.gsm_location_from_info(self.gsm_info)

    @cached_property
    def lat(self):
        return ('' if self.position[2] == 'N' else '-') + self.position[1]

    @cached_property
    def lon(self):
        return ('' if self.position[4] == 'E' else '-') + self.position[3]

    @cached_property
    def ts_num(self):
        return arrow.get(self.date+' '+self.time, 'DDMMYY HHmmss').timestamp

def chunkstring(string, length):
    return (string[0+i:length+i] for i in range(0, len(string), length))

class message(object):
    def __init__(self, company, device_id, data, raw):
        self.raw = raw        
        self.data = data
        self.device_id = device_id.decode()
        self.company = company.decode()
        c = data.split(b',', 1)
        self.cmd, self.params = c[0].decode(), (None if len(c) == 1 else c[1])
        self.ts = arrow.utcnow().timestamp
        self.payload = None

    def to_dynamo(self):
        ident = self.identifier
        event = {
            'device_id': {'S': ident['device_id']},
            'ts': {'N': ident['ts']},
            'cmd': {'S': self.cmd},
            'direction': {'S': self.direction},
        }
        if self.params:
            try:
                event['data'] = {'S': self.params.decode()}
            except UnicodeDecodeError:
                event['data_raw'] = {'B': binascii.b2a_base64(self.params)}

        if self.payload:
            event['payload'] = {'S': self.payload}

        ld = self.location_data

        if ld:
            event['location'] = {'M': {
                'ts': {'N': str(ld.ts_num)},
                'type': {'S': ld.position[0]},
                'lat': {'N': ld.lat},
                'lon': {'N': ld.lon},
                'batt': {'N': ld.batt},
            }}

        return event

    @classmethod
    def from_dynamo(cls, data):
        #print (data)

        if 'data' in data:
            params = data['data']['S'].encode('ascii')
        elif 'data_raw' in data:
            params = binascii.a2b_base64(data['data_raw']['B'])
        else:
            params = None

        company, device_id = data['device_id']['S'].split('_', 2)
        msg = cls.from_command(
            company, device_id, 
            data['cmd']['S'], 
            params = params,
            direction = data['direction']['S'],
            ts = data['ts']['N'].split('.')[0]
        )

        assert data['ts']['N'] == msg.identifier['ts'], 'in db: %s, expected %s'%(data['ts'], msg.identifier)

        return msg

    @property
    def identifier(self):
        return {
            'ts': ('%s.%s'%(str(self.ts), self.crc32())).rstrip('0'), 
            'device_id': '%s_%s'%(self.company, self.device_id)
        }

    def to_json(self):
        return {
            'cmd': self.cmd.decode(),
            'params': binascii.b2a_base64(self.params or b'').decode(),
            'device': self.device_id.decode(),
            'company': self.company.decode(),
        }

    @classmethod
    def from_command(cls, company, device_id, cmd, params=None, direction=None, ts=None, payload=None):
        msg = cls.__new__(cls)
        msg.company = company
        msg.device_id = device_id
        msg.cmd = cmd
        msg.params = params.encode() if params is not None and not isinstance(params, bytes) else params
        msg.ts = arrow.utcnow().timestamp if ts is None else ts
        msg.data = cmd.encode('ascii') + (b',%s'%msg.params if msg.params else b'')
        msg.raw = b'[%s*%s*%s*%s]'%(
            company.encode('ascii'), 
            device_id.encode('ascii'),
            binascii.hexlify(struct.pack('>H', len(msg.data))).upper(),
            msg.data
        )
        msg.payload = payload
        msg.direction = direction
        return msg

    @classmethod
    def from_json(cls, data):
        try:
            msg_st = json.loads(data)
        except json.decoder.JSONDecodeError:
            #got old message format
            msg_st = ast.literal_eval(data)
            for k in list(msg_st['cmd'].keys()):
                msg_st['cmd'][k.decode()] = msg_st['cmd'][k]

        msg = cls.__new__(cls)
        msg.company = str(msg_st['cmd']['company'], encoding='ascii')
        msg.device_id = str(msg_st['cmd']['device'], encoding='ascii')
        msg.cmd = str(msg_st['cmd']['cmd'], encoding='ascii')
        msg.params = binascii.a2b_base64(msg_st['cmd']['params'])
        msg.ts = msg_st['ts']
        msg.direction = msg_st['from']
        msg.payload = None
        msg.raw = data
        return msg

    @cached_property
    def location_data(self):
        if self.cmd not in ('UD', 'UD2', 'AL'): return None
        return ld(self.params)
    
    def crc32(self):
        '''this code is so strange because of need to remain compatible with old data in database'''
        
        fstr = "%s.%s.%s.b%s.%s" if six.PY2 and self.params is not None else '%s.%s.%s.%s.%s'  #some compatibility fixes with python2.7 backport
        print ((fstr%(self.company, self.device_id, self.cmd, repr(self.params), self.direction)).encode())
        return binascii.crc32((fstr%(self.company, self.device_id, self.cmd, repr(self.params), self.direction)).encode()) & 0xffffffff

