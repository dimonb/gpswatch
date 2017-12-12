import logging.handlers
import asyncio
import secrets
import binascii
import os
import json
import aiobotocore

from cached_property import cached_property

from proto.transport import stream
from proto.message import message

log = logging.getLogger('gpstrack.gpswatch')

class MessageQueue:
    def __init__(self, loop):
        self.loop = loop
        self.topic = None

    def create_client(self, client):
        aws_key_id = os.environ['AWS_ACCESS_KEY_ID']
        aws_key_secret = os.environ['AWS_SECRET_ACCESS_KEY']
        aws_region = os.environ['AWS_REGION']
        session = aiobotocore.get_session(loop=self.loop)
        return session.create_client(client, region_name=aws_region,
                aws_secret_access_key=aws_key_secret,
                aws_access_key_id=aws_key_id)

    @cached_property
    def sns(self):
        return self.create_client('sns')

    @cached_property
    def dynamo(self):
        return self.create_client('dynamodb')

    async def send_message(self, msg):
        if self.topic is None:
            self.topic = await self.sns.create_topic(Name="gpswatch")

        await self.dynamo.put_item(
            TableName='gpswatch',
            Item=msg.to_dynamo()
        )

        await self.sns.publish(
            TopicArn=self.topic['TopicArn'],
            Message=json.dumps({'id': msg.identifier, 'cmd': msg.cmd, 'direction': msg.direction}),
        )

    async def get_setings(self, device_id):
        return await self.dynamo.get_item(
            TableName='gpswatch',
            Key={'device_id': {'S': device_id}, 'ts': {'N': '0'}},
            ConsistentRead=True,
        )

    async def update_settings(self, device_id, key, value):
        return await self.dynamo.update_item(
            TableName='gpswatch',
            Key={'device_id': {'S': device_id}, 'ts': {'N': '0'}},
            UpdateExpression="SET %s = :val"%key,
            ExpressionAttributeValues={ 
                ":val": value
            },
        )

class GPSWatchClientProtocol(asyncio.Protocol, stream):
    def __init__(self, server, loop):
        self.server = server
        self.loop = loop
        self.context = 'S'+self.server.context[1:]
        self.transport = None
        self.commands = []
        self.queue = MessageQueue(loop)
        self.last_msg = None
        super().__init__()

    def connection_made(self, transport):
        self.peername = transport.get_extra_info('peername')
        log.info('Connection to server made: %s', self.peername, extra={'context': self.context})
        self.transport = transport

        cmds = self.commands
        self.command = []
        for c in cmds:
            asyncio.ensure_future(self.msg_from_watch(c))

    def data_received(self, data):
        asyncio.ensure_future(self.process_data(data))

    @asyncio.coroutine
    def process_data(self, data):
        asyncio.Task.current_task().context = self.context
        self.send(data)

    @asyncio.coroutine
    def msg_from_watch(self, msg):
        asyncio.Task.current_task().context = self.context
        if self.transport:
            self.transport.write(msg.raw)
        else:
            self.commands.append(msg)
        yield from self.queue.send_message(msg)

    def message_received(self, msg):
        msg.direction = 'server'
        if self.server:
            asyncio.ensure_future(self.server.msg_from_server(msg))

    def connection_lost(self, exc):
        log.info('connection to server lost', extra={'context': self.context})
        self.server.transport.close()
        self.server = None

 
class GPSWatchServerProtocol(asyncio.Protocol, stream):
    '''handle connection from watches, proxies it to original server and push to queue'''

    online = {}

    def __init__(self, loop, server_host_port = ('127.0.0.1', 8001)):
        try:
            self.loop = loop
            self.queue = MessageQueue(loop)
            self.server_host_port = server_host_port
        except:
            log.exception('init failed')
            raise
        
        self.last_msg = None
        self.device_id = None
        super().__init__()

    def connection_made(self, transport):
        self.peername = transport.get_extra_info('peername')
        self.context = 'W'+'{:#010x}'.format(binascii.crc32(str(id(self)).encode()))[2:].upper()
        
        log.info('Incoming connection from %s', self.peername, extra={'context': self.context})
        self.transport = transport
        self.client = None


    '''
    @asyncio.coroutine
    def connection_routine(self):
        asyncio.Task.current_task().context = self.context
        GPSWatchClientProtocol(self, self.loop)

        coro = self.loop.create_connection(lambda: self.client, self.server_host_port[0], self.server_host_port[1])
        asyncio.ensure_future(coro)
    '''

    def connection_lost(self, exc):
        log.info('connection to watches lost', extra={'context': self.context})

        if self.device_id:
            if GPSWatchServerProtocol.online[self.device_id] == self:
                GPSWatchServerProtocol.online.pop(self.device_id)

        if self.client.transport:
            self.client.transport.close()
        self.client = None

    def data_received(self, data):
        asyncio.ensure_future(self.process_data(data))

    @asyncio.coroutine
    def process_data(self, data):
        asyncio.Task.current_task().context = self.context
        self.send(data)

    @asyncio.coroutine
    def msg_from_server(self, msg):
        asyncio.Task.current_task().context = self.context
        self.send_message(msg)
        yield from self.queue.send_message(msg)

    async def msg_from_watch(self, msg):
        asyncio.Task.current_task().context = self.context
        if self.client is None:
            log.debug('initialize connection to server')
            self.device_id = msg.identifier['device_id']
            
            GPSWatchServerProtocol.online[self.device_id] = self

            if msg.cmd != 'LK':
                log.warning('connection is not ready, skip message')
                return
            settings = await self.queue.get_setings(msg.identifier['device_id'])
            log.debug(settings)

            if not 'Item' in settings:
                log.debug('no settings found, initialize new device')
                while True:
                    sc = secrets.randbelow(10000000000)
                    if not 'Item' in await self.queue.get_settings('SEC_%s'%sc):
                        break

                await self.queue.update_settings(msg.identifier['device_id'], 'secret', {'S': str(sc)})
                settings['Item'] = {'secret': {'S': str(sc)}}

            if not 'host' in settings['Item'] or not 'port' in settings['Item']:
                log.debug('no host and port configured')
                sc = settings['Item']['secret']['S']
                await self.queue.update_settings('SEC_%s'%sc, 'ref_device_id', {'S': msg.identifier['device_id']})
                sc_msg = message.from_command(msg.company, msg.device_id, 'MESSAGE', binascii.hexlify(sc.encode('utf-16-be')))
                log.debug('sending message with secret code: %s'%sc)
                self.transport.write(sc_msg.raw)
            else:
                self.client = GPSWatchClientProtocol(self, self.loop)
                await self.loop.create_connection(lambda: self.client, settings['Item']['host']['S'], settings['Item']['port']['S'])

        await self.client.msg_from_watch(msg)

    def send_message(self, msg):
        self.last_msg = msg
        self.transport.write(msg.raw)

    def message_received(self, msg):
        msg.direction = 'watch'
        if self.last_msg and self.last_msg.cmd == msg.cmd: #link reply
            msg.payload = json.dumps(self.last_msg.identifier)
            self.last_msg = None
        asyncio.ensure_future(self.msg_from_watch(msg))

async def process_gpswatch_queue(loop):
    queue = MessageQueue(loop)

    sqs = queue.create_client('sqs')
    dynamo = queue.create_client('dynamodb')

    q = await sqs.create_queue(QueueName='gpswatch-queue')

    while True:
        messages = await sqs.receive_message(QueueUrl=q['QueueUrl'], AttributeNames=['All'], WaitTimeSeconds=20)
        for m in messages.get('Messages', []):
            msg = json.loads(m['Body'])

            w_msg = await dynamo.get_item(
				TableName='gpswatch',
				Key={'device_id': {'S': msg['id']['device_id']}, 'ts': {'N': msg['id']['ts']}},
				ConsistentRead=True,
			)
            
            w_msg = w_msg.get('Item')

            if not w_msg:
                log.info('No record found: %s'%msg)
                continue
            
            if not msg['id']['device_id'] in GPSWatchServerProtocol.online:
                log.info('No device online: %s'%msg['id']['device_id'])
                continue

            d_msg = message.from_dynamo(w_msg)
            GPSWatchServerProtocol.online[msg['id']['device_id']].send_message(d_msg)

            await sqs.delete_message(QueueUrl=q['QueueUrl'], ReceiptHandle=m['ReceiptHandle'])

if __name__ == '__main__':

    # configure logging to support execution context
    loop = None
    class ContextLogRecord(logging.LogRecord):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            
            task = asyncio.Task.current_task(loop) if loop else None
            context =  getattr(task, 'context', None)
            if context:
                self.context = context

    class ContextFormatter(logging.Formatter):
        def format(self, record):
            if not hasattr(record, 'context'):
                record.context = 'GLOBAL'
            return super().format(record)

    logging.setLogRecordFactory(ContextLogRecord)
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    handler.setFormatter(ContextFormatter('%(asctime)s P%(process)s C%(context)s %(levelname)-8s %(name)-15s: %(message)s'))
    logger.addHandler(handler)
    handler2 = logging.handlers.TimedRotatingFileHandler('gpswatch.log', when='D', backupCount=10)
    handler2.setFormatter(ContextFormatter('%(asctime)s P%(process)s C%(context)s %(levelname)-8s %(name)-15s: %(message)s'))
    logger.addHandler(handler2)
    logger.setLevel(logging.DEBUG)

    loop = asyncio.get_event_loop()
    coro = loop.create_server(lambda: GPSWatchServerProtocol(loop, server_host_port=('52.28.132.157', 8001)), '0.0.0.0', 8001)
    asyncio.ensure_future(process_gpswatch_queue(loop), loop=loop)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    loop.stop()


