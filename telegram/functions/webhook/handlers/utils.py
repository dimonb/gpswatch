# -- coding: utf-8 --

import os
import telepot
import json
import botocore.session

def create_client(client):
    aws_key_id = os.environ['ACCESS_KEY_ID']
    aws_key_secret = os.environ['SECRET_ACCESS_KEY']
    aws_region = os.environ['REGION']
    #log.debug('key: %s, secret: %s, region: %s', aws_key_id, aws_key_secret, aws_region)
    session = botocore.session.get_session()
    return session.create_client(client, region_name=aws_region,
            aws_secret_access_key=aws_key_secret,
            aws_access_key_id=aws_key_id)

class SQSHelper:
    __sqs = None
    __queue_url = None
    def __init__(self):
        if not SQSHelper.__sqs:
            SQSHelper.__sqs = create_client('sqs')
            SQSHelper.__queue_url = SQSHelper.__sqs.create_queue(QueueName='gpswatch-queue')['QueueUrl']
        self.client = SQSHelper.__sqs
        self.url = SQSHelper.__queue_url

    def send_message(self, msg):
        self.client.send_message(QueueUrl=self.url, MessageBody=msg)

class DynamoHelper:
    __dynamo = None
    def __init__(self):
        if not DynamoHelper.__dynamo:
            DynamoHelper.__dynamo = create_client('dynamodb')
        self.client = DynamoHelper.__dynamo

    def get_object(self, key, ts):
        res = self.client.get_item(
            TableName='gpswatch',
            Key={'device_id': {'S': key}, 'ts': {'N': '%s'%ts}},
            ConsistentRead=True,
        )
        return res.get('Item')

    def get_settings(self, key):
        return self.get_object(key, 0)

    
    def send_message(self, msg):
        self.client.put_item(
            TableName='gpswatch',
            Item=msg.to_dynamo()
        )
        
        SQSHelper().send_message(json.dumps({'id': msg.identifier, 'cmd': msg.cmd, 'direction': msg.direction}))

    def update_settings(self, key, expression, values):
        self.client.update_item(
            TableName='gpswatch',
            Key={'device_id': {'S': key}, 'ts': {'N': '0'}},
            UpdateExpression=expression,
            ExpressionAttributeValues=values)


    def active_devices(self, user_id):
        res = self.get_settings('TEL_%s'%user_id)
        return res['watches_device_id']['SS'] if res else None


    def query(self, **kwargs):
        while True:
            res = self.client.query(
                TableName='gpswatch',
                **kwargs
            )
            for item in res['Items']:
                yield item
            
            if 'LastEvaluatedKey' in res:
                kwargs['ExclusiveStartKey'] = res['LastEvaluatedKey']
            else:
                break


bot = telepot.Bot(os.environ['BOT_KEY'])
