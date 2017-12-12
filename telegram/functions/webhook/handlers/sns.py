# -- coding: utf-8 -- 

import json
import utils
import requests
import StringIO
import ffmpy
import uuid
import subprocess
import logging
import os

import proto.message

import xml.etree.ElementTree as ET


log = logging.getLogger('webhook.handlers.sns')


def find_reply_message(w_msg):
    if not 'payload' in w_msg: return
    did = json.loads(w_msg['payload']['S'])
    if not 'ts' in did or not 'device_id' in did: return
    w_msg = utils.DynamoHelper().get_object(did['device_id'], did['ts'])
    if not 'payload' in w_msg: return
    did = json.loads(w_msg['payload']['S'])
    if not 'message_id' in did or not 'chat_id' in did: return
    return did['message_id']


def on_voice_message(chat_id, message):
    data = message.params
    m = [
        ('7d01', '7d'),
        ('7d02', '5b'),
        ('7d03', '5d'),
        ('7d04', '2c'),
        ('7d05', '2a')]
    
    for k, v in reversed(m):
        data = data.replace(k.decode('hex'), v.decode('hex'))

    ff = ffmpy.FFmpeg(
            inputs={'pipe:0': '-f amr'},
            outputs={'pipe:1': '-f opus'}
    )
    opus, stderr = ff.run(input_data=data, stdout=subprocess.PIPE)

    ff = ffmpy.FFmpeg(
            inputs={'pipe:0': '-f amr'},
            outputs={'pipe:1': '-f wav'}
    )
    wav, stderr = ff.run(input_data=data, stdout=subprocess.PIPE)

    res = requests.post(
        'https://asr.yandex.net/asr_xml',
        params = {
            'uuid': str(uuid.uuid4()).replace('-', ''),
            'topic': 'queries',
            'key': os.environ['YANDEX_SPEECHKIT_KEY'],
        }, headers = {
            'Content-Type': 'audio/x-wav'
        }, data = wav)

    text = 'Message from watch'
    try:
        r = ET.fromstring(res.text.encode('utf-8'))
        if r.attrib['success'] == '1':
            text = max(list(r), key=lambda x: float(x.attrib['confidence'])).text
    except:
        log.exception(res.text)

    utils.bot.sendVoice(chat_id, StringIO.StringIO(opus), caption=text)

def on_new_location(chat_id, msg):
    dynamo = utils.DynamoHelper()

    ld = msg.location_data

    settings = dynamo.get_settings(msg.identifier['device_id'])

    if not proto.message.tstate(settings['tstate']['S']).on_wrist and ld.on_wrist:
        utils.bot.sendMessage(chat_id, 'alarm: watches are put on')

    dynamo.update_settings(
        msg.identifier['device_id'],
        "SET loc = :loc, batt = :batt, ld_ts = :ld_ts, ld_ts_%s = :ld_ts, gsm_info = :gsm_info, tstate = :tstate"%(ld.position[0],),
        {
            ":loc": {'M': {"lat": {'N': ld.lat}, "lon": {'N': ld.lon}}}, 
            ":batt": {'N': ld.batt},
            ":ld_ts": {'N': str(ld.ts_num)},
            ":gsm_info": {'S': ','.join(ld.gsm_info)},
            ":tstate": {'S': ld.tstate},
        },
    )

def on_alarm(chat_id, msg):
    utils.bot.sendMessage(
        chat_id, 'alarm: %s'%msg.location_data.alarm,
    )
    utils.bot.sendLocation(chat_id, msg.location_data.lat, msg.location_data.lon)

def handler(event, context):
    for msg in event['Records']:
        msg = json.loads(msg['Sns']['Message'])

        dynamo = utils.DynamoHelper()

        settings = dynamo.get_settings(msg['id']['device_id'])
        if not settings or not settings.get('chat_id'): continue
        chat_id = settings.get('chat_id')['S']

        w_msg = dynamo.get_object(msg['id']['device_id'], msg['id']['ts'])

        if not w_msg:
            raise RuntimeError, 'No record found: %s'%msg

        m = proto.message.message.from_dynamo(w_msg)

        ld = m.location_data
        if ld:
            log.debug('setting new location: %s, %s; bat=%s'%(ld.lat, ld.lon, ld.batt))
            if m.cmd == 'AL':
                on_alarm(chat_id, m)
            on_new_location(chat_id, m)
        elif m.cmd == 'LK':
            log.debug('confirm online status')
        elif m.cmd in ('TKQ2', 'TKQ'):
            log.debug('some strange statuses: %s'%m.to_json())
        elif m.cmd == 'CR':
            utils.bot.sendMessage(
                chat_id, 'pong' if m.direction == 'watch' else 'ping',
                reply_to_message_id=find_reply_message(w_msg)
            )
        elif m.cmd == 'MESSAGE':
            if m.direction == 'watch':
                utils.bot.sendMessage(
                    chat_id, 'received',
                    reply_to_message_id=find_reply_message(w_msg)
                )
        elif m.cmd == 'FLOWER':
            if m.direction == 'watch':
                utils.bot.sendMessage(
                    chat_id, 'set',
                    reply_to_message_id=find_reply_message(w_msg)
                )
        elif m.cmd in ('TK', 'TK2'):
            on_voice_message(chat_id, m)
        else:
            utils.bot.sendMessage(
                chat_id, 'event: \ndirection=%s\ncmd=%s\nparams=%s\nid=%s'%(m.direction, m.cmd, repr(m.params[:100]) if m.params else m.params, m.identifier),
            )

