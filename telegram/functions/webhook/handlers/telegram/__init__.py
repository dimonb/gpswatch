# -- coding: utf-8 --


from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton

import os
import json
import telepot
import logging
import binascii
import struct
import arrow
import proto.message
from .. import utils

log = logging.getLogger('webhook.handlers.telegram')

def on_register_watch_command(chat_id, message):
    cmds = message['text'].split()
    if len(cmds) not in (2, 4):
        utils.bot.sendMessage(
             chat_id, 
             '''To register watches you should do next steps:

1. send SMS to watches `pw,123456,ts#`, wait for answer and remember `ip_url` and `port` configuration. 
2. send SMS to change server: `pw,123456,ip,52.16.32.101,8001#`, wait answer from watches
3. watches will receive text message with `secret code` after several minutes. This code allow to bind telegram account with device id
4. send this code to telegram bot in format: `/register_watch <secret code> <ip_url> <port>` to bind telegram account and configure settings
5. profit
''', 
            parse_mode='Markdown',
            reply_to_message_id=message['message_id']
        )
    else:
        dynamo = utils.DynamoHelper()
        res = dynamo.get_settings('SEC_%s'%cmds[1])
        if not res:
            utils.bot.sendMessage(
                chat_id, 'Invalid secret',
                reply_to_message_id=message['message_id']
            )
        else:
            if len(cmds) > 2: #update host and port
                dynamo.update_settings(
                    res['ref_device_id']['S'], 
                    "SET host = :host, port = :port",
                    {":host": {'S': cmds[2]}, ":port": {'S': cmds[3]}},
                )

            dynamo.update_settings(
                'TEL_%s'%message['from']['id'],
                "ADD watches_device_id :wd",
                {":wd": {'SS': [res['ref_device_id']['S']]},}
            )

            utils.bot.sendMessage(
                chat_id, 'Profit',
                reply_to_message_id=message['message_id']
            )

def on_set_active_chat_command(chat_id, message):
    dynamo = utils.DynamoHelper()
    devices = dynamo.active_devices(message['from']['id'])
    
    cmds = message['text'].split()
    if len(cmds) > 1:
        device = cmds[1]
        if device not in devices:
            utils.bot.sendMessage(
                chat_id, 'Invalid device: %s'%device,
                reply_to_message_id=message['message_id']
            )
            return
    else:
        if len(devices) > 1:
            utils.bot.sendMessage(
                chat_id, 'Select one of devices: %s'%','.join(devices),
                reply_to_message_id=message['message_id']
            )
            return
        device = devices[0]

    dynamo.update_settings(
        device, 
        "SET chat_id = :chat_id",
        {":chat_id": {'S': str(chat_id)}}
    )

    utils.bot.sendMessage(
        chat_id, 'This chat will be default for device %s from now'%device,
        reply_to_message_id=message['message_id']
    )
        

def on_list_watches_command(chat_id, msg):
    dynamo = utils.DynamoHelper()
    devices = dynamo.active_devices(msg['from']['id'])

    if not devices:
        raise ReplyException('You have no active devices')
    else:
        utils.bot.sendMessage(
            chat_id, 'Active devices: %s'% ','.join(devices),
            reply_to_message_id=msg['message_id']
        )


def on_track_command(chat_id, msg):
    dynamo = utils.DynamoHelper()
    devices = dynamo.active_devices(msg['from']['id'])

    cmds = msg['text'].split()
    if len(cmds) > 1:
        ndays = int(cmds[1])
    else:
        ndays = 1

    for device in devices:
        while True:
            secret = struct.unpack('Q', os.urandom(8))[0]
            if not dynamo.get_settings('GPX_%s'%secret):
                break

        dynamo.update_settings(
            'GPX_%s'%secret, 
            "SET device = :device, f = :f, t = :t",
            {
                ":device": {'S': device}, 
                ":f": {'N': str(arrow.utcnow().replace(days=-ndays).timestamp)},
                ":t": {'N': str(arrow.utcnow().timestamp)},
            },
        )

        '''map_keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text='Map', url='https://1yzbcp1ac5.execute-api.eu-west-1.amazonaws.com/p/p/%s.html'%secret),
                    ]])'''

        utils.bot.sendMessage(
            chat_id, 'https://tracker.dimonb.com/%s.html'%secret,
            reply_to_message_id=msg['message_id'], 
        )

def parse_command(msg, nargs, err1, err2):
    cmds = msg['text'].split()
    if cmds[0][0] != '/':
        cmds.insert(0, '/cc')

    dynamo = utils.DynamoHelper()
    devices = dynamo.active_devices(msg['from']['id'])

    if len(devices) > 1:
        if len(cmds) < nargs+2  or cmds[1] not in devices:
            raise ReplyException(err2)

        device = cmds[1]
        params = cmds[2:]
    elif len(cmds) < nargs+1:
        raise ReplyException(err1)
    else:
        device = devices[0]
        params = cmds[1:]

    company, device = device.split('_')

    return company, device, params


def on_msg_command(chat_id, msg):
    dev, did, params = parse_command(
            msg, 1, 
            'Command format is `/msg message`', 
            'Command format is `/msg device_id message`'
        )

    payload = json.dumps({'chat_id': chat_id, 'message_id': msg['message_id']})
    m = proto.message.message.from_command(dev, did, 'MESSAGE', binascii.hexlify(' '.join(params).encode('utf-16-be')), direction='server_t', payload=payload) 

    utils.DynamoHelper().send_message(m)
    

def on_flower_command(chat_id, msg):
    dev, did, nflowers = parse_command(
            msg, 1, 
            'Command format is `/flower N` where N is number of flowers', 
            'Command format is `/flower device_id N` where N is number of flowers and device is you device\_id'
        )

    payload = json.dumps({'chat_id': chat_id, 'message_id': msg['message_id']})
    m = proto.message.message.from_command(dev, did, 'FLOWER', nflowers[0], direction='server_t', payload=payload) 

    utils.DynamoHelper().send_message(m)

def on_monitor_command(chat_id, msg):
    dev, did, phone = parse_command(
            msg, 1, 
            'Command format is `/monitor phone`', 
            'Command format is `/monitor device_id phone`'
        )

    payload = json.dumps({'chat_id': chat_id, 'message_id': msg['message_id']})
    m = proto.message.message.from_command(dev, did, 'MONITOR', phone, direction='server_t', payload=payload) 

    utils.DynamoHelper().send_message(m)

def on_ping_command(chat_id, msg): 
    dev, did, cc = parse_command(
        msg, 0, 'Command format is /ping', 'Command format is /ping device_id'
    )
    payload = json.dumps({'chat_id': chat_id, 'message_id': msg['message_id']})
    m = proto.message.message.from_command(dev, did, 'CR', None, direction='server_t', payload=payload) 
    utils.DynamoHelper().send_message(m)

def on_status_command(chat_id, msg): 
    dev, did, cc = parse_command(
        msg, 0, 'Command format is /status', 'Command format is /status device_id'
    )
    settings = utils.DynamoHelper().get_settings('%s_%s'%(dev, did))

    from telepot.namedtuple import InlineKeyboardMarkup, InlineKeyboardButton

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                       InlineKeyboardButton(text='GPS Position', callback_data='GPS/%s'%settings['device_id']['S']),
                       InlineKeyboardButton(text='GSM Position', callback_data='GSM/%s'%settings['device_id']['S']),
                   ]])

    utils.bot.sendMessage(
        chat_id, 
        'last online: %s\nlast gps: %s\non wrist: %s\nbattery: %s'%(
            arrow.get(settings['ld_ts']['N']).to('Europe/Moscow').humanize(),
            arrow.get(settings['ld_ts_A']['N']).to('Europe/Moscow').humanize() if 'ld_ts_A' in settings else 'unknown',
            'no' if struct.unpack('>L', binascii.unhexlify(settings['tstate']['S']))[0] & (1 << 3) else 'yes', 
            settings['batt']['N'],
        ),
        reply_to_message_id=msg['message_id'], reply_markup=keyboard,
    )


class ReplyException(Exception): pass

def handler(event, context):  
    message = event['message']
    content_type, chat_type, chat_id = telepot.glance(message)
    log.debug('content_type: %s', content_type)

    if content_type != 'text':
        return

    command = message['text'].split()[0].split('@')[0]

    try:
        if command == '/register_watch':
            on_register_watch_command(chat_id, message)
        elif command == '/set_active_chat':
            on_set_active_chat_command(chat_id, message)
        elif command == '/list_watches':
            on_list_watches_command(chat_id, message)
        elif command == '/track':
            on_track_command(chat_id, message)
        elif command == '/flower':
            on_flower_command(chat_id, message)
        elif command == '/monitor':
            on_monitor_command(chat_id, message)
        elif command == '/msg':
            on_msg_command(chat_id, message)
        elif command == '/status':
            on_status_command(chat_id, message)
        elif command == '/ping':
            on_ping_command(chat_id, message)
        else:
            on_msg_command(chat_id, message)

    except ReplyException as ex:
        utils.bot.sendMessage(
            chat_id, str(ex),
            reply_to_message_id=message['message_id'],
            parse_mode='Markdown',
        )

def callback_query(event, context):
    cq = event['callback_query']
    query_id, from_id, query_data = telepot.glance(cq, flavor='callback_query')

    tp, param = cq['data'].split('/', 2)

    settings =  utils.DynamoHelper().get_settings(param)
    if tp == 'GPS':
        pos = settings['loc']['M']
        utils.bot.sendLocation(cq['message']['chat']['id'], pos['lat']['N'], pos['lon']['N'], reply_to_message_id=cq['message']['message_id'])
    if tp == 'GSM':
        pos = proto.message.ld.gsm_location_from_info(settings['gsm_info']['S'].split(','))
        utils.bot.sendLocation(cq['message']['chat']['id'], pos['location']['lat'], pos['location']['lng'], reply_to_message_id=cq['message']['message_id'])

    utils.bot.answerCallbackQuery(query_id, text='Ok')

