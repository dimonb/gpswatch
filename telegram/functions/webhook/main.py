# -- coding: utf-8 --


import logging
import handlers


log = logging.getLogger('webhook.main')


def handler(event, context):
    log.debug('event: %s', event)
    log.debug('context: %s', context)
    
    if 'message' in event:
        return handlers.telegram.handler(event, context)

    if 'callback_query' in event:
        return handlers.telegram.callback_query(event, context)

    if 'Records' in event:
        return handlers.sns.handler(event, context)

    if 'params' in event:
        return handlers.http.handler(event, context)

logging.getLogger('botocore').setLevel(logging.INFO)
