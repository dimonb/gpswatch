# -- coding: utf-8 --

import os
import arrow
import gpxpy.gpx

from .. import utils

TPL = open(os.path.join(os.path.split(__file__)[0], 'route.html')).read()

def handler(event, context):
    track_id = event['params']['path']['track-id']

    sec, tp = track_id.split('.')
    if tp == 'html':
        return {'body': TPL%('https://tracker.dimonb.com/%s.gpx'%sec)}

    dynamo = utils.DynamoHelper()

    s = dynamo.get_settings('GPX_%s'%sec)
    device_id = s['device']['S']
    f = s['f']['N']
    t = s['t']['N']

    gpx = gpxpy.gpx.GPX()
    gpx_track = gpxpy.gpx.GPXTrack()
    gpx.tracks.append(gpx_track)

    gpx_segment = gpxpy.gpx.GPXTrackSegment()
    gpx_track.segments.append(gpx_segment)

    for r in dynamo.query(
                Select= 'SPECIFIC_ATTRIBUTES',
                ProjectionExpression = '#ll',
                KeyConditionExpression = 'device_id = :device_id AND ts BETWEEN :f AND :t',
                FilterExpression = 'begins_with(cmd, :cmd)',
                ExpressionAttributeValues = {
                    ':device_id': {'S': device_id},
                    ':f': {'N': f},
                    ':t': {'N': t},
                    ':cmd': {'S': 'UD'},
                },
                ExpressionAttributeNames = {
                    '#ll': 'location',
                }):
        loc = r['location']['M']
        if loc['type']['S'] == 'A':
            gpx_segment.points.append(gpxpy.gpx.GPXTrackPoint(loc['lat']['N'], loc['lon']['N'], time=arrow.get(loc['ts']['N']).to('Europe/Moscow')))

    return {'body': gpx.to_xml()}

