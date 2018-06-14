import os
import datetime
import json
from base64 import b64decode
import boto3
from botocore.vendored import requests


def handler(event, context):
    """
    Invokes the main function for each report module
    """
    with open('config.json') as f:
        config = json.load(f)

    reports = config['reports']

    function = os.environ.get('FUNCTION', None)
    env = os.environ.get('ENV', None)

    day = datetime.datetime.now().strftime('%Y%m%d')
    bucket = 'kf-reports-us-east-1-{}-quality-reports'.format(env)
    output = '{}/{}-reports'.format(bucket, day)

    lam = boto3.client('lambda')
    
    for report in reports:
        report_output = '{}/{}'.format(output, report['name'])
        report['output'] = report_output
        response = lam.invoke(
            FunctionName=function,
            InvocationType='Event',
            Payload=str.encode(json.dumps(report)),
        )
        print('invoked report {}'.format(report['name']))
        print('output to {}'.format(report['output']))

    # Send slack message
    if 'SLACK_SECRET' in os.environ and 'SLACK_CHANNEL' in os.environ:
        kms = boto3.client('kms', region_name='us-east-1')
        SLACK_SECRET = os.environ.get('SLACK_SECRET', None)
        SLACK_TOKEN = kms.decrypt(CiphertextBlob=b64decode(SLACK_SECRET)).get('Plaintext', None).decode('utf-8')
        SLACK_CHANNEL = os.environ.get('SLACK_CHANNEL', '').split(',')
        SLACK_CHANNEL = [c.replace('#','').replace('@','') for c in SLACK_CHANNEL]

        for channel in SLACK_CHANNEL:
            bucket = output.split('/')[0]
            path = '/'.join(output.split('/')[1:])
            report_url = f"https://s3.amazonaws.com/{bucket}/index.html#{path}/"
            attachments = [
                {
                    "fallback": ":runner: Running {} reports".format(len(reports)),
                    "title": ":runner: Running {} reports".format(len(reports)),
                    "color": "good"
                },
                {
                    "fallback": "Report will be available at <{}|{}>".format(report_url, report_url),
                    "text": "Report will be available at <{}|{}>".format(report_url, report_url),
                    "color": "good"
                }
            ]
            message = {
                'username': 'Report Bot',
                'icon_emoji': ':bar_chart:',
                'channel': channel,
                'attachments': attachments
            }

            resp = requests.post('https://slack.com/api/chat.postMessage',
                headers={'Authorization': 'Bearer '+SLACK_TOKEN},
                json=message)

