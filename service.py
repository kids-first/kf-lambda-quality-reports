import os
import time
from base64 import b64decode
import boto3
from botocore.vendored import requests


def handler(event, context):
    """
    Try to resolve the module from the event, then import and run
    """
    if 'SLACK_SECRET' in os.environ and 'SLACK_CHANNEL' in os.environ:
        kms = boto3.client('kms', region_name='us-east-1')
        SLACK_SECRET = os.environ.get('SLACK_SECRET', None)
        SLACK_TOKEN = kms.decrypt(CiphertextBlob=b64decode(SLACK_SECRET)).get('Plaintext', None).decode('utf-8')
        SLACK_CHANNEL = os.environ.get('SLACK_CHANNEL', '').split(',')
        SLACK_CHANNEL = [c.replace('#','').replace('@','') for c in SLACK_CHANNEL]

    t0 = time.time()
    package = event.get('module', None)
    if package is None:
        return

    sub = package.split('.')[-1]

    module = __import__(package, globals(), locals(), [sub], 0)
    print('calling', module)

    attachments = [
        {
            "fallback": ":white_check_mark: Finished report",
            "text": ":white_check_mark: Finished report",
            "fields": [
                {
                    "title": "Name",
                    "value": event['name'].capitalize(),
                    "short": True
                },
                {
                    "title": ":clock1: Time Taken",
                    "value": "{:.2f}s".format(time.time() - t0),
                    "short": True
                }
            ],
            "color": "good"
        }
    ]

    at, files = module.handler(event, context)

    attachments.append(at)

    # Send slack notification
    if SLACK_TOKEN is not None:
        for channel in SLACK_CHANNEL:
            message = {
                'username': 'Report Bot',
                'icon_emoji': ':bar_chart:',
                'channel': channel,
                'attachments': attachments
            }

            resp = requests.post('https://slack.com/api/chat.postMessage',
                headers={'Authorization': 'Bearer '+SLACK_TOKEN},
                json=message)

        # Upload files
        for name, path in files.items():
            print('uploading', name, path)
            r = requests.post('https://slack.com/api/files.upload',
                              headers={'Authorization': 'Bearer '+SLACK_TOKEN},
                              params={'channels': SLACK_CHANNEL,
                                      'title': name,
                                      'icon_emoji': ':bar_chart:',
                                      'username': 'Report Bot'},
                              files={'file': (path, open(path, 'rb'))})


if __name__ == '__main__':
    handler({'name': 'counts',
             'module': 'reports.counts',
             'output': 'kf-reports-us-east-1-env-quality-reports/today/counts/'}, {})
