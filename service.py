import os
import time
from botocore.vendored import requests


def handler(event, context):
    """
    Try to resolve the module from the event, then import and run
    """
    SLACK_TOKEN = os.environ.get('SLACK_SECRET', None)
    SLACK_CHANNELS = os.environ.get('SLACK_CHANNEL', '').split(',')
    SLACK_CHANNELS = [c.replace('#','').replace('@','') for c in SLACK_CHANNELS]

    t0 = time.time()
    package = event.get('module', None)
    if package is None:
        return

    sub = package.split('.')[-1]

    module = __import__(package, globals(), locals(), [sub], 0)
    print('calling', module)

    attachments = [
        {
            "fallback": "Finished report",
            "text": "Finished report",
            "fields": [
                {
                    "title": "Name",
                    "value": event['name'].capitalize(),
                    "short": true
                },
                {
                    "title": "Time Taken",
                    "value": time.time() - t0,
                    "short": true
                }
            ],
            "color": "good"
        }
    ]
    attachments.extend(module.handler(event, context))

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


if __name__ == '__main__':
    handler({'name': 'counts',
             'module': 'reports.counts',
             'output': 'kf-reports-us-east-1-env-quality-reports/today/counts/'}, {})
