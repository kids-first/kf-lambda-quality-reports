import os
import traceback
import sys
import time
from base64 import b64decode
import boto3
from botocore.vendored import requests


def handler(event, context):
    """
    Try to resolve the module from the event, then import and run
    """
    t0 = time.time()
    if 'SLACK_SECRET' in os.environ and 'SLACK_CHANNEL' in os.environ:
        kms = boto3.client('kms', region_name='us-east-1')
        SLACK_SECRET = os.environ.get('SLACK_SECRET', None)
        SLACK_TOKEN = kms.decrypt(CiphertextBlob=b64decode(SLACK_SECRET)).get('Plaintext', None).decode('utf-8')
        SLACK_CHANNEL = os.environ.get('SLACK_CHANNEL', '').split(',')
        SLACK_CHANNEL = [c.replace('#','').replace('@','') for c in SLACK_CHANNEL]

    env = event.get('ENV', 'dev')
    package = event.get('module', None)
    if package is None:
        return
    output = event.get('output', None)
    bucket = output.split('/')[0]
    path = '/'.join(output.split('/')[1:])

    sub = package.split('.')[-1]

    module = __import__(package, globals(), locals(), [sub], 0)
    print('calling', module)

    failed = False
    try:
        at, files = module.handler(event, context)
    except Exception as err:
        print(err)
        traceback.print_exc(file=sys.stdout)
        failed = True
        at = []
        files = {}

    report_url = f"https://s3.amazonaws.com/{bucket}/index.html#{path}/"

    report_name = "{} report".format(event['name'].capitalize())

    attachments = []
    if not failed:
        attachments.append({
            "fallback": ":white_check_mark: " + report_name + " completed",
            "title": ":white_check_mark: " + report_name + " completed",
            "title_link": report_url,
            "color": "good"
        })

        attachments.append({
            "fallback": ":memo: Report summar",
            "fields": [
                {
                    "title": ":open_file_folder: Files Available",
                    "value": len(files),
                    "short": True
                },
                {
                    "title": ":clock1: Time Taken",
                    "value": "{:.2f}s".format(time.time() - t0),
                    "short": True
                }
            ],
            "color": "good"
        })
        attachments.extend(at)
    else:
        attachments = [
            {
                "fallback": ":x: I'm very sorry, but  " + report_name + " failed :(",
                "title": ":x: I'm very sorry, but " + report_name + " failed :(",
                "color": "danger"
            }
        ]

    # Upload files to s3
    for name, path in files.items():
        upload_to_s3(path, output)

    return

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

        # Upload files to slack
        for name, path in files.items():
            break
            upload_to_slack(name, path, SLACK_TOKEN, SLACK_CHANNEL)


def upload_to_slack(name, path, SLACK_TOKEN, SLACK_CHANNEL):
    r = requests.post('https://slack.com/api/files.upload',
                      headers={'Authorization': 'Bearer '+SLACK_TOKEN},
                      params={'channels': SLACK_CHANNEL,
                              'title': name,
                              'icon_emoji': ':bar_chart:',
                              'username': 'Report Bot'},
                      files={'file': (path, open(path, 'rb'))})


def upload_to_s3(path, output):
    bucket = output.split('/')[0]
    key = '/'.join(output.split('/')[1:] + ['/'.join(path.split('/')[1:])])
    content = None

    if key.endswith('gz'):
        content = None
    elif key.endswith('json'):
        content = 'application/json'
    elif key.endswith('png'):
        content = 'image/png'
    elif key.endswith('jpg') or key.endswith('.jpeg'):
        content = 'image/jpeg'
    elif key.endswith('csv'):
        content = 'text/csv'
    if key.endswith('pdf'):
        content = 'application/pdf'
    else:
        content = 'text/plain'

    args = None
    if content:
        args = {'ContentType': content}

    s3 = boto3.client('s3')
    s3.upload_file(path, Bucket=bucket, Key=key,
                   ExtraArgs=args)



if __name__ == '__main__':
    handler({'name': 'counts',
             'module': 'reports.counts',
             'output': 'kf-reports-us-east-1-env-quality-reports/today/counts/'}, {})
