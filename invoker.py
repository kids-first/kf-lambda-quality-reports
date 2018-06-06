import os
import datetime
import json
import boto3
from botocore.vendored import requests


def handler(event, context):
    """
    Invokes the main function for each report module
    """
    reports = [
        {'name': 'counts', 'module': 'reports.counts'},
    ]

    function = os.environ.get('FUNCTION', None)
    env = os.environ.get('ENV', None)

    day = datetime.datetime.now().strftime('%Y%m%d')
    bucket = 'kf-reports-us-east-1-{}-quality-reports'.format(env)
    output = '{}/{}-reports'.format(bucket, day)

    lam = boto3.client('lambda')
    
    for report in reports:
        output = '{}/{}'.format(output, report['name'])
        report['output'] = output
        response = lam.invoke(
            FunctionName=function,
            InvocationType='Event',
            Payload=str.encode(json.dumps(report)),
        )
