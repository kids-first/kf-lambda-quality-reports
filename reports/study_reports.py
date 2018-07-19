import os
import json
import boto3
from botocore.vendored import requests


def handler(event, context):
    """
    Re-invoke a lambda for every study in the dataservice
    """
    # Call dataservice to get study list
    api = os.environ.get('DATASERVICE', None)
    resp = requests.get(api+'/studies?limit=100')
    studies = [s['kf_id'] for s in resp.json()['results']]

    # Get s3 location where this report is to be saved
    output = event.get( 'output')

    lam = boto3.client('lambda')

    for study_id in studies:
        report_output = f"{output}/{study_id}_QC_report"
        report = {
            'name': f"{study_id} QC Report",
            'module': 'reports.study_report',
            'output': report_output,
            'study_id': study_id
        }

        response = lam.invoke(
            FunctionName=context.function_name,
            InvocationType='Event',
            Payload=str.encode(json.dumps(report)),
        )

    return [], {}

# For local testing
if __name__ == '__main__':
    handler({"name": "Study Report",
            "module": "reports.study_reports",
            "output": "kf-reports-us-east-1-env-quality-reports/today/study_reports/"
            }, {})
