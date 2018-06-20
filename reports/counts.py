import csv
import gzip
import os
import matplotlib
matplotlib.use('PS')
import matplotlib.pyplot as plt
import boto3
from botocore.vendored import requests


def handler(event, context):
    """ Get counts for each type of entity """
    api = os.environ.get('DATASERVICE', None)
    output = event.get('output')
    if api is None:
        return

    # List of entities
    endpoints = [
        '/investigators',
        '/study-files',
        '/families',
        '/family-relationships',
        '/cavatica-apps',
        '/sequencing-centers',
        '/participants',
        '/diagnoses',
        '/phenotypes',
        '/outcomes',
        '/biospecimens',
        '/genomic-files',
        '/sequencing-experiments',
        '/cavatica-tasks',
        '/cavatica-task-genomic-files'
    ]

    # Get study kf_ids
    resp = requests.get(api+'/studies?limit=100')
    studies = [s['kf_id'] for s in resp.json()['results']]

    counts = by_study(api, endpoints, studies)

    with open('/tmp/counts.csv.gz', 'wb') as csv_file:
        with gzip.open(csv_file, 'wt') as gz:
            writer = csv.writer(gz)
            writer.writerow(['study_id']+endpoints)
            for key, value in counts.items():
                writer.writerow([key]+value)

    # Plotting
    plt.figure(figsize=(15, 10))
    ind = list(range(len(endpoints)))
    for k, v in counts.items():
        plt.bar(ind, v, width=0.5, label=k)

    plt.xticks(rotation=-50)
    plt.ylabel('count')
    plt.gca().set_xticks(ind)
    plt.gca().set_xticklabels(endpoints)
    plt.legend(bbox_to_anchor=(1.0, 1.0), ncol=2, loc=7)
    plt.title('Entity Counts by Endpoint and Study')
    plt.tight_layout()

    plt.savefig('/tmp/entity_counts_by_study.png')

    s3_url = 'https://s3.amazonaws.com/' + output

    files = {
        'Entity Counts by Study Breakdown': '/tmp/entity_counts_by_study.png',
        'Entity Counts by Study Data': '/tmp/counts.csv.gz'
    }

    return [], files


def by_study(api, endpoints, studies):
    """ Get counts by endpoint """
    counts_by_study = {}

    for study in studies:
        by_endpoint = []
        for endpoint in endpoints:
            print('get', api+endpoint+'?limit=1&study_id='+study)
            resp = requests.get(api+endpoint+'?limit=1&study_id='+study)
            by_endpoint.append(resp.json()['total'])
        counts_by_study[study] = by_endpoint

    return counts_by_study


# For local testing
if __name__ == '__main__':
    handler({ "name": "counts",
            "module": "reports.counts",
            "output": "kf-reports-us-east-1-env-quality-reports/today/counts/"
            }, {})
