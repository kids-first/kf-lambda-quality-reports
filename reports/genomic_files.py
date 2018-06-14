import csv
import gzip
import os
import matplotlib
matplotlib.use('PS')
import matplotlib.pyplot as plt
import boto3
from botocore.vendored import requests


def handler(event, context):
    """ Get counts for each type of genomic file """
    api = os.environ.get('DATASERVICE', None)
    output = event.get('output')
    if api is None:
        return

    endpoint = '/genomic-files'

    data_types = ['Aligned Reads',
                  'Aligned Reads Index',
                  'Unaligned Reads',
                  'Simple Nucleotide Variation',
                  'Variant Calls',
                  'Variant Calls Index',
                  'gVCF',
                  'gVCF Index',
                  'Other']

    # Get study kf_ids
    resp = requests.get(api+'/studies?limit=100')
    studies = [s['kf_id'] for s in resp.json()['results']]

    counts = by_study(api, endpoint, studies, data_types)

    with open('/tmp/datatypes.csv.gz', 'wb') as csv_file:
        with gzip.open(csv_file, 'wt') as gz:
            writer = csv.writer(gz)
            writer.writerow(['study_id']+data_types)
            for key, value in counts.items():
                writer.writerow([key]+value)

    # Plotting
    plt.figure(figsize=(15, 10))
    ind = list(range(len(data_types)))
    for k, v in counts.items():
        plt.bar(ind, v, width=0.5, label=k)

    plt.xticks(rotation=-50)
    plt.ylabel('count')
    plt.gca().set_xticks(ind)
    plt.gca().set_xticklabels(data_types)
    plt.legend(bbox_to_anchor=(1.0, 1.0), ncol=2, loc=7)
    plt.title('Genomic File Data Type Distribution by Study')
    plt.tight_layout()

    plt.savefig('/tmp/gf_data_types_by_study.png')
    plt.savefig('/tmp/gf_data_types_by_study_slack.png', dpi=20)

    attachments = []

    files = {
        'Genomic File Data Types by Study Breakdown': '/tmp/gf_data_types_by_study.png',
        'Genomic File Data Types by Study Data': '/tmp/datatypes.csv.gz'
    }

    return attachments, files


def by_study(api, endpoint, studies, data_types):
    """ Get counts by data type """
    counts_by_study = {}

    for study in studies:
        by_data_type = []
        for data_type in data_types:
            resp = requests.get(api+endpoint+'?limit=1&study_id='+study+'&data_type='+data_type)
            by_data_type.append(resp.json()['total'])
        counts_by_study[study] = by_data_type

    return counts_by_study


# For local testing
if __name__ == '__main__':
    handler({ "name": "genomic files",
            "module": "reports.genomic_files",
            "output": "kf-reports-us-east-1-env-quality-reports/today/genomic_files/"
            }, {})
