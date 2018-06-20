import hvac
import csv
import gzip
import os
import matplotlib
matplotlib.use('PS')
import matplotlib.pyplot as plt
import boto3
import psycopg2
from collections import defaultdict


def handler(event, context):
    env = os.environ.get('ENV')
    vault_url = os.environ.get('VAULT_URL')
    path = os.environ.get('DATASERVICE_PG_SECRET')

    client = hvac.Client(url=vault_url)

    session = boto3.Session()
    credentials = session.get_credentials()
    r = client.auth_aws_iam(credentials.access_key,
                            credentials.secret_key,
                            credentials.token)
    secret = client.read(path)['data']

    conn = psycopg2.connect(
            host=secret['hostname'],
            dbname='kfpostgres'+env,
            user=secret['username'],
            password=secret['password'])

    cur = conn.cursor()

    cur.execute("""
    SELECT phenotype.source_text_phenotype, count(phenotype.source_text_phenotype), study.kf_id AS study_id FROM phenotype, participant, study
    WHERE phenotype.participant_id = participant.kf_id and participant.study_id = study.kf_id
    GROUP BY phenotype.source_text_phenotype,study.kf_id
    HAVING count(phenotype.source_text_phenotype) > 100
    ORDER BY count(phenotype.source_text_phenotype);
    """)
    data = cur.fetchall()
    cur.close()
    conn.close()

    files = {}
    by_pheno = defaultdict(int)
    by_study = defaultdict(lambda: defaultdict(int))

    path = '/tmp/phenotypes.csv'
    with open(path, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['source text phenotype', 'count', 'study_id'])
        for v in data:
            by_pheno[v[0]] += v[1]
            by_study[v[2]][v[0]] += v[1]
            writer.writerow(v)
    files['Phenotype Data'] = path

    files['All Studies'] = make_plot(by_pheno, 'all studies')
    for study_id, d in by_study.items():
        files[study_id] = make_plot(d, study_id)

    return [], files


def make_plot(d, title):
    plt.figure(figsize=(15, 10))
    ind = list(range(len(d)))
    plt.bar(ind, sorted(list(d.values()), reverse=True), width=0.5)
    plt.xticks(rotation=-50)
    plt.ylabel('count')
    plt.gca().set_xticks(ind)
    plt.gca().set_xticklabels([v[0] for v in sorted(d.items(), key=lambda x: x[1])])
    plt.gca().set_yscale('log')
    #plt.legend(bbox_to_anchor=(1.0, 1.0), ncol=2, loc=7)
    plt.title('Phenotype distribution for ' + title)
    plt.tight_layout()
    path = '/tmp/phenotypes_{}.png'.format(title)
    plt.savefig(path)
    return path


# For local testing
if __name__ == '__main__':
    handler({"name": "phenotypes",
            "module": "reports.phenotypes",
            "output": "kf-reports-us-east-1-env-quality-reports/today/phenotypes/"
            }, {})
