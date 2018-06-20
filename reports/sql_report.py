import os
import csv
import hvac
import boto3
import psycopg2
import psycopg2.extras

def handler(event, context):
    env = os.environ.get('ENV')
    vault_url = os.environ.get('VAULT_URL')
    path = os.environ.get('DATASERVICE_PG_SECRET')

    if 'query_statement' not in event:
        raise Exception('must include a query_statement for sql reports!')

    query = event['query_statement']

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

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(query)

    fpath = '/tmp/report.csv'
    with open(fpath, 'w') as f:
        data = cur.fetchone()
        writer = csv.DictWriter(f, fieldnames=data.keys())
        writer.writeheader()
        writer.writerow(data)
        for line in cur:
            writer.writerow(line)

    cur.close()
    conn.close()
    return [], {'Report': fpath}


# For local testing
if __name__ == '__main__':
    handler({"name": "phenotypes",
            "module": "reports.phenotypes",
            "output": "kf-reports-us-east-1-env-quality-reports/today/phenotypes/",
            "query_statement": "select s.external_id,s.kf_id, b.dbgap_consent_code, count(b.*) from biospecimen b join participant p on b.participant_id = p.kf_id join study s on p.study_id = s.kf_id group by s.external_id,s.kf_id, b.dbgap_consent_code order by s.kf_id, b.dbgap_consent_code"
            }, {})
