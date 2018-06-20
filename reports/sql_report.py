import os
import csv
import hvac
import boto3
import psycopg2

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
    print('read secret')

    conn = psycopg2.connect(
            host=secret['hostname'],
            dbname='kfpostgres'+env,
            user=secret['username'],
            password=secret['password'])

    cur = conn.cursor()
    cur.execute(query)

    path = '/tmp/report.csv'
    with open(path, 'w') as f:
        writer = csv.writer(f)
        for line in cur.fetchall():
            writer.writerow(line)

    cur.close()
    conn.close()
    return [], {'Report': path}


# For local testing
if __name__ == '__main__':
    handler({"name": "phenotypes",
            "module": "reports.phenotypes",
            "output": "kf-reports-us-east-1-env-quality-reports/today/phenotypes/"
            }, {})
