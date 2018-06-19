import os
import hvac
import boto3
import psycopg2

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
    print('read secret')

    conn = psycopg2.connect(
            host=secret['hostname'],
            dbname='kfpostgres'+env,
            user=secret['username'],
            password=secret['password'])

    cur = conn.cursor()

    cur.execute("""
    SELECT phenotype.source_text_phenotype, count(phenotype.source_text_phenotype), study.kf_id AS study_id FROM phenotype, participant, study
    WHERE phenotype.participant_id = participant.kf_id and participant.study_id = study.kf_id
    GROUP BY phenotype.source_text_phenotype,study.kf_id;
    """)
    print(cur.fetchall())
    cur.close()
    conn.close()
    return [], {}


# For local testing
if __name__ == '__main__':
    handler({"name": "phenotypes",
            "module": "reports.phenotypes",
            "output": "kf-reports-us-east-1-env-quality-reports/today/phenotypes/"
            }, {})
