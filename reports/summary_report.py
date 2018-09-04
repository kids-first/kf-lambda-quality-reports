from datetime import datetime, timedelta
import hvac
import re
import os
import glob
import pandas as pd
import numpy as np
import boto3
from botocore.vendored import requests
from collections import defaultdict

from jinja2 import Environment, FileSystemLoader

TABLES = [
    'study',
    'participant',
    'biospecimen',
    'phenotype',
    'diagnosis',
    'genomic_file'
]

IGNORE_COLS = ['uuid', 'created_at', 'modified_at', 'kf_id']


def get_pg_connection_str():
    """
    Helper function to load database creds from vault
    """
    if 'CONN_STR' in os.enviorn:
        return os.environ['CONN_STR']
    env = os.environ.get('ENV')
    vault_url = os.environ.get('VAULT_URL')
    path = os.environ.get('DATASERVICE_PG_SECRET')

    session = boto3.Session()
    client = hvac.Client(url=vault_url)
    credentials = session.get_credentials()
    r = client.auth_aws_iam(credentials.access_key,
                            credentials.secret_key,
                            credentials.token)
    secret = client.read(path)['data']

    usr = secret['username']
    pas = secret['password']
    host = secret['hostname']
    return f"postgres://{usr}:{pas}@{host}:5432/kfpostgres{env}"


def handler(event, context):
    """
    Compile a summary report of table and column counts
    """
    study_id = event.get('study_id')
    output = event.get('output')
    conn_str = get_pg_connection_str()

    g = SummaryGenerator(output=f"/tmp/", s3output=output, conn_str=conn_str)

    # Collect all files in /tmp for upload
    files = set(glob.glob('/tmp/**/*.png', recursive=True))
    files = files.union(set(glob.glob('/tmp/**/*.csv', recursive=True)))
    files = files.union(set(glob.glob('/tmp/**/*.html', recursive=True)))

    return [], {p.replace('/tmp/', ''): p for p in list(files)}


class SummaryGenerator:

    def __init__(self, study_id=None, output='', conn_str=''):
        """
        :param study_id: The kf_id of the study to generate a summary for. If
            `None`, the report will be run for all data.
        :param output: The path to save results to. If run for a specific
            study, results will be saved within a directory named by the kf_id
            of that study in this path.
        :param conn_str: The sql connection string for the database
        """
        self.study_id = study_id
        self.conn_str = conn_str
        self.output = output
        if self.study_id is not None:
            self.output += self.study_id+'/'

        os.makedirs(self.output+'tables', exist_ok=True)

    def make_report(self):
        """
        Compile summaries for all tables and save them
        """
        table_summaries = {}
        for table in TABLES:
            # Read table from postgres
            df = pd.read_sql_table(table, con=self.conn_str)
            table_summaries[table] = table_report(df)

        # Flatten each tables report into one dict
        summaries = {f'{k1}_{k2}': v2 for k1, v1 in table_summaries.items()
                                     for k2, v2 in v1.items()}

        # Save each summary file to csv
        for name, summary in summaries.items():
            summary.to_csv(f'{self.output}tables/{name}.csv')

        # Print report to html
        env = Environment(loader=FileSystemLoader(os.path.dirname(os.path.abspath(__file__))))
        template = env.get_template("summary_template.html")

        template_vars = {
            "title": "Data Summary Report",
            "date": datetime.now(),
            "sections": table_summaries
        }

        # Render and save HTML
        html_out = template.render(template_vars)
        filename = f"Table_Summary_Report_{datetime.now().strftime('%Y%m%d')}.html"
        with open(self.output+filename, "w+") as f:
            f.write(html_out)

        # TODO Call diff report lambda

def table_report(df):
    """
    Summarizes the table by doing highlevel summary stats and compiling
    detailed summaries for columns with categorical values
    Returns a dict keyed by name of the table and a dataframe
    """
    reports = {}
    # Get rid of the id and unique columns
    df = df[list(set(df.columns) - set(IGNORE_COLS))]
    # Compile the highlevel summary
    reports['summary'] = df.describe(include='all', percentiles=[])

    for name, col in reports['summary'].items():
        if not name.endswith('_id') and col['unique']/col['count'] < 0.1:
            reports[name] = column_report(df, name)

    return reports


def column_report(df, col):
    """
    Compute the unique value counts for a given column
    """
    counts = (df[[col]].reset_index()
                       .groupby(col)
                       .count()
                       .sort_values('index', ascending=False)
                       .reset_index()
                       .rename(columns={'index':'count'}))
    return counts


# For local testing
if __name__ == '__main__':
    handler({"name": "Study Report",
            "module": "reports.study_report",
            "output": "kf-reports-us-east-1-env-quality-reports/today/study_reports/SD_XXX_QC_Report/"
            }, {})
