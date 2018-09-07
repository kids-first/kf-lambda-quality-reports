from datetime import datetime, timedelta
import hvac
import re
import os
import glob
import json
import pandas as pd
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
    if 'CONN_STR' in os.environ:
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
    change_report = event.get('change_report', False)
    conn_str = get_pg_connection_str()

    g = SummaryGenerator(output=f"/tmp/", conn_str=conn_str)
    g.make_report()

    # Collect all files in /tmp for upload
    files = set(glob.glob('/tmp/**/*.png', recursive=True))
    files = files.union(set(glob.glob('/tmp/**/*.csv', recursive=True)))
    files = files.union(set(glob.glob('/tmp/**/*.html', recursive=True)))


    if change_report:
        call_daily_change_report(context.function_name, output)

    return [], {p.replace('/tmp/', ''): p for p in list(files)}


def call_daily_change_report(function, output):
    # Call the ChangeReport lambda
    bucket = output.replace('s3://', '').split('/')[0]
    # Will upload change report back to the same directory as this report
    prefix = 's3://' + '/'.join(output.replace('s3://', '').split('/')[1:])

    date = datetime.strptime(prefix.split('/')[-2].replace('-reports', ''),
                             '%Y%m%d')
    yesterday = date - timedelta(days=1)
    yesterday_path = output.replace(date.strftime('%Y%m%d'),
                                    yesterday.strftime('%Y%m%d'))

    payload = {
        "name": "Change Report",
        "module": "reports.change_report",
        "summary_path_1": yesterday_path+'/summaries',
        "summary_path_2": output+'/summaries',
        "output": output,
        "title": "Daily Change Report",
        "subtitle": f"{yesterday.strftime('%Y%m%d')} to " +
                    f"{date.strftime('%Y%m%d')}",
    }

    print('Invoke change report:', json.dumps(payload))

    client = boto3.client('lambda')
    client.invoke(
        FunctionName=function,
        InvocationType='Event',
        Payload=str.encode(json.dumps(payload))
    )


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

        os.makedirs(os.path.join(self.output, 'summaries'), exist_ok=True)

    def make_report(self):
        """
        Compile summaries for all tables and save them
        """
        table_summaries = {}
        for table in TABLES:
            # Read table from postgres
            df = pd.read_sql_table(table, con=self.conn_str)
            table_summaries[table] = table_report(df)

        # Save each summary file to csv
        for table_name, table in table_summaries.items():
            os.makedirs(os.path.join(self.output,
                                     'summaries',
                                     table_name),
                        exist_ok=True)
            for col_name, column in table.items():
                path = os.path.join(self.output,
                                    'summaries',
                                    table_name,
                                    f'{col_name}.csv')
                column.to_csv(path)

        # Print report to html
        env = Environment(loader=FileSystemLoader(os.path.dirname(os.path.abspath(__file__))))
        template = env.get_template("summary_template.html")

        template_vars = {
            "title": "Data Summary Report",
            "date": datetime.now(),
            "counts": None,
            "sections": table_summaries
        }

        # Render and save HTML
        html_out = template.render(template_vars)
        filename = f"Table_Summary_Report_{datetime.now().strftime('%Y%m%d')}.html"
        with open(self.output+filename, "w+") as f:
            f.write(html_out)


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
        if not name.endswith('_id'):
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
