from datetime import datetime
import hvac
import os
import glob
import pandas as pd
import boto3
from botocore.vendored import requests

from xhtml2pdf import pisa
from jinja2 import Environment, FileSystemLoader


def get_pg_connection_str():
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

TABLES = [
    'study',
    'participant',
    'biospecimen',
    'phenotype',
    'diagnosis',
    'genomic_file'
]
IGNORE_COLS = ['uuid', 'created_at', 'modified_at', 'kf_id']


def handler(event, context):
    """
    Compile a summary report of table and column counts
    """
    study_id = event.get('study_id')
    output = event.get('output')
    conn_str = get_pg_connection_str()

    g = SummaryGenerator(output=f"/tmp/", conn_str=conn_str)
    g.make_report()

    # Collect all files in /tmp
    files = set(glob.glob('/tmp/**/*.png', recursive=True))
    files = files.union(set(glob.glob('/tmp/**/*.csv', recursive=True)))
    files = files.union(set(glob.glob('/tmp/**/*.pdf', recursive=True)))
    files = files.union(set(glob.glob('/tmp/**/*.html', recursive=True)))

    return [], {p.replace('/tmp/', ''): p for p in list(files)}


class SummaryGenerator:

    def __init__(self, study_id=None, output='', conn_str=''):
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

        # Print report to pdf
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template("reports/summary_template.html")

        template_vars = {
            "title": "Data Summary Report",
            "date": datetime.now(),
            "sections": table_summaries
        }
        # Render and save HTML
        html_out = template.render(template_vars)
        filename = f'Table_Summary_Report.html'
        with open(self.output+filename, "w+") as f:
            f.write(html_out)

        # DO NOT save to pdf for now. We don't do any table size checking
        return

        # Render and save PDF
        filename = f'Table_Summary_Report.pdf'
        with open(self.output+filename, "w+b") as out_pdf_file_handle:
            pisa.CreatePDF(
                src=html_out,
                dest=out_pdf_file_handle)


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
