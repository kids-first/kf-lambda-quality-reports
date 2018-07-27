from datetime import datetime
import hvac
import re
import os
import glob
import pandas as pd
import boto3
from botocore.vendored import requests
from collections import defaultdict

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
DIFF_RE = re.compile(r'^.*\(([+-]?\d+)\)$')


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
        os.makedirs(self.output+'diffs', exist_ok=True)

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
        filename = f'Table_Summary_Report.html'
        with open(self.output+filename, "w+") as f:
            f.write(html_out)

        diff = DiffGenerator(output=self.output+'diffs/',
                             today=self.output+'tables/',
                             summaries=table_summaries,
                             yesterday=self.output+'yesterday/')
        diff.make_report()


        # DO NOT save to pdf for now. We don't do any table size checking
        return

        # Render and save PDF
        filename = f'Table_Summary_Report.pdf'
        with open(self.output+filename, "w+b") as out_pdf_file_handle:
            pisa.CreatePDF(
                src=html_out,
                dest=out_pdf_file_handle)

    def compute_diff(self, today, yesterday):
        """
        Compares today to yesterday by subtracting today from yesterday
        """
        return today - yesterday


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


class DiffGenerator:

    def __init__(self, output='/tmp/', today='', yesterday='', summaries=None):
        """
        :param today: A directory path where to find all of today's summaries
        :param yesterday: A directory where to find all of yesterday's summaries
        """
        if not os.path.isdir(today) or not os.path.isdir(yesterday):
            raise Exception(f'Please provide valid directory paths {today} {yesterday}')

        self.output = output
        self.today_path = today
        self.yesterday_path = yesterday
        self.today_files = os.listdir(today)
        self.yesterday_files = os.listdir(yesterday)

        summaries = {f'{k1}_{k2}': v2 for k1, v1 in summaries.items()
                                     for k2, v2 in v1.items()}
        self.summaries = summaries

    def make_report(self):
        diffs = self.compute_diffs()

        env = Environment(loader=FileSystemLoader(os.path.dirname(os.path.abspath(__file__))))
        template = env.get_template("summary_template.html")

        template_vars = {
            "title": "Daily Change Report",
            "date": datetime.now(),
            "sections": diffs
        }
        # Render and save HTML
        html_out = template.render(template_vars)
        filename = f'Table_Diff_Report.html'
        with open(self.output+filename, "w+") as f:
            f.write(html_out)

    def compute_diffs(self):
        """
        Looks through summary tables and computes diffs for each returning a
        dict of styled html tables organized by table and column and leaving
        out any summaries that did not change.
        """

        sections = defaultdict(dict)
        # Iterate through the summaries from the summary report and try to
        # diff against the same summary from yesterday, if it exists
        for f, df1 in self.summaries.items():
            section = f.split('_')[0]
            name = f[f.find('_')+1:].replace('.csv', '')

            # df1 = pd.read_csv(os.path.join(self.today_path, f), index_col=0)
            df2_path = os.path.join(self.yesterday_path, f+'.csv')
            # Don't try to diff if it's a summary that didn't exist yesterday
            if not os.path.exists(df2_path):
                continue
            df2 = pd.read_csv(df2_path, index_col=0)
            sections[section][name] = self.compute_diff(df1, df2)
            sections[section][name].to_csv(f'{self.output}{section}_{name}_diff.csv')
            # Don't try to style an empty diff and just return None
            if sections[section][name].shape[0] == 0:
                sections[section][name] = None
                continue
            sections[section][name] = sections[section][name].style.applymap(self.highlight_diff).set_table_attributes('class="table table-striped table-hover"')

        return sections

    def compute_diff(self, df1, df2):
        """
        Compares two dataframes
        """
        df = pd.concat([df1, df2], 
                        axis='columns',
                        keys=['DF1', 'DF2'])

        diff = pd.DataFrame(columns=set(df.columns.get_level_values(1)))

        for c in set(df.columns.get_level_values(1)):
            if c not in df['DF1'].columns:
                diff[c] = df['DF2'][c]
            elif c not in df['DF2'].columns:
                diff[c] = df['DF1'][c]
            else:
                diff[c] = df.xs(c, level=1, drop_level=1, axis=1) \
                            .apply(self.differ, axis=1)

        if 'count' in diff.columns:
            diff = diff[~diff['count'].str.endswith('(+0)')]
            diff = diff[list(set(diff.columns) - {'count'}) + ['count']]
            
        return diff

    def highlight_diff(self, value):
        if value is None:
            return ''
        
        try:
            m = DIFF_RE.match(str(value))
            if m:
                value = int(str(m.groups(0)[0]))
            else:
                return ''

            if value < 0:
                return 'color: #dc3545;'
            elif value > 0:
                return 'color: #28a745;'
            else:
                return ''
        except ValueError as verr:
            return ''
        return ''

    def differ(self, r):
        """
        Compares two values and returns the new value
        """
        first = r['DF1']
        second = r['DF2']
        try:
            first = int(str(first))
            second = int(str(second))
        except ValueError as verr:
            if str(first) != str(second):
                return f'<span class="text-success">{first}</span> <span class="text-danger">(-{second})</span>'
            else:
                return first
        return f'{first} ({(first - second):+})'



# For local testing
if __name__ == '__main__':
    handler({"name": "Study Report",
            "module": "reports.study_report",
            "output": "kf-reports-us-east-1-env-quality-reports/today/study_reports/SD_XXX_QC_Report/"
            }, {})
