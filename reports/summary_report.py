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
DIFF_RE = re.compile(r'^(.*\()([+-]?\d+(\.\d+)?)\)$')


def handler(event, context):
    """
    Compile a summary report of table and column counts
    """
    study_id = event.get('study_id')
    output = event.get('output')
    conn_str = get_pg_connection_str()

    g = SummaryGenerator(output=f"/tmp/", s3output=output, conn_str=conn_str)
    g.make_report()

    # Collect all files in /tmp
    files = set(glob.glob('/tmp/**/*.png', recursive=True))
    files = files.union(set(glob.glob('/tmp/**/*.csv', recursive=True)))
    files = files.union(set(glob.glob('/tmp/**/*.pdf', recursive=True)))
    files = files.union(set(glob.glob('/tmp/**/*.html', recursive=True)))

    return [], {p.replace('/tmp/', ''): p for p in list(files)}


class SummaryGenerator:

    def __init__(self, study_id=None, output='', s3output='', conn_str=''):
        self.study_id = study_id
        self.conn_str = conn_str
        self.output = output
        self.s3output = s3output
        if self.study_id is not None:
            self.output += self.study_id+'/'

        os.makedirs(self.output+'tables', exist_ok=True)
        os.makedirs(self.output+'yesterday', exist_ok=True)
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
        filename = f"Table_Summary_Report_{datetime.now().strftime('%Y%m%d')}.html"
        with open(self.output+filename, "w+") as f:
            f.write(html_out)

        self.make_diff_report(table_summaries)

        # DO NOT save to pdf for now. We don't do any table size checking
        return

        # Render and save PDF
        filename = f'Table_Summary_Report.pdf'
        with open(self.output+filename, "w+b") as out_pdf_file_handle:
            pisa.CreatePDF(
                src=html_out,
                dest=out_pdf_file_handle)

    def make_diff_report(self, table_summaries):
        """
        Download all of yesterday's summary tables then build the diff report
        Derive yesterdays s3 location based on the output for this report
        """
        m = re.match(r'.*/(\d{8})-reports/.*', self.s3output)
        if not m or not m.groups(1) or len(m.groups(1)) == 0:
            print(f"yesterday's date could not be resolved: {m}")
            return
        yesterday = datetime.strptime(m.groups(1)[0], '%Y%m%d') - timedelta(days=1)
        yesterday = datetime.strftime(yesterday, '%Y%m%d')
        yesterday_path = self.s3output.replace(m.groups(1)[0], yesterday)
        yesterday_path = yesterday_path + '/tables/'
        bucket = yesterday_path.split('/')[0]
        yesterday_path = '/'.join(yesterday_path.split('/')[1:])

        client = boto3.client('s3')
        paginator = client.get_paginator('list_objects')
        print(f'Downloading all summaries from {bucket}{yesterday_path}')

        for page in paginator.paginate(Bucket=bucket, Prefix=yesterday_path):
            if 'Contents' not in page or len(page['Contents']) == 0:
                print('Could not find data from yesterday')
                return
            for obj in page['Contents']:
                fname = obj['Key'].split('/')[-1]
                client.download_file(bucket,
                                     obj['Key'],
                                     self.output+'yesterday/'+fname)


        diff = DiffGenerator(output=self.output+'diffs/',
                             summaries=table_summaries,
                             yesterday=self.output+'yesterday/')
        diff.make_report()

        # Remove yesterday's data so it's not uploaded again
        import shutil
        shutil.rmtree(self.output+'yesterday')

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

    def __init__(self, output='/tmp/', yesterday=None, summaries=None):
        """
        :param output: The directory to output all diff tables
        :param summaries: The summary tables for the current state
        :param yesterday: A directory where to find all of yesterday's summaries
        """
        if not os.path.isdir(yesterday):
            raise Exception(f'Please provide valid directory paths')

        self.output = output
        self.yesterday_path = yesterday
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
        filename = f"../Table_Diff_Report_{datetime.now().strftime('%Y%m%d')}.html"
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

            df2_path = os.path.join(self.yesterday_path, f+'.csv')
            # Don't try to diff if it's a summary that didn't exist yesterday
            if not os.path.exists(df2_path):
                continue
            df2 = pd.read_csv(df2_path, index_col=0)

            # Diff using k: v dictonary comparison
            if len(df1.columns) == 2 and df1.columns[1] == 'count':
                sections[section][name] = self.count_diff(df1, df2)
                sections[section][name].to_csv(f'{self.output}{section}_{name}_diff.csv')
                sections[section][name] = sections[section][name]\
                        .drop(['count', 'change', 'summary', 'count_yesterday'],
                                             axis=1)\
                        .rename(columns={'summary_html': 'count (change)'})
            # Diff for wide tables with strings
            else:
                sections[section][name] = self.compute_diff(df1, df2)
                sections[section][name].to_csv(f'{self.output}{section}_{name}_diff.csv')
            # Don't try to style an empty diff and just return None
            if sections[section][name].shape[0] == 0:
                sections[section][name] = None
                continue
            if len(df1.columns) == 2 and df1.columns[1] == 'count':
                sections[section][name] = sections[section][name].style.set_table_attributes('class="table table-striped table-hover"')
            else:
              sections[section][name] = sections[section][name].applymap(self.highlight_diff)
              sections[section][name] = sections[section][name].style.set_table_attributes('class="table table-striped table-hover"')

        return sections

    def count_diff(self, df1, df2):
        """
        Compares two dataframes by converting to dicts
        """
        def diff_format(r):
            return f"{int(r['count'])} ({int(r['change']):+})"

        def diff_html(r):
            """
            Wraps values in span tags for display in web page
            """
            change = f"{int(r['change']):+}"
            color = ''
            if r['change'] > 0:
                color = 'text-success'
            elif r['change'] < 0:
                color = 'text-danger'
            else:
                color = 'text-color'
            change = f'(<span class="{color}">{change}</span>)'
            return f"{int(r['count'])} {change}"

        # Convert datetimes to strings
        for c in df1.select_dtypes(include=[np.datetime64]):
            df1[c] = df1[c].dt.strftime('%Y-%m-%d %H:%M:%S')

        for c in df2.select_dtypes(include=[np.datetime64]):
            df2[c] = df2[c].dt.strftime('%Y-%m-%d %H:%M:%S')

        diff = df2.merge(df1, how='outer', on=df1.columns[0],
                         suffixes=['_yesterday', '']).fillna(0)
        diff['change'] = diff['count'] - diff['count_yesterday']
        diff = diff.sort_values(['change', 'count'], ascending=False).reset_index(drop=True)
        diff['summary'] = diff.apply(diff_format, axis=1)
        diff['summary_html'] = diff.apply(diff_html, axis=1)
        diff = diff[diff['change'] != 0]
        return diff

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
            
        return diff

    def highlight_diff(self, value):
        if value is None:
            return value
        
        try:
            m = DIFF_RE.match(str(value))
            if m:
                change = float(str(m.group(2)))
            else:
                return value

            if change < 0:
                r = DIFF_RE.sub(r"\1<span class='text-danger'>\2</span>)", value)
                return r
            elif change > 0:
                r = DIFF_RE.sub(r"\1<span class='text-success'>\2</span>)", value)
                return r
            else:
                return value
        except ValueError as verr:
            return value
        return value

    def differ(self, r):
        """
        Compares two values and returns the new value
        """
        first = r['DF1']
        second = r['DF2']
        try:
            try:
                first = int(str(first))
                second = int(str(second))
            except ValueError:
                first = float(str(first))
                second = float(str(second))
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
