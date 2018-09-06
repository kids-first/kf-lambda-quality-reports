import boto3
import os
import re
import glob
from datetime import datetime

import pandas as pd
import numpy as np

from collections import defaultdict

from jinja2 import Environment, FileSystemLoader


DIFF_RE = re.compile(r'^(.*\()([+-]?\d+(\.\d+)?)\)$')


def handler(event, context):
    """
    Create a change report between two different summary directories

    The event param should be formatted as:
    {
        "summary_path_1": String,
        "summary_path_2": String,
        "output": String,
        "title": String
    }

    Where `summary_path_1` and `summary_path_2` are the local, or s3 paths of
    the summary tables.
    `output` is the local or s3 path to save the diff tables and report to.
    `title` is a description of the report.

    We expect `summary_path` directories to be formatted as follows:
    ```
    studies/
      name.csv
      external_id.csv
    participants/
      external_id.csv
      gender.csv
    table_n/
      column_1.csv
      column_2.csv
      ...
    ```
    """
    path_1 = event.get('summary_path_1')
    path_2 = event.get('summary_path_2')
    output = event.get('output', './output/')
    local_output = event.get('local_output', '/tmp/')
    title = event.get('title', '')
    subtitle = event.get('subtitle', 'Change Report')

    g = ChangeGenerator(path_1, path_2, title=title, subtitle=subtitle)
    diff_message = g.make_report()
    print(diff_message)

    # Collect all files in ouput
    files = set(glob.glob(local_output+'diffs/**/*.csv', recursive=True))
    files = files.union(set(glob.glob(local_output+'**/*.html', recursive=True)))

    if len(diff_message) > 0:
        url = output
        url += f"/{title.lower().replace(' ', '_')}.html"
        url = 'https://s3.amazonaws.com/' + url
        diff_message[0]["actions"] = [
            {
                "type": "button",
                "text": ":eyes: Check it Out :eyes:",
                "url": url,
                "style": "primary"
            }
        ]

    print(files)
    print(output)
    print(os.listdir('/tmp'))
    print(os.listdir('/tmp/diffs'))

    return diff_message, {p.replace('/tmp/', ''): p for p in list(files)}


class ChangeGenerator:

    def __init__(self, path_1, path_2, output='/tmp/',
                 title='Change Report', subtitle=''):
        """
        :param path_1: Path to directory of first summary files
        :param path_2: Path to directory of second summary files
        :param output: The directory to output all diff tables
        """
        self.output = output
        self.title = title
        self.subtitle = subtitle
        if 's3://' in path_1:
            path_1 = self.download_summary(path_1, output=output+'summary_1/')
        if 's3://' in path_2:
            path_2 = self.download_summary(path_2, output=output+'summary_2/')

        if not os.path.isdir(path_1) or not os.path.isdir(path_2):
            raise IOError(f'Please provide valid directory paths')

        self.path_1 = path_1
        self.path_2 = path_2
        self.tables = self.compare_tables(path_1, path_2)
        self.columns = self.compare_columns(path_1, path_2)

    def download_summary(self, path, output='/tmp/'):
        """ Download summary tables from s3 to local """
        os.makedirs(output, exist_ok=True)
        path = path.replace('s3://', '')
        bucket = path.split('/')[0]
        key = '/'.join(path.split('/')[1:])

        client = boto3.client('s3')
        paginator = client.get_paginator('list_objects')
        paginator = paginator.paginate(Bucket=bucket, Prefix=key)
        print(f'Downloading all summaries from {bucket}/{key}')

        for page in paginator:
            for obj in page['Contents']:
                fname = '/'.join(obj['Key'].split('/')[-2:])
                d = os.path.join(output, '/'.join(fname.split('/')[:-1]))
                os.makedirs(d, exist_ok=True)
                client.download_file(bucket,
                                     obj['Key'],
                                     os.path.join(output, fname))
        return output

    def make_report(self):
        diffs, counts = self.compute_diffs()

        # Styling of the diff tables
        for name, table in diffs.items():
            if name == 'summary':
                continue
            for col, column in table.items():
                diffs[name][col] = (diffs[name][col]
                        .drop(['count_2', 'change', 'summary', 'count_1'],
                              axis=1)
                        .rename(columns={'summary_html': 'count (change)'}))
                diffs[name][col] = (diffs[name][col]
                        .style.set_table_attributes(
                            'class="table table-striped table-hover"'))

        env = Environment(loader=FileSystemLoader(os.path.dirname(os.path.abspath(__file__))))
        template = env.get_template("summary_template.html")

        formatted_counts = {}
        for table, columns in counts.items():
            formatted_counts[table] = pd.DataFrame.from_dict(columns,
                                                             orient='index',
                                                             columns=['change'])
            formatted_counts[table].index.name = 'column'
            formatted_counts[table] = formatted_counts[table].reset_index()

        template_vars = {
            "title": self.title,
            "subtitle": self.subtitle,
            "date": datetime.now(),
            "sections": diffs,
            "counts": formatted_counts
        }
        # Render and save HTML
        html_out = template.render(template_vars)
        title = self.title.lower().replace(' ', '_')
        filename = f"{title}.html"
        with open(os.path.join(self.output, filename), "w+") as f:
            f.write(html_out)

        # Create diff message
        return self.diff_summary_message(counts)

    def diff_summary_message(self, counts):
        """
        Compute number of changes by table then report number of tables
        changed and number of values changed
        """
        summaries = {f'{k1}_{k2}': v2 for k1, v1 in counts.items()
                                     for k2, v2 in v1.items()}

        total = int(sum(summaries.values()))
        non_zero = [k for k, v in summaries.items() if v > 0]

        if total <= 0:
            return []
        
        expl = np.random.choice(['Wow', 'Wowza', 'Woa', 'Gee', 'Oh my', 'Hey',
                                 'Take a look', 'Check it out', 'Gosh', 'Golly'])

        message = (f"{expl}! There were *{total}* changes made across "
                    +f"*{len(non_zero)}/{len(summaries)}* columns yesterday!")

        attachments = [{
            "text": message,
            "fallback": message.replace('*', ''),
            "color": "#3AA3E3",
            "attachment_type": "default",
        }]

        return attachments

    def compare_tables(self, path_1, path_2):
        """ Compare available tables between the two summaries """
        tables_1 = set(os.listdir(path_1))
        tables_2 = set(os.listdir(path_2))
        added = list(tables_2 - tables_1)
        deleted = list(tables_1 - tables_2)
        same = list(tables_1 & tables_2)

        return {'added': added, 'deleted': deleted, 'same': same}

    def compare_columns(self, path_1, path_2):
        """ Compare available columns between the two summaries """
        tables_1 = set(os.listdir(path_1))
        tables_2 = set(os.listdir(path_2))

        columns = defaultdict(dict)
        for table in tables_1 | tables_2:
            columns[table]['added'] = []
            columns[table]['deleted'] = []
            columns[table]['same'] = []

            def strip_exts(path):
                return [p.split('.')[0] for p in os.listdir(path)]

            if not os.path.isdir(os.path.join(path_1, table)):
                columns[table]['added'] = strip_exts(path_2+'/'+table)
                continue
            elif not os.path.isdir(os.path.join(path_2, table)):
                columns[table]['deleted'] = strip_exts(path_1+'/'+table)
                continue

            col_1 = set(strip_exts(os.path.join(path_1, table)))
            col_2 = set(strip_exts(os.path.join(path_2, table)))
            columns[table]['same'] = list(col_1 & col_2)
            columns[table]['added'] = list(col_2 - col_1)
            columns[table]['deleted'] = list(col_1 - col_2)

        return columns

    def shape_diff(self):
        df = pd.DataFrame.from_dict(self.columns,
                                    orient='index').drop('same', axis=1)
        df = df[['added', 'deleted']].applymap(lambda x: ', '.join(x))
        df = df.dropna(thresh=1, axis=0)
        return df

    def compute_diffs(self):
        """
        Looks through summary tables and computes diffs for each returning a
        dict of styled html tables organized by table and column and leaving
        out any summaries that did not change.
        """

        tables = defaultdict(dict)
        # Also keep track of total number of changes per column
        counts = defaultdict(dict)

        # Look through each table
        for table, columns in self.columns.items():
            out_dir = os.path.join(self.output, 'diffs', table)
            os.makedirs(out_dir, exist_ok=True)
            # Make diffs for each column that is shared between summaries
            for column in columns['same']:
                if column == 'summary':
                    continue
                df_1 = pd.read_csv(os.path.join(self.path_1,
                                                table,
                                                column+'.csv'),
                                   index_col=0)
                df_2 = pd.read_csv(os.path.join(self.path_2,
                                                table,
                                                column+'.csv'),
                                   index_col=0)
                if len(df_1) == 0 or len(df_2) == 0:
                    continue

                tables[table][column] = self.count_diff(df_1, df_2)
                # Save diff
                out_path = os.path.join(out_dir, f'{column}_diff.csv')
                tables[table][column].to_csv(out_path)
                # Update the counts
                counts[table][column] = (tables[table][column]['change']
                                            .abs()
                                            .sum())
        return tables, counts

        self.diff_counts = counts
        return sections

    def count_diff(self, df1, df2):
        """
        Compares two dataframes by converting to dicts
        """
        def diff_format(r):
            return f"{int(r['count_2'])} ({int(r['change']):+})"

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
            return f"{int(r['count_2'])} {change}"

        # Convert datetimes to strings
        for c in df1.select_dtypes(include=[np.datetime64]):
            df1[c] = df1[c].dt.strftime('%Y-%m-%d %H:%M:%S')

        for c in df2.select_dtypes(include=[np.datetime64]):
            df2[c] = df2[c].dt.strftime('%Y-%m-%d %H:%M:%S')

        diff = df2.merge(df1, how='outer', on=df1.columns[0],
                         suffixes=['_2', '_1']).fillna(0)
        diff['change'] = diff['count_2'] - diff['count_1']
        diff['summary'] = diff.apply(diff_format, axis=1)
        diff['summary_html'] = diff.apply(diff_html, axis=1)
        diff = diff[diff['change'] != 0]
        diff = diff.sort_values(['change', 'count_2'], ascending=False).reset_index(drop=True)
        #diff = diff.reset_index(drop=True)
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
    handler({"name": "Change Report",
            "module": "reports.change_report",
            "output": "kf-reports-us-east-1-env-quality-reports/today/change_report/SD_XXX_QC_Report/"
            }, {})
