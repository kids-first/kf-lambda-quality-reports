import hvac
import os
import glob
import pandas as pd
import matplotlib
matplotlib.use('PS')
import matplotlib.pyplot as plt
import boto3
from botocore.vendored import requests

import matplotlib.pyplot as plt
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


def handler(event, context):
    """
    Compile a report for a given study
    """
    study_id = event.get('study_id')
    output = event.get('output')
    conn_str = get_pg_connection_str()
    g = ReportGenerator(study_id, output=f"/tmp/", conn_str=conn_str)
    g.make_report()

    files = set(glob.glob('/tmp/**/*.png', recursive=True))
    files = files.union(set(glob.glob('/tmp/**/*.csv', recursive=True)))
    files = files.union(set(glob.glob('/tmp/**/*.pdf', recursive=True)))
    return [], {p.replace('/tmp/', ''): p for p in list(files)}


class ReportGenerator:

    def __init__(self, study_id, output=None, conn_str=''):
        self.study_id = study_id
        self.conn_str = conn_str
        self.output = output
        if self.output is None:
            self.output = self.study_id+'/'
        os.makedirs(self.output+'figures', exist_ok=True)
        os.makedirs(self.output+'tables', exist_ok=True)

    def make_report(self):
        study = self.get_study_info()
        participant_report = self.get_participant_report()
        family_report = self.get_family_report()
        biospecimen_report = self.get_biospecimen_report()

        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template("reports/study_template.html")

        template_vars = {
            "study": study,
            "participant": participant_report,
            "family": family_report,
            "biospecimen": biospecimen_report
        }
        html_out = template.render(template_vars)

        filename = f'{self.study_id}_QC_Report.pdf'
        with open(self.output+filename, "w+b") as out_pdf_file_handle:
            pisa.CreatePDF(
                src=html_out,
                dest=out_pdf_file_handle)

    def get_study_info(self):
        study_info = pd.read_sql_query("SELECT * FROM study WHERE kf_id='{}';".format(self.study_id),
                                       con=self.conn_str)
        study_info = study_info.iloc[0]
        return study_info

    def column_stats(self, df, prefix='', ignore=None):
        """
        Produce column level summaries
        Create value distribution plots and value count tables for each column
        that is not in `ignore`
        """
        if ignore is None:
            ignore = {}

        ignore = ignore.union({'uuid', 'kf_id', 'created_at', 'modified_at',
                               'external_id'})
        TABLES = {}
        FIGURES = {}

        for col in set(df.columns) - ignore:
            counts = (df.groupby(col)[col]
                        .count()
                        .sort_values(ascending=False))
            if len(counts)> 0:
                f = plt.figure()
                counts.plot(kind='bar')
                plt.title(col)
                plt.tight_layout()
                f.savefig(self.output+'figures/{}.png'.format(col))
                plt.close(f)
                FIGURES[col] = self.output+'figures/{}.png'.format(col)

            TABLES[col] = pd.DataFrame(counts.values, counts.index, columns=['count'])
            TABLES[col].to_csv(self.output+'tables/{}{}.csv'
                               .format(prefix+'_' if prefix else '', col))
            TABLES[col] = TABLES[col].reset_index().to_html(index=False)

        return FIGURES, TABLES

    def get_participant_report(self):
        df = pd.read_sql_query("SELECT * FROM participant WHERE study_id='{}';".format(self.study_id),
                               con=self.conn_str)
        self.df_p = df

        ignore = {'study_id', 'alias_group_id', 'family_id'}
        FIGURES, TABLES = self.column_stats(df, 'participant', ignore)

        counts = df.groupby('study_id')[['kf_id']].count().sort_values('kf_id', ascending=False)
        nulls = df.isnull().sum()

        nulls.to_csv(self.output+'tables/participant_nulls.csv')

        dupes = df.groupby('external_id').count()
        dupes = df.groupby('external_id').count()[['kf_id']].rename(columns={'kf_id': 'count'})
        dupes = dupes[dupes['count'] > 1].reset_index()

        report = {
            'counts': counts.reset_index().to_html(index=False),
            'nulls': pd.DataFrame(nulls, columns=['# Nulls']).to_html(),
            'dupe_external': dupes.to_html(index=False),
            'tables': TABLES,
            'figures': FIGURES
        }
        return report

    def get_biospecimen_report(self):
        stmt = """
        SELECT biospecimen.*, participant.study_id
        FROM biospecimen 
           LEFT JOIN participant ON biospecimen.participant_id=participant.kf_id
        WHERE participant.study_id='{}';
        """.format(self.study_id)
        df = pd.read_sql_query(stmt, con=self.conn_str)
        self.df_bs = df

        ignore = {'study_id', 'participant_id',
                  'external_sample_id', 'external_aliquot_id'}
        FIGURES, TABLES = self.column_stats(df, 'biospecimen', ignore)

        counts = df.groupby('study_id')[['kf_id']].count().sort_values('kf_id', ascending=False)
        nulls = df.isnull().sum()

        nulls.to_csv(self.output+'tables/biospecimen_nulls.csv')

        dupes = df.groupby('external_aliquot_id').count()
        dupes = df.groupby('external_aliquot_id').count()[['kf_id']].rename(columns={'kf_id': 'count'})
        dupes = dupes[dupes['count'] > 1].reset_index()

        report = {
            'counts': counts.reset_index().to_html(index=False),
            'nulls': pd.DataFrame(nulls, columns=['# Nulls']).to_html(),
            'dupe_external': dupes.to_html(index=False),
            'tables': TABLES,
            'figures': FIGURES
        }
        return report


    def get_family_report(self):
        FIGURES = {}
        stmt = """
        SELECT *
        FROM family
        WHERE kf_id in (
            SELECT familY_id
            FROM participant
            WHERE study_id = '{}')
        """.format(self.study_id)

        f = plt.figure()
        df_fam = pd.read_sql_query(stmt, con=self.conn_str)
        df_pf = self.df_p.merge(df_fam, left_on='family_id',
                                right_on='kf_id', suffixes=['_p', '_f'])
        df_pf[df_pf['is_proband']].groupby('kf_id_f')['kf_id_p'].count().plot(kind='hist')
        plt.title('Probands per Family')
        f.savefig(self.output+'figures/proband_dist.png')
        plt.close(f)
        FIGURES['proband_dist'] = self.output+'figures/proband_dist.png'

        counts = df_pf[df_pf['is_proband']].groupby('kf_id_f')[['kf_id_p']].count()
        more_than_one = counts[counts['kf_id_p'] > 1].reset_index().rename(columns={'kf_id_f': 'family_id', 'kf_id_p': 'participant_id'})
        more_than_one.to_csv(self.output+'/tables/more_than_one_proband.csv')

        no_proband = counts[counts['kf_id_p'] == 0].reset_index().rename(columns={'kf_id_f': 'family_id', 'kf_id_p': 'participant_id'})
        no_proband.to_csv(self.output+'/tables/no_proband.csv')

        f = plt.figure()
        fam_size = df_pf.groupby('kf_id_f')[['kf_id_p']].count()
        fam_size = fam_size.rename(columns={'kf_id_p': 'size'})
        fam_size.plot(kind='hist', ax=plt.gca())
        fam_size = pd.DataFrame(fam_size['size'].value_counts())
        fam_size.index.name = '# members'
        fam_size.columns = ['count']
        fam_size = fam_size.reset_index()
        plt.title('Family Sizes')
        f.savefig(self.output+'figures/family_sizes.png')
        FIGURES['family_sizes'] = self.output+'figures/family_sizes.png'
        plt.close(f)

        report = {
            'more_than_one': more_than_one.to_html(index=False),
            'no_proband': no_proband.to_html(index=False),
            'family_size': fam_size.to_html(index=False),
            'figures': FIGURES
        }
        return report



# For local testing
if __name__ == '__main__':
    handler({"name": "Study Report",
            "module": "reports.study_report",
            "output": "kf-reports-us-east-1-env-quality-reports/today/study_reports/SD_XXX_QC_Report/"
            }, {})
