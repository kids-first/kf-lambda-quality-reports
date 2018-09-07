import boto3
import os
import pytest
from moto import mock_s3

import matplotlib
matplotlib.use('TkAgg')

from reports import change_report


@pytest.fixture
def objects():
    @mock_s3
    def f():
        client = boto3.client('s3')
        client.create_bucket(Bucket='tests')

        with open('tests/data/change_report/summary_1/biospecimens/composition.csv', 'rb') as data:
            client.upload_fileobj(data, 'tests', 'summary_1/biospecimens/composition.csv')

        with open('tests/data/change_report/summary_2/biospecimens/composition.csv', 'rb') as data:
            client.upload_fileobj(data, 'tests', 'summary_2/biospecimens/composition.csv')

    return f



def test_invalid_dir():
    """ Test that non-existant directories fail """
    event = { 
        'summary_path_1': 'tests/data/does_not_exist/',
        'summary_path_2': 'tests/data/change_report/',
        'output': 'change_report',
        'title': 'Biospecimen Change Report',
    }

    with pytest.raises(IOError) as err:
        change_report.handler(event, {})
    assert 'provide valid' in str(err.value)


def test_compare_tables():
    """ Test that two summaries' tables are compared correctly """
    path_1 = 'tests/data/change_report/summary_1/'
    path_2 = 'tests/data/change_report/summary_2/'

    g = change_report.ChangeGenerator(path_1, path_2, output='tests/data/output')
    assert g.tables == {
        'same': ['biospecimens'],
        'added': ['diagnoses'],
        'deleted': ['participants'],
    }


@mock_s3
def test_compare_columns():
    """ Test that two summaries' columns are compared correctly """
    path_1 = 'tests/data/change_report/summary_1/'
    path_2 = 'tests/data/change_report/summary_2/'

    g = change_report.ChangeGenerator(path_1, path_2, output='tests/data/output')
    assert g.columns == {
        'biospecimens': {
            'same': ['composition'],
            'added': ['analyte_type'],
            'deleted': [],
        },
        'diagnoses': {
            'same': [],
            'added': ['diagnosis_category'],
            'deleted': [],
        },
        'participants': {
            'same': [],
            'added': [],
            'deleted': ['gender'],
        }
    }


@mock_s3
def test_compute_diffs(tmpdir):
    """ Test that column diffs are calculated correctly """
    path_1 = 'tests/data/change_report/summary_1/'
    path_2 = 'tests/data/change_report/summary_2/'
    p = tmpdir.mkdir("output")

    g = change_report.ChangeGenerator(path_1, path_2, output=p)
    tables, counts = g.compute_diffs()
    assert os.path.isfile(p+'/diffs/biospecimens/composition_diff.csv')
    assert not os.path.isfile(p+'/biospecimens/analyte_type_diff.csv')
    assert list(tables.keys()) == ['biospecimens']
    assert list(tables['biospecimens'].keys()) == ['composition']

    df = tables['biospecimens']['composition']
    assert df[df['composition'] == 'Bone Marrow']['change'].iloc[0] == 7
    assert df[df['composition'] == 'Blood']['change'].iloc[0] == 15
    assert df[df['composition'] == 'Blood']['count_2'].iloc[0] == 16


def test_make_report(tmpdir):
    """ Test that diff report is created and message formatted """
    path_1 = 'tests/data/change_report/summary_1/'
    path_2 = 'tests/data/change_report/summary_2/'
    p = tmpdir.mkdir("output")

    g = change_report.ChangeGenerator(path_1, path_2, output=p)
    g.make_report()
    assert os.path.isfile(p+'/change_report.html')
