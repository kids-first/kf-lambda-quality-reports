import boto3
import os
import pytest
from moto import mock_s3

from reports import change_report


@pytest.fixture
def objects():
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


@mock_s3
def test_invalid_path(objects):
    """ Test that invalid s3 paths fail """
    objects()
    client = boto3.client('s3')
    event = { 
        'summary_path_1': 's3://tests/summary_1/',
        'summary_path_2': 's3://tests/summary_2/',
        'output': 'tests/data/output/',
        'title': 'Biospecimen Change Report',
    }

    res = change_report.handler(event, {})


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
