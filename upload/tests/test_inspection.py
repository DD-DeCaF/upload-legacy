"""
Tests for the inspecting files

 """
import upload.upload as cup
from os.path import abspath, join, dirname
import pandas as pd
import pytest
import json


@pytest.fixture(scope='session')
def examples():
    return abspath(join(dirname(abspath(__file__)), "..", "data", "examples"))


def test_media_inspection(examples):
    up = cup.MediaUploader('TST', join(examples, 'media.csv'), [])
    assert isinstance(up.df, pd.DataFrame)
    for item in up.iloop_args:
        name, ingredients, info = item
        assert all(key in info for key in ['name', 'identifier', 'ph'])
        assert isinstance(ingredients, list)
        assert isinstance(name, str)
    with pytest.raises(ValueError) as excinfo:
        cup.MediaUploader('TST', join(examples, 'media-invalid.csv'), [])
    report = json.loads(str(excinfo.value))
    assert report['error-count'] == 1
    error = report['tables'][0]['errors'].pop()
    assert 'does not conform to the maximum' in error['message']


def test_strains_inspection(examples):
    up = cup.StrainsUploader('TST', join(examples, 'strains.csv'))
    assert isinstance(up.df, pd.DataFrame)
    assert up.iloop_args[0]['strain_alias'] == 'scref'
    assert up.iloop_args[1]['strain_alias'] == 'bla-3'
    assert up.iloop_args[2]['strain_alias'] == 'foo-3'
    with pytest.raises(ValueError) as excinfo:
        cup.StrainsUploader('TST', join(examples, 'strains-invalid.csv'))
    report = json.loads(str(excinfo.value))
    assert report['error-count'] == 1
    error = report['tables'][0]['errors'].pop()
    assert 'bad expected gnomic' in error['message']


def test_experiment_inspection(examples):
    up = cup.ExperimentUploader('TST', join(examples, 'samples.csv'), join(examples, 'physiology.csv'), [])
    assert isinstance(up.samples_df, pd.DataFrame)
    assert isinstance(up.physiology_df, pd.DataFrame)
