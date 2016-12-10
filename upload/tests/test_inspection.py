"""
Tests for the inspecting files

 """
import json
import pytest
from functools import partial
from os.path import abspath, join, dirname
import pandas as pd
import upload.upload as cup
from upload import iloop_client
from upload.settings import Default
from upload.checks import experiment_identifier_unknown, medium_name_unknown, \
    strain_alias_unknown, compound_name_unknown, synonym_to_chebi_name


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
    assert up.iloop_args[1]['strain_alias'] == 'eggs'
    assert up.iloop_args[2]['strain_alias'] == 'spam'
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


def test_experiment_inspection_with_iloop(examples):
    iloop = iloop_client(Default.ILOOP_API, Default.ILOOP_TOKEN)
    up = cup.ExperimentUploader('TST',
                                join(examples, 'samples.csv'),
                                join(examples, 'physiology.csv'),
                                custom_checks=[
                                    partial(compound_name_unknown, iloop),
                                    partial(experiment_identifier_unknown,
                                            iloop),
                                    partial(medium_name_unknown, iloop),
                                    partial(strain_alias_unknown, iloop)],
                                synonym_mapper=partial(synonym_to_chebi_name,
                                                       iloop))
    assert isinstance(up.samples_df, pd.DataFrame)
    assert isinstance(up.physiology_df, pd.DataFrame)
