"""
Tests for the inspecting files

 """
import json
import pytest
from functools import partial
from os.path import abspath, join, dirname
import pandas as pd
from collections import namedtuple
import upload.upload as cup
from upload import iloop_client
from upload.settings import Default
from upload.checks import (reaction_id_unknown, medium_name_unknown, protein_id_unknown,
                           strain_alias_unknown, compound_name_unknown, synonym_to_chebi_name)

TEST_PROJECT = 'DEM'  # TODO: use project part of default fixture
PROJECT_OBJECT = namedtuple('Project', ['code'])(code=TEST_PROJECT)


@pytest.fixture(scope='session')
def examples():
    return abspath(join(dirname(abspath(__file__)), "..", "data", "examples"))


def test_media_inspection(examples):
    up = cup.MediaUploader(PROJECT_OBJECT, join(examples, 'media.csv'), [])
    assert isinstance(up.df, pd.DataFrame)
    for item in up.iloop_args:
        name, ingredients, info = item
        assert all(key in info for key in ['name', 'identifier', 'ph'])
        assert isinstance(ingredients, list)
        assert isinstance(name, str)
    with pytest.raises(ValueError) as excinfo:
        cup.MediaUploader(PROJECT_OBJECT, join(examples, 'media-invalid.csv'), [])
    report = json.loads(str(excinfo.value))
    assert report['error-count'] == 1
    error = report['tables'][0]['errors'].pop()
    assert 'does not conform to the maximum' in error['message']


def test_strains_inspection(examples):
    up = cup.StrainsUploader(PROJECT_OBJECT, join(examples, 'strains.csv'))
    assert isinstance(up.df, pd.DataFrame)
    assert len(up.iloop_args) == len(up.df)
    assert set(strain['strain_alias'] for strain in up.iloop_args) == set(up.df['strain'])
    with pytest.raises(ValueError) as excinfo:
        cup.StrainsUploader(PROJECT_OBJECT, join(examples, 'strains-invalid.csv'))
    report = json.loads(str(excinfo.value))
    assert report['error-count'] == 1
    error = report['tables'][0]['errors'].pop()
    assert 'bad expected gnomic' in error['message']


def test_fermentation_inspection(examples):
    up = cup.FermentationUploader(PROJECT_OBJECT, join(examples, 'samples.csv'), join(examples, 'physiology.csv'), [])
    assert isinstance(up.samples_df, pd.DataFrame)
    assert isinstance(up.physiology_df, pd.DataFrame)


def test_screen_inspection(examples):
    up = cup.ScreenUploader(PROJECT_OBJECT, join(examples, 'screening.csv'), [])
    assert isinstance(up.df, pd.DataFrame)


def test_fermentation_inspection_with_iloop(examples):
    iloop = iloop_client(Default.ILOOP_API, Default.ILOOP_TOKEN)
    project = iloop.Project.first(where={'code': TEST_PROJECT})
    up = cup.FermentationUploader(project,
                                  join(examples, 'samples.csv'),
                                  join(examples, 'physiology.csv'),
                                  custom_checks=[
                                      partial(compound_name_unknown, iloop, None),
                                      partial(medium_name_unknown, iloop, None),
                                      partial(strain_alias_unknown, iloop, project)],
                                  synonym_mapper=partial(synonym_to_chebi_name, iloop, None))
    assert isinstance(up.samples_df, pd.DataFrame)
    assert isinstance(up.physiology_df, pd.DataFrame)


def test_fluxomics_inspection_with_iloop(examples):
    iloop = iloop_client(Default.ILOOP_API, Default.ILOOP_TOKEN)
    project = iloop.Project.first(where={'code': TEST_PROJECT})
    up = cup.XrefMeasurementUploader(project,
                                     join(examples, 'fluxes.csv'),
                                     subject_type='reaction',
                                     custom_checks=[
                                         partial(medium_name_unknown, iloop, None),
                                         partial(reaction_id_unknown, iloop, None),
                                         partial(strain_alias_unknown, iloop, project)])
    assert isinstance(up.samples_df, pd.DataFrame)


def test_proteomics_inspection_with_iloop(examples):
    iloop = iloop_client(Default.ILOOP_API, Default.ILOOP_TOKEN)
    project = iloop.Project.first(where={'code': TEST_PROJECT})
    up = cup.XrefMeasurementUploader(project,
                                     join(examples, 'protein_abundances.csv'),
                                     subject_type='protein',
                                     custom_checks=[
                                         partial(medium_name_unknown, iloop, None),
                                         partial(protein_id_unknown, iloop, None),
                                         partial(strain_alias_unknown, iloop, project)])
    assert isinstance(up.samples_df, pd.DataFrame)
