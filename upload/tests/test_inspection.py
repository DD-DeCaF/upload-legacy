"""
Tests for the inspecting files

 """
import json
from collections import namedtuple
from os.path import join
import functools

import pandas as pd
import pytest

import upload.upload as cup
from upload.checks import check_safe_partial as partial
from upload.checks import (compound_name_unknown, medium_name_unknown,
                           protein_id_unknown, reaction_id_unknown,
                           strain_alias_unknown, synonym_to_chebi_name)

TEST_PROJECT = 'DEM'  # TODO: use project part of default fixture
PROJECT_OBJECT = namedtuple('Project', ['code'])(code=TEST_PROJECT)


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


def test_screen_inspection(examples):
    up = cup.ScreenUploader(PROJECT_OBJECT, join(examples, 'screening.csv'), [])
    assert isinstance(up.df, pd.DataFrame)


def test_fermentation_inspection(examples, project):
    up = cup.FermentationUploader(project,
                                  join(examples, 'samples.csv'),
                                  join(examples, 'physiology.csv'),
                                  custom_checks=[
                                      partial(compound_name_unknown, None),
                                      partial(medium_name_unknown, None),
                                      partial(strain_alias_unknown, project)],
                                  synonym_mapper=functools.partial(synonym_to_chebi_name, None))
    assert isinstance(up.samples_df, pd.DataFrame)
    assert isinstance(up.physiology_df, pd.DataFrame)


def test_fluxomics_inspection(examples, project):
    checks = [partial(medium_name_unknown, None),
              partial(reaction_id_unknown, None),
              partial(strain_alias_unknown, project)]
    up = cup.XrefMeasurementUploader(project, join(examples, 'fluxes.csv'),
                                     subject_type='reaction', custom_checks=checks)
    assert isinstance(up.df, pd.DataFrame)
    with pytest.raises(ValueError) as excinfo:
        cup.XrefMeasurementUploader(project,
                                    join(examples, 'fluxes-invalid.csv'),
                                    subject_type='reaction',
                                    custom_checks=checks)
    report = json.loads(str(excinfo.value))
    assert report['error-count'] == 2
    error = report['tables'][0]['errors'].pop()
    assert 'unknown reaction identifier' in error['message']


def test_proteomics_inspection(examples, project):
    checks = [partial(medium_name_unknown, None),
              partial(protein_id_unknown, None),
              partial(strain_alias_unknown, project)]
    up = cup.XrefMeasurementUploader(project,
                                     join(examples, 'protein_abundances.csv'),
                                     subject_type='protein', custom_checks=checks)
    assert isinstance(up.df, pd.DataFrame)
    with pytest.raises(ValueError) as excinfo:
        cup.XrefMeasurementUploader(project,
                                    join(examples, 'protein_abundances-invalid.csv'),
                                    subject_type='reaction',
                                    custom_checks=checks)
    report = json.loads(str(excinfo.value))
    assert report['error-count'] == 1
    error = report['tables'][0]['errors'].pop()
    assert 'unknown protein identifier' in error['message']
