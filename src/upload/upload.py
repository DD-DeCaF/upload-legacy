# Copyright 2018 Novo Nordisk Foundation Center for Biosustainability, DTU.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pandas as pd
from datetime import datetime
from potion_client.exceptions import ItemNotFound
from goodtables import Inspector
from dateutil.parser import parse as parse_date
import json
from os.path import abspath, dirname, join, exists
from requests import HTTPError
from copy import deepcopy
from upload import logger

from upload.constants import measurement_test, compound_skip
from upload.checks import genotype_not_gnomic
from upload import _isnan


def place_holder_compound_synonym_mapper(synonym):
    return synonym


def get_schema(schema_name):
    default_schemas = {'strains': 'strains_schema.json',
                       'media': 'media_schema.json',
                       'sample_information': 'sample_information_schema.json',
                       'physiology': 'physiology_schema.json',
                       'screen': 'screen_schema.json',
                       'fluxes': 'fluxes_schema.json',
                       'protein_abundances': 'protein_abundances_schema.json'}
    schema_name = default_schemas[schema_name]
    schema_dir = abspath(join(dirname(abspath(__file__)), "data", "schemas"))
    schema = join(schema_dir, schema_name)
    if not exists(schema):
        raise FileNotFoundError('missing schema %s' % schema)
    return schema


class DataFrameInspector(object):
    """ class for inspecting a table and reading it to a DataFrame


    :param file_name: name of the csv file to read
    :param schema_name: name of the json file specifying the scheme, possibly one of the schema in this package
    without path
    :param custom_checks: list of additional custom check functions to apply
    """

    def __init__(self, file_name, schema_name, custom_checks=None):
        self.schema = get_schema(schema_name)
        self.file_name = file_name
        self.custom_checks = custom_checks if custom_checks else []

    def inspect(self):
        """ inspect the data frame and return an error report """
        inspector = Inspector(custom_checks=self.custom_checks, order_fields=True)
        report = inspector.inspect(self.file_name, preset='table', schema=self.schema)
        if not report['valid']:
            raise ValueError(json.dumps(report, indent=4))

    def __call__(self):
        """ inspect and read to DataFrame """
        self.inspect()
        return pd.read_csv(self.file_name)


def inspected_data_frame(file_name, schema_name, custom_checks=None):
    """inspect and read a csv file

    :param file_name: name of the csv file to read
    :param schema_name: name of the json file specifying the scheme, possibly one of the schema in this package
    without path
    :param custom_checks: list of additional custom check functions to apply
    :return DataFrame: the inspected data frame
    """
    return DataFrameInspector(file_name=file_name, schema_name=schema_name,
                              custom_checks=custom_checks)()


class AbstractDataUploader(object):
    """ abstract class for uploading data to iloop """

    def __init__(self, project):
        self.project = project

    def upload(self, iloop):
        raise NotImplementedError


class MediaUploader(AbstractDataUploader):
    """upload media definitions

    inspect file using 'media_schema.json'. Upload if no existing medium with the exact same recipe. Key for the
    medium is generated using current date.

    :param project: project object
    :param file_name: name of the csv file to read
    """

    def __init__(self, project, file_name, custom_checks, synonym_mapper=place_holder_compound_synonym_mapper):
        super(MediaUploader, self).__init__(project)
        self.df = inspected_data_frame(file_name, 'media', custom_checks=custom_checks)
        self.iloop_args = []
        self.synonym_mapper = synonym_mapper
        self.prepare_upload()

    def prepare_upload(self):
        # directly naming the column 'compound' triggers a curious error when slicing
        self.df['chebi_name'] = pd.Series(
            [self.synonym_mapper(synonym) for synonym in
             self.df['compound_name']],
            index=self.df.index)

        self.df = self.df[self.df.chebi_name != compound_skip]
        grouped_media = self.df.groupby(['medium'])
        for medium_name, medium in grouped_media:
            ingredients_df = medium[['chebi_name', 'concentration']]
            ingredients_df.columns = ['compound', 'concentration']
            ingredients = list(ingredients_df.T.to_dict().values())
            if len(medium.pH.unique()) > 1:
                raise ValueError('expected only on pH per medium')
            ph = float(medium.iloc[0].pH)
            now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            self.iloop_args.append((
                medium_name,
                ingredients,
                {
                    'name': medium_name,
                    'identifier': '{}_{}'.format(medium_name, now),
                    'ph': ph
                })
            )

    def upload(self, iloop):
        for medium_name, ingredients, item in self.iloop_args:
            for k, v in item.items():
                if isinstance(v, str):
                    item[k] = v.strip()
            media_object = iloop.Medium.create(**item, organization=self.project.organization)
            media_object.update_contents(ingredients)


class StrainsUploader(AbstractDataUploader):
    """upload strain definitions

    inspect file using 'strains_schema.json' then sort the input data frame to make sure that parents are created
    before their children to avoid broken links.

    :param project: project object
    :param file_name: name of the csv file to read
    """

    def __init__(self, project, file_name):
        super(StrainsUploader, self).__init__(project)
        self.df = inspected_data_frame(file_name, 'strains', custom_checks=[genotype_not_gnomic])
        self.iloop_args = []
        self.prepare_upload()

    def prepare_upload(self):
        def depth(df, i, key, key_parent):
            if _isnan(df.loc[i][key_parent]):
                return 0
            else:
                try:
                    return depth(df, df[df[key] == df.loc[i][key_parent]].index[0], key, key_parent) + 1
                except IndexError:
                    return 0  # parent assumed to already be defined

        self.df['depth_pool'] = [depth(self.df, i, 'pool', 'parent_pool') for i in self.df.index]
        self.df['depth_strain'] = [depth(self.df, i, 'strain', 'parent_strain') for i in self.df.index]
        self.df = self.df.sort_values(by=['depth_pool', 'depth_strain'])
        for strain in self.df.itertuples():
            genotype_pool = '' if str(strain.genotype_pool) == 'nan' else strain.genotype_pool
            genotype_strain = '' if str(strain.genotype_strain) == 'nan' else strain.genotype_strain
            self.iloop_args.append({
                'pool_alias': strain.pool,
                'pool_type': strain.pool_type,
                'parent_pool_alias': strain.parent_pool,
                'genotype_pool': genotype_pool,
                'strain_alias': strain.strain,
                'parent_strain_alias': strain.parent_strain,
                'genotype': genotype_strain,
                'is_reference': bool(strain.reference),
                'organism': strain.organism,
                'project': self.project
            })

    def upload(self, iloop):
        for item in self.iloop_args:
            item = {k: v.strip() for k, v in item.items() if isinstance(v, str)}
            try:
                iloop.Strain.one(where={'alias': item['strain_alias'], 'project': self.project})
            except ItemNotFound:
                try:
                    pool_object = iloop.Pool.one(where={'alias': item['pool_alias'], 'project': self.project})
                except ItemNotFound:
                    parent_pool_object = None
                    if 'parent_pool_alias' in item and not _isnan(item['parent_pool_alias']):
                        try:
                            parent_pool_object = iloop.Pool.one(where={'alias': item['parent_pool_alias'],
                                                                       'project': self.project})
                        except ItemNotFound:
                            raise ItemNotFound('missing pool %s' % item['parent_strain_alias'])
                    iloop.Pool.create(alias=item['pool_alias'],
                                      project=self.project,
                                      parent_pool=parent_pool_object,
                                      genotype=item['genotype_pool'],
                                      type=item['pool_type'])
                    pool_object = iloop.Pool.one(where={'alias': item['pool_alias'], 'project': self.project})
                parent_object = None
                if 'parent_strain_alias' in item and not _isnan(item['parent_strain_alias']):
                    try:
                        parent_object = iloop.Strain.one(where={'alias': item['parent_strain_alias'],
                                                                'project': self.project})
                    except ItemNotFound:
                        raise ItemNotFound('missing strain %s' % item['parent_strain_alias'])
                iloop.Strain.create(alias=item['strain_alias'],
                                    pool=pool_object,
                                    project=self.project,
                                    parent_strain=parent_object,
                                    is_reference=bool(item.get('is_reference', False)),
                                    organism=item['organism'],
                                    genotype=item['genotype'])


class ExperimentUploader(AbstractDataUploader):
    """uploader for experiment data
    """

    def __init__(self, project, type, sample_name, overwrite=True,
                 synonym_mapper=place_holder_compound_synonym_mapper):
        super(ExperimentUploader, self).__init__(project)
        self.overwrite = overwrite
        self.synonym_mapper = synonym_mapper
        self.type = type
        self.sample_name = sample_name
        self.experiment_keys = []
        self.assay_cols = ['unit', 'parameter', 'numerator_chebi', 'denominator_chebi']
        self.samples_df = None
        self.df = None

    def extra_transformations(self):
        self.df['numerator_chebi'] = self.df['numerator_compound_name'].apply(self.synonym_mapper)
        self.df['denominator_chebi'] = self.df['denominator_compound_name'].apply(self.synonym_mapper)
        self.df['test_id'] = self.df[self.assay_cols].apply(lambda x: '_'.join(str(i) for i in x), axis=1)
        if self.df[['sample_id', 'test_id']].duplicated().any():
            raise ValueError('found duplicated rows, should not have happened')

    def upload(self, iloop):
        pass

    def upload_experiment_info(self, iloop):
        conditions_keys = list(set(self.samples_df.columns.values).difference(set(self.experiment_keys)))
        grouped_experiment = self.samples_df.groupby('experiment')
        for exp_id, experiment in grouped_experiment:
            exp_info = experiment[self.experiment_keys].drop_duplicates()
            exp_info = next(exp_info.itertuples())
            try:
                existing = iloop.Experiment.one(where={'identifier': exp_id, 'project': self.project})
                timestamp = existing.date.strftime('%Y-%m-%d')
                if str(timestamp) != exp_info.date:
                    if not self.overwrite:
                        raise HTTPError('existing mismatching experiment %s' % exp_id)
                    else:
                        logger.info('archiving existing experiment {}'.format(exp_id))
                        existing.archive()
                        raise ItemNotFound
            except ItemNotFound:
                logger.info('creating new experiment {}'.format(exp_id))
                sample_info = experiment[conditions_keys].set_index(self.sample_name)
                conditions = _cast_non_str_to_float(experiment[self.experiment_keys].iloc[0].to_dict())
                conditions = {key: value for key, value in conditions.items() if not _isnan(value)}
                iloop.Experiment.create(project=self.project,
                                        type=self.type,
                                        identifier=exp_id,
                                        date=parse_date(exp_info.date),
                                        description=exp_info.description,
                                        attributes={'conditions': conditions,
                                                    'operation': sample_info.to_dict()['operation'],
                                                    'temperature': float(exp_info.temperature)})


class FermentationUploader(ExperimentUploader):
    """uploader for experiment and sample descriptions and associated physiology data

    require two files, one that tabulates the information about an experiment and the samples associated with that
    experiment, and one for the physiology data. Validate with 'sample_information_schema.json' and
    'physiology_schema.json' respectively. Upload first the experiment details (optionally overwrite any existing
    experiment with the same name first). Then upload the samples with associated  physiology data.

    :param project: project object
    :param samples_file_name: name of the csv file to read
    :param physiology_file_name: name of the csv file to read
    """

    def __init__(self, project, samples_file_name, physiology_file_name, custom_checks, overwrite=True,
                 synonym_mapper=place_holder_compound_synonym_mapper):
        super(FermentationUploader, self).__init__(project, type='fermentation', sample_name='reactor',
                                                   overwrite=overwrite, synonym_mapper=synonym_mapper)
        self.assay_cols.extend(['phase_start', 'phase_end'])
        self.experiment_keys = ['experiment', 'description', 'date', 'do', 'gas', 'gasflow', 'ph_set', 'ph_correction',
                                'stirrer', 'temperature']
        self.samples_df = inspected_data_frame(samples_file_name, 'sample_information', custom_checks=custom_checks)
        self.samples_df['sample_id'] = self.samples_df[['experiment', 'reactor']].apply(lambda x: '_'.join(x), axis=1)
        sample_ids = self.samples_df['sample_id'].copy()
        sample_ids.sort_values(inplace=True)
        physiology_validator = DataFrameInspector(physiology_file_name, 'physiology', custom_checks=custom_checks)
        with open(physiology_validator.schema) as json_schema:
            physiology_schema = json.load(json_schema)
        for sample_id in sample_ids:
            physiology_schema['fields'].append({
                'name': sample_id,
                'title': 'measurements for {}'.format(sample_id),
                'type': 'number'
            })
        physiology_validator.schema = json.dumps(physiology_schema)
        self.physiology_df = physiology_validator()
        sample_cols = ['sample_id', 'experiment', 'reactor', 'operation',
                       'feed_medium', 'batch_medium', 'strain']
        self.df = (pd.melt(self.physiology_df,
                           id_vars=['phase_start', 'phase_end', 'quantity', 'parameter',
                                    'denominator_compound_name', 'numerator_compound_name', 'unit'],
                           var_name='sample_id')
                   .merge(self.samples_df[sample_cols], on='sample_id'))
        self.extra_transformations()

    def upload(self, iloop):
        self.upload_experiment_info(iloop)
        self.upload_physiology(iloop)

    def upload_physiology(self, iloop):
        for exp_id, experiment in self.df.groupby(['experiment']):
            scalars = []
            sample_dict = {}
            experiment_object = iloop.Experiment.one(where={'identifier': exp_id, 'project': self.project})
            sample_info = experiment[['feed_medium', 'batch_medium', 'reactor', 'strain']].drop_duplicates()
            for sample in sample_info.itertuples():
                sample_dict[sample.reactor] = {
                    'name': sample.reactor,
                    'strain': iloop.Strain.one(where={'alias': sample.strain, 'project': self.project}),
                    'medium': iloop.Medium.one(where={'name': sample.batch_medium}),
                    'feed_medium': iloop.Medium.one(where={'name': sample.feed_medium})
                }
            for phase_num, phase in experiment.groupby(['phase_start', 'phase_end']):
                phase_object = get_create_phase(iloop, float(phase.phase_start.iloc[0]),
                                                float(phase.phase_end.iloc[0]), experiment_object)
                for test_id, assay in phase.groupby('test_id'):
                    row = assay.iloc[0].copy()
                    test = measurement_test(row.unit, row.parameter, row.numerator_chebi, row.denominator_chebi,
                                            row.quantity)
                    a_scalar = {
                        'measurements': {reactor.reactor: [float(reactor.value)] for reactor in assay.itertuples()},
                        'test': deepcopy(test),
                        'phase': phase_object
                    }
                    scalars.append(a_scalar)
            experiment_object.add_samples({'samples': sample_dict, 'scalars': scalars})


class ScreenUploader(ExperimentUploader):
    """uploader for screening data
    """

    def __init__(self, project, file_name, custom_checks, overwrite=True,
                 synonym_mapper=place_holder_compound_synonym_mapper):
        super(ScreenUploader, self).__init__(project, type='screening', sample_name='well',
                                             overwrite=overwrite, synonym_mapper=synonym_mapper)
        self.experiment_keys = ['project', 'experiment', 'description', 'date', 'temperature']
        self.df = inspected_data_frame(file_name, 'screen', custom_checks=custom_checks)
        self.df['project'] = self.project.code
        self.df['barcode'] = self.df[['project', 'experiment', 'plate_name']].apply(lambda x: '_'.join(x), axis=1)
        self.df['well'] = self.df[['row', 'column']].apply(lambda x: ''.join(str(y) for y in x), axis=1)
        self.df['sample_id'] = self.df[['barcode', 'well']].apply(lambda x: '_'.join(x), axis=1)
        self.samples_df = self.df
        self.df.dropna(0, subset=['value'], inplace=True)
        self.extra_transformations()

    def upload(self, iloop):
        self.upload_experiment_info(iloop)
        self.upload_plates(iloop)
        self.upload_screen(iloop)

    def upload_plates(self, iloop):
        for exp_id, experiment in self.df.groupby(['experiment']):
            experiment_object = iloop.Experiment.one(where={'identifier': exp_id, 'project': self.project})
            plates_df = self.df[['experiment', 'barcode', 'well', 'medium', 'strain', 'plate_model']].drop_duplicates()
            for barcode, plate in plates_df.groupby(['barcode']):
                plate_info = plate[['well', 'medium', 'strain']].set_index('well')
                contents = {}
                for well in plate_info.itertuples():
                    contents[well.Index] = {
                        'strain': iloop.Strain.one(where={'alias': well.strain, 'project': self.project}),
                        'medium': iloop.Medium.one(where={'name': well.medium})
                    }
                try:
                    plate = iloop.Plate.one(where={'barcode': barcode, 'project': self.project})
                    plate.update_contents(contents)
                except ItemNotFound:
                    iloop.Plate.create(barcode=barcode, experiment=experiment_object, contents=contents,
                                       type=plate.plate_model[0], project=self.project)

    def upload_screen(self, iloop):
        for exp_id, experiment in self.df.groupby(['experiment']):
            experiment_object = iloop.Experiment.one(where={'identifier': exp_id, 'project': self.project})
            sample_dict = {}
            scalars = []

            for barcode, plate in experiment.groupby(['barcode']):
                sample_info = plate[['sample_id', 'well']].drop_duplicates()
                plate_object = iloop.Plate.one(where={'barcode': barcode, 'project': self.project})
                for sample in sample_info.itertuples():
                    sample_dict[sample.sample_id] = {
                        'plate': plate_object,
                        'position': sample.well,
                    }

            for test_id, assay in experiment.groupby('test_id'):
                row = assay.iloc[0].copy()
                test = measurement_test(row.unit, row.parameter, row.numerator_chebi, row.denominator_chebi,
                                        row.quantity)

                a_scalar = {
                    'measurements': {sample.sample_id: [float(sample.value)] for sample in assay.itertuples()},
                    'test': deepcopy(test),
                }
                scalars.append(a_scalar)
            experiment_object.add_samples({'samples': sample_dict, 'scalars': scalars})


class XrefMeasurementUploader(ExperimentUploader):
    """uploader for data associated with an entity define in an external database, e.g. a sequence or a reaction
    """

    def __init__(self, project, file_name, custom_checks, subject_type, overwrite=True):
        super(XrefMeasurementUploader, self).__init__(project, type='fermentation', sample_name='sample_name',
                                                      overwrite=overwrite)
        self.experiment_keys = ['project', 'experiment', 'description', 'date', 'temperature']
        inspection_key = dict(protein='protein_abundances', reaction='fluxes')[subject_type]
        self.df = inspected_data_frame(file_name, inspection_key, custom_checks=custom_checks)
        self.df['project'] = self.project.code
        self.samples_df = self.df
        self.subject_type = subject_type
        self.df.dropna(0, subset=['value'], inplace=True)

    def upload(self, iloop):
        self.upload_experiment_info(iloop)
        self.upload_sample_info(iloop)
        self.upload_measurements(iloop)

    def upload_sample_info(self, iloop):
        sample_info = self.df[['experiment', 'medium', 'sample_name', 'strain']].drop_duplicates()
        for sample in sample_info.itertuples():
            experiment = iloop.Experiment.one(where={'identifier': sample.experiment, 'project': self.project})
            try:
                return iloop.Sample.one(where={'name': sample.sample_name, 'experiment': experiment})
            except ItemNotFound:
                logger.info('creating new sample {}'.format(sample.sample_name))
                medium = iloop.Medium.one(where={'name': sample.medium})
                strain = iloop.Strain.one(where={'alias': sample.strain, 'project': self.project})
                iloop.Sample.create(experiment=experiment,
                                    project=self.project,
                                    name=sample.sample_name,
                                    medium=medium,
                                    strain=strain)

    def upload_measurements(self, iloop):
        accessions_df = self.df['xref_id'].str.split(':', expand=True)
        accessions_df.columns = ['db_name', 'accession']
        self.df = self.df.join(accessions_df)
        measurement_grouping = self.df.groupby(['sample_name', 'phase_start', 'phase_end'])
        unique_df = measurement_grouping[['mode', 'db_name']].nunique()
        if (unique_df['mode'] != 1).any() or (unique_df['db_name'] != 1).any():
            raise ValueError('multiple mode/db_names in upload not supported')
        for grouping, df in measurement_grouping:
            sample_name, phase_start, phase_end = grouping
            experiment_object = iloop.Experiment.one(where={'identifier': df['experiment'].iat[0],
                                                            'project': self.project})
            sample_object = iloop.Sample.one(where={'name': sample_name, 'experiment': experiment_object})
            phase_object = get_create_phase(iloop, float(phase_start), float(phase_end),
                                            sample_object.experiment)
            sample_object.add_xref_measurements(phase=phase_object, type=self.subject_type,
                                                values=df['value'].tolist(),
                                                accessions=df['accession'].tolist(),
                                                db_name=df['db_name'].iat[0],
                                                mode=df['mode'].iat[0])


def _cast_non_str_to_float(dictionary):
    for key in dictionary:
        if not isinstance(dictionary[key], str):
            dictionary[key] = float(dictionary[key])
    return dictionary


def get_create_phase(iloop, start, end, experiment):
    try:
        phase_object = iloop.ExperimentPhase.one(where={'start': start, 'end': end,
                                                        'experiment': experiment})
    except ItemNotFound:
        phase_object = iloop.ExperimentPhase.create(experiment=experiment,
                                                    start=start,
                                                    end=end,
                                                    title='{}__{}'.format(start, end))
    return phase_object
