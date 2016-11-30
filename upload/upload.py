import pandas as pd
import numpy as np
from datetime import datetime
from potion_client.exceptions import ItemNotFound
from goodtables import Inspector, check
import json
from os.path import abspath, dirname, join, exists
import gnomic
from requests import HTTPError
from copy import deepcopy

from upload.constants import measurement_test, compound_skip


@check('genotype-not-gnomic', type='structure', context='body', after='duplicate-row')
def genotype_not_gnomic(errors, columns, row_number, state):
    """ checker logging if any columns named genotype have rows with non-gnomic strings """
    gnomic_parser = gnomic.GnomicParser()
    for column in columns:
        if 'header' in column and column['header'] == 'genotype':
            try:
                gnomic_parser.parse(column['value'])
            except gnomic.GrakoException:
                message = 'Row {row_number} has bad expected gnomic string "{value}" in column {column_number}'
                message = message.format(
                    row_number=row_number,
                    column_number=column['number'],
                    value=column['value'])
                errors.append({
                    'code': 'bad-value',
                    'message': message,
                    'row-number': row_number,
                    'column-number': column['number'],
                })


def place_holder_compound_synonym_mapper(synonym):
    return synonym


class DataFrameInspector(object):
    """ class for inspecting a table and reading it to a DataFrame


    :param file_name: name of the csv file to read
    :param schema_name: name of the json file specifying the scheme, possibly one of the schema in this package
    without path
    :param custom_checks: list of additional custom check functions to apply
    """

    def __init__(self, file_name, schema_name, custom_checks=None):
        schema_dir = abspath(join(dirname(abspath(__file__)), "data", "schemas"))
        self.schema = join(schema_dir, schema_name) if not exists(schema_name) else schema_name
        if not exists(self.schema):
            raise FileNotFoundError('missing schema %s' % self.schema)
        self.file_name = file_name
        self.custom_checks = custom_checks if custom_checks else []

    def inspect(self):
        """ inspect the data frame and return an error report """
        inspector = Inspector(custom_checks=self.custom_checks)
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

    :param project: project code
    :param file_name: name of the csv file to read
    """

    def __init__(self, project, file_name, custom_checks, synonym_mapper=place_holder_compound_synonym_mapper):
        super(MediaUploader, self).__init__(project)
        self.df = inspected_data_frame(file_name, 'media_schema.json', custom_checks=custom_checks)
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
            media_object = None
            try:
                current = iloop.Medium.read_find_media_with_ingredients(supplements=ingredients)
                if not any(medium_name == current_medium.name for current_medium in current):
                    media_object = iloop.Medium.create(**item)
                    media_object.update_contents(ingredients)
            except HTTPError:
                if media_object:
                    media_object.archive()
                raise


class StrainsUploader(AbstractDataUploader):
    """upload strain definitions

    inspect file using 'strains_schema.json' then sort the input data frame to make sure that parents are created
    before their children to avoid broken links.

    :param project: project code
    :param file_name: name of the csv file to read
    """

    def __init__(self, project, file_name):
        super(StrainsUploader, self).__init__(project)
        self.df = inspected_data_frame(file_name, 'strains_schema.json', custom_checks=[genotype_not_gnomic])
        self.iloop_args = []
        self.prepare_upload()

    def prepare_upload(self):
        def depth(df, i):
            if df.loc[i].parent is np.nan:
                return 0
            else:
                try:
                    return depth(df, df[df.strain == df.loc[i].parent].index[0]) + 1
                except IndexError:
                    return 0  # parent assumed to already be defined

        self.df['depth'] = [depth(self.df, i) for i in self.df.index]
        self.df = self.df.sort_values(by='depth')
        for strain in self.df.itertuples():
            genotype = '' if str(strain.genotype) == 'nan' else strain.genotype
            self.iloop_args.append({
                'strain_alias': strain.strain,
                'parent_strain_alias': strain.parent,
                'pool': strain.pool,
                'project': self.project,
                'is_reference': bool(strain.reference),
                'organism': strain.organism,
                'genotype': genotype
            })

    def upload(self, iloop):
        for item in self.iloop_args:
            try:
                iloop.Strain.first(where={'alias': item['strain_alias'], 'project': item['project']})
            except ItemNotFound:
                pool_object = iloop.Pool.first(where={'identifier': item['pool']})
                if item['parent_strain_alias'] is not np.nan:
                    parent_object = iloop.Strain.first(where={'alias': item['parent_strain_alias']})
                else:
                    parent_object = None
                iloop.Strain.create(alias=item['strain_alias'],
                                    pool=pool_object,
                                    project=self.project,
                                    parent_strain=parent_object,
                                    is_reference=bool(item['is_reference']),
                                    organism=item['organism'],
                                    genotype=item['genotype'])


class ExperimentUploader(AbstractDataUploader):
    """uploader for experiment and sample descriptions and associated physiology data

    require two files, one that tabulates the information about an experiment and the samples associated with that
    experiment, and one for the physiology data. Validate with 'sample_information_schema.json' and
    'physiology_schema.json' respectively. Upload first the experiment details (optionally overwrite any existing
    experiment with the same name first). Then upload the samples with associated  physiology data.

    :param project: project code
    :param samples_file_name: name of the csv file to read
    :param physiology_file_name: name of the csv file to read
    """

    def __init__(self, project, samples_file_name, physiology_file_name, custom_checks, overwrite=True,
                 synonym_mapper=place_holder_compound_synonym_mapper):
        super(ExperimentUploader, self).__init__(project)
        self.overwrite = overwrite
        self.samples_df = inspected_data_frame(samples_file_name,
                                               'sample_information_schema.json')
        self.samples_df['sample_id'] = self.samples_df[['experiment', 'reactor']].apply(lambda x: '_'.join(x), axis=1)
        self.synonym_mapper = synonym_mapper
        sample_ids = self.samples_df['sample_id'].copy()
        sample_ids.sort_values(inplace=True)
        physiology_validator = DataFrameInspector(physiology_file_name, 'physiology_schema.json',
                                                  custom_checks=custom_checks)
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
        self.df = (
            pd.melt(self.physiology_df,
                    id_vars=['phase_start', 'phase_end', 'parameter',
                             'denominator_compound_name',
                             'numerator_compound_name', 'unit'],
                    var_name='sample_id')
                .merge(self.samples_df[sample_cols], on='sample_id')
        )

        self.df['numerator_chebi'] = self.df['numerator_compound_name'].apply(self.synonym_mapper)
        self.df['denominator_chebi'] = self.df[
            'denominator_compound_name'].apply(self.synonym_mapper)
        assay_cols = ['unit', 'parameter', 'numerator_chebi',
                      'denominator_chebi']
        self.df['test_id'] = self.df[assay_cols].apply(
            lambda x: '_'.join(str(i) for i in x), axis=1)
        if self.df[['sample_id', 'test_id']].duplicated().any():
            raise ValueError('found duplicated rows, should not have happened')

    def upload(self, iloop):
        self.upload_experiment_info(iloop)
        self.upload_physiology(iloop)

    def upload_experiment_info(self, iloop):
        experiment_keys = ['project', 'experiment', 'description',
                           'date', 'do', 'gas', 'gasflow', 'ph_set', 'ph_correction', 'stirrer', 'temperature']

        conditions_keys = list(set(self.samples_df.columns.values).difference(set(experiment_keys)))
        grouped_experiment = self.samples_df.groupby('experiment')
        for exp_id, experiment in grouped_experiment:
            exp_info = experiment[experiment_keys].drop_duplicates()
            exp_info = next(exp_info.itertuples())
            try:
                existing = iloop.Experiment.first(where={'identifier': exp_id})
                timestamp = existing.date.strftime('%Y-%m-%d')
                if str(timestamp) != exp_info.date and not self.overwrite:
                    raise ValueError('existing mismatching experiment with same identifier')
                elif self.overwrite:
                    existing.archive()
                    raise ItemNotFound
            except ItemNotFound:
                sample_info = experiment[conditions_keys].set_index('reactor')
                conditions = _cast_non_str_to_float(experiment[experiment_keys].iloc[0].to_dict())
                iloop.Experiment.create(project=self.project,
                                        type='fermentation',
                                        identifier=exp_id,
                                        date=datetime.strptime(exp_info.date, '%Y-%m-%d'),
                                        description=exp_info.description,
                                        attributes={'conditions': conditions,
                                                    'operation': sample_info.to_dict()['operation'],
                                                    'temperature': float(exp_info.temperature)})

    def upload_physiology(self, iloop):
        for phase_num, phase in self.df.groupby(['phase_start', 'phase_end', 'experiment']):
            experiment_object = iloop.Experiment.first(where={'identifier': phase.experiment.iloc[0]})
            try:
                phase_object = iloop.ExperimentPhase.first(where={'start': int(phase.phase_start.iloc[0]),
                                                                  'end': int(phase.phase_end.iloc[0]),
                                                                  'experiment': experiment_object})
            except ItemNotFound:
                phase_object = iloop.ExperimentPhase.create(experiment=experiment_object,
                                                            start=int(phase.phase_start.iloc[0]),
                                                            end=int(phase.phase_end.iloc[0]),
                                                            title='{}__{}'.format(phase.phase_start.iloc[0],
                                                                                  phase.phase_end.iloc[0]))
            scalars = []
            for test_id, assay in phase.groupby('test_id'):
                row = assay.iloc[0].copy()
                test = measurement_test(row.unit, row.parameter, row.numerator_chebi, row.denominator_chebi)
                a_scalar = {
                    'measurements': {reactor.reactor: [float(reactor.value)] for reactor in assay.itertuples()},
                    'test': deepcopy(test),
                    'phase': phase_object
                }
                scalars.append(a_scalar)
            sample_info = phase[['feed_medium', 'batch_medium', 'reactor', 'strain']].drop_duplicates()
            sample_dict = {}
            for sample in sample_info.itertuples():
                sample_dict[sample.reactor] = {
                    'name': sample.reactor,
                    'strain': iloop.Strain.first(where={'alias': sample.strain}),
                    'medium': iloop.Medium.first(where={'name': sample.batch_medium}),
                    'feed_medium': iloop.Medium.first(where={'name': sample.feed_medium})
                }
            experiment_object.add_samples({'samples': sample_dict, 'scalars': scalars})


def _cast_non_str_to_float(dictionary):
    for key in dictionary:
        if not isinstance(dictionary[key], str):
            dictionary[key] = float(dictionary[key])
    return dictionary
