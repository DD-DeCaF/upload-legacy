from functools import lru_cache, partial
from goodtables import check
from potion_client.exceptions import ItemNotFound
import gnomic
import os
import pickle

from upload.constants import skip_list, synonym_to_chebi_name_dict, compound_skip
from upload import iloop_client, logger
from upload.settings import Default


class IloopCache:

    def __init__(self):
        with open(os.path.join(os.path.dirname(__file__), 'data/chebi.pickle'), 'rb') as compounds_pickle:
            compounds = pickle.load(compounds_pickle)
        self.cache_fun = {'protein': lambda iloop: frozenset(iloop.Xref.subset(type='protein')),
                          'reaction': lambda iloop: frozenset(iloop.Xref.subset(type='reaction')),
                          # would do this but extremely slow https://github.com/biosustain/iloop/issues/107
                          # 'compound': lambda: frozenset(x.chebi_name for x in
                          #                               self.iloop.ChemicalEntity.instances(per_page=100)),
                          'compound': lambda iloop: compounds,
                          'medium': lambda iloop: frozenset(x.name for x in iloop.Medium.instances()),
                          'experiment': lambda iloop: frozenset((x.identifier, x.project.id) for x in
                                                                iloop.Experiment.instances()),
                          'strain': lambda iloop: frozenset((x.alias, x.project.id) for x in iloop.Strain.instances())}
        self.identifiers = {}
        self.update(iloop_client(Default.ILOOP_API, Default.ILOOP_TOKEN), lite=False)

    def update(self, iloop, lite=False):
        """Update the cached identifiers

        :param iloop: iloop client
        :param lite: bool, update all identifiers or only those that tend to change (medium, experiment and strains)
        """
        objects = ['medium', 'experiment', 'strain'] if lite else list(self.cache_fun.keys())
        for obj in objects:
            self.identifiers[obj] = self.cache_fun[obj](iloop)
            len_ids = len(self.identifiers[obj])
            logger.info('{} {} identifiers cached'.format(len_ids, obj))

iloop_cache = IloopCache()


def check_safe_partial(func, *args, **keywords):
    new_function = partial(func, *args, **keywords)
    new_function.check = func.check
    return new_function


@lru_cache(maxsize=None)
def synonym_to_chebi_name(project, synonym):
    """ map a synonym to a chebi name using iloop and a static ad-hoc lookup table

    :param synonym: str, synonym for a compound
    :param project: ignored, necessary for symmetry with other lookup functions
    :return str: the chebi name of the (guessed) compound or COMPOUND_SKIP if the compound is to be ignored,
    e.g. not tracked by iloop. missing values/nan return string 'nan'
    """
    try:
        if synonym in skip_list:
            return compound_skip
        if synonym in synonym_to_chebi_name_dict:
            synonym = synonym_to_chebi_name_dict[synonym]
        elif synonym.lower() in synonym_to_chebi_name_dict:
            synonym = synonym_to_chebi_name_dict[synonym.lower()]
        exists = synonym in iloop_cache.identifiers['compound']
        lower_exists = synonym.lower() in iloop_cache.identifiers['compound']
    except AttributeError:
        return 'nan'
    if not exists and lower_exists:
        synonym = synonym.lower()
    if not exists and not lower_exists:
        raise ValueError('failed to map {} to chebi'.format(synonym))
    return synonym


def valid_experiment_identifier(project, identifier):
    assert (identifier, project.id) in iloop_cache.identifiers['experiment']


def valid_strain_alias(project, alias):
    assert (alias, project.id) in iloop_cache.identifiers['strain']


def valid_medium_name(project, name):
    assert name in iloop_cache.identifiers['medium']


def valid_reaction_identifier(project, identifier):
    assert identifier in iloop_cache.identifiers['reaction']


def valid_protein_identifier(project, identifier):
    assert identifier in iloop_cache.identifiers['protein']


@check('genotype-not-gnomic', type='structure', context='body', after='duplicate-row')
def genotype_not_gnomic(errors, columns, row_number, state):
    """ checker logging if any columns named genotype have rows with non-gnomic strings """
    gnomic_parser = gnomic.GnomicParser()
    for column in columns:
        if 'header' in column and 'genotype' in column['header']:
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


def identifier_unknown(project, entity, check_function, message, errors, columns, row_number):
    for column in columns:
        if 'header' in column and entity in column['header']:
            try:
                if column['value']:
                    check_function(project, column['value'])
            except (ValueError, ItemNotFound):
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


@check('compound-name-unknown', type='structure', context='body', after='duplicate-row')
def compound_name_unknown(project, errors, columns, row_number, state):
    """ checker logging if any columns with name containing 'compound_name' has rows with unknown compounds """
    message = (
        'Row {row_number} has unknown compound name "{value}" '
        'in column {column_number}, expected '
        'valid chebi name, see https://www.ebi.ac.uk/chebi/ '
    )
    identifier_unknown(
        None,
        'compound_name',
        synonym_to_chebi_name,
        message,
        errors, columns, row_number
    )


@check('experiment-identifier-unknown', type='structure', context='body', after='duplicate-row')
def experiment_identifier_unknown(project, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown experiment "{value}" '
               'in column {column_number} '
               'definition perhaps not uploaded yet')
    identifier_unknown(
        project,
        'experiment',
        valid_experiment_identifier,
        message,
        errors, columns, row_number
    )


@check('strain-alias-unknown', type='structure', context='body', after='duplicate-row')
def strain_alias_unknown(project, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown strain alias "{value}" '
               'in column {column_number} '
               'definition perhaps not uploaded yet')
    identifier_unknown(
        project,
        'strain',
        valid_strain_alias,
        message,
        errors, columns, row_number
    )


@check('medium-name-unknown', type='structure', context='body', after='duplicate-row')
def medium_name_unknown(project, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown medium name "{value}" '
               'in column {column_number} '
               'definition perhaps not uploaded yet ')
    identifier_unknown(
        None,
        'medium',
        valid_medium_name,
        message,
        errors, columns, row_number
    )


@check('reaction-id-unknown', type='structure', context='body', after='duplicate-row')
def reaction_id_unknown(project, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown reaction identifier "{value}" '
               'in column {column_number} '
               'definition perhaps not known to iloop')
    identifier_unknown(
        None,
        'xref_id',
        valid_reaction_identifier,
        message,
        errors, columns, row_number
    )


@check('protein-id-unknown', type='structure', context='body', after='duplicate-row')
def protein_id_unknown(project, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown protein identifier "{value}" '
               'in column {column_number} '
               'definition perhaps not known to iloop')
    identifier_unknown(
        None,
        'xref_id',
        valid_protein_identifier,
        message,
        errors, columns, row_number
    )
