from functools import lru_cache
from goodtables import check
import numpy as np
from upload.constants import skip_list, synonym_to_chebi_name_dict, compound_skip
from upload import iloop_client
from upload.settings import Default
from potion_client.exceptions import ItemNotFound

iloop = iloop_client(Default.ILOOP_API, Default.ILOOP_TOKEN)


@lru_cache(maxsize=None)
def synonym_to_chebi_name(synonym):
    """ map a synonym to a chebi name using iloop and a static ad-hoc lookup table

    :param synonym: str, synonym for a compound
    :return str: the chebi name of the (guessed) compound or COMPOUND_SKIP if the compound is to be ignored,
    e.g. not tracked by iloop. missing values/nan return string 'nan'
    """
    if synonym == '' or synonym is np.nan:
        return 'nan'
    if synonym in skip_list:
        return compound_skip
    if synonym in synonym_to_chebi_name_dict:
        synonym = synonym_to_chebi_name_dict[synonym]
    elif synonym.lower() in synonym_to_chebi_name_dict:
        synonym = synonym_to_chebi_name_dict[synonym.lower()]
    compound = iloop.ChemicalEntity.instances(where={'chebi_name': synonym})
    compound_lower = iloop.ChemicalEntity.instances(where={'chebi_name': synonym.lower()})
    if len(compound) == 0 and len(compound_lower) > 0:
        compound = compound_lower
    if len(compound) != 1:
        raise ValueError('failed to map {} to chebi'.format(synonym))
    return compound[0].chebi_name


@lru_cache(maxsize=None)
def valid_experiment_identifier(identifier):
    iloop.Experiment.first(where={'identifier': identifier})


@lru_cache(maxsize=None)
def valid_strain_alias(alias):
    iloop.Strain.first(where={'alias': alias})


@lru_cache(maxsize=None)
def valid_medium_name(name):
    iloop.Medium.first(where={'name': name})


@check('compound-name-unknown', type='structure', context='body', after='duplicate-row')
def compound_name_unknown(errors, columns, row_number, state):
    """ checker logging if any columns with name containing 'compound_name' has rows with unknown compounds """
    for column in columns:
        if 'header' in column and 'compound_name' in column['header']:
            try:
                if column['value']:
                    synonym_to_chebi_name(column['value'])
            except ValueError:
                message = (
                    'Row {row_number} has unknown compound name "{value}" in column {column_number}, expected '
                    'valid chebi name, see https://www.ebi.ac.uk/chebi/ ')
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


@check('experiment-identifier-unknown', type='structure', context='body', after='duplicate-row')
def experiment_identifier_unknown(errors, columns, row_number, state):
    for column in columns:
        if 'header' in column and 'experiment' in column['header']:
            try:
                if column['value']:
                    valid_experiment_identifier(column['value'])
            except ItemNotFound:
                message = ('Row {row_number} has unknown experiment "{value}" '
                           'in column {column_number} '
                           'definition perhaps not uploaded yet')
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


@check('strain-alias-unknown', type='structure', context='body', after='duplicate-row')
def strain_alias_unknown(errors, columns, row_number, state):
    for column in columns:
        if 'header' in column and 'strain' in column['header']:
            try:
                if column['value']:
                    valid_strain_alias(column['value'])
            except ItemNotFound:
                message = ('Row {row_number} has unknown strain alias "{value}" '
                           'in column {column_number} '
                           'definition perhaps not uploaded yet')
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


@check('medium-name-unknown', type='structure', context='body', after='duplicate-row')
def medium_name_unknown(errors, columns, row_number, state):
    for column in columns:
        if 'header' in column and 'medium' in column['header']:
            try:
                if column['value']:
                    valid_medium_name(column['value'])
            except ItemNotFound:
                message = ('Row {row_number} has unknown medium name "{value}" '
                           'in column {column_number} '
                           'definition perhaps not uploaded yet ')
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
