from functools import lru_cache
from goodtables import check
import numpy as np
from upload.constants import skip_list, synonym_to_chebi_name_dict, compound_skip
from potion_client.exceptions import ItemNotFound


@lru_cache(maxsize=None)
def synonym_to_chebi_name(iloop, synonym):
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
def valid_experiment_identifier(iloop, identifier):
    iloop.Experiment.first(where={'identifier': identifier})


@lru_cache(maxsize=None)
def valid_strain_alias(iloop, alias):
    iloop.Strain.first(where={'alias': alias})


@lru_cache(maxsize=None)
def valid_medium_name(iloop, name):
    iloop.Medium.first(where={'name': name})


def identifier_unknown(iloop, entity, check_function, message,
                       errors, columns, row_number):
    for column in columns:
        if 'header' in column and entity in column['header']:
            try:
                if column['value']:
                    check_function(iloop, column['value'])
            except ItemNotFound:
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
def compound_name_unknown(iloop, errors, columns, row_number, state):
    """ checker logging if any columns with name containing 'compound_name' has rows with unknown compounds """
    message = (
        'Row {row_number} has unknown compound name "{value}" '
        'in column {column_number}, expected '
        'valid chebi name, see https://www.ebi.ac.uk/chebi/ '
    )
    identifier_unknown(
        iloop,
        'compound_name',
        synonym_to_chebi_name,
        message,
        errors, columns, row_number
    )


@check('experiment-identifier-unknown', type='structure', context='body', after='duplicate-row')
def experiment_identifier_unknown(iloop, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown experiment "{value}" '
               'in column {column_number} '
               'definition perhaps not uploaded yet')
    identifier_unknown(
        iloop,
        'experiment',
        valid_experiment_identifier,
        message,
        errors, columns, row_number
    )


@check('strain-alias-unknown', type='structure', context='body', after='duplicate-row')
def strain_alias_unknown(iloop, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown strain alias "{value}" '
               'in column {column_number} '
               'definition perhaps not uploaded yet')
    identifier_unknown(
        iloop,
        'strain',
        valid_strain_alias,
        message,
        errors, columns, row_number
    )


@check('medium-name-unknown', type='structure', context='body', after='duplicate-row')
def medium_name_unknown(iloop, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown medium name "{value}" '
               'in column {column_number} '
               'definition perhaps not uploaded yet ')
    identifier_unknown(
        iloop,
        'medium',
        valid_medium_name,
        message,
        errors, columns, row_number
    )
