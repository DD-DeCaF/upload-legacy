from functools import lru_cache, partial
from goodtables import check
from potion_client.exceptions import ItemNotFound
import gnomic

from upload.constants import skip_list, synonym_to_chebi_name_dict, compound_skip
from upload import iloop_client, logger
from upload.settings import Default


IDENTIFIER_TYPES = frozenset(['protein', 'reaction'])


def load_identifiers(type):
    iloop = iloop_client(Default.ILOOP_API, Default.ILOOP_TOKEN)
    return frozenset(iloop.Xref.subset(type=type))


class XrefIdentifiers:
    try:
        IDENTIFIERS = {
            type: load_identifiers(type) for type in IDENTIFIER_TYPES
        }
        logger.info('xref identifiers cached')
    except AttributeError:
        logger.info('failed caching xref identifiers, omics/xref upload disabled')


def check_safe_partial(func, *args, **keywords):
    new_function = partial(func, *args, **keywords)
    new_function.check = func.check
    return new_function


@lru_cache(maxsize=None)
def synonym_to_chebi_name(iloop, project, synonym):
    """ map a synonym to a chebi name using iloop and a static ad-hoc lookup table

    :param synonym: str, synonym for a compound
    :param iloop: the iloop object to call
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
        compound = iloop.ChemicalEntity.instances(where={'chebi_name': synonym})
        compound_lower = iloop.ChemicalEntity.instances(where={'chebi_name': synonym.lower()})
    except AttributeError:
        return 'nan'
    if len(compound) == 0 and len(compound_lower) > 0:
        compound = compound_lower
    if len(compound) != 1:
        raise ValueError('failed to map {} to chebi'.format(synonym))
    return compound[0].chebi_name


def valid_experiment_identifier(iloop, project, identifier):
    iloop.Experiment.one(where={'identifier': identifier, 'project': project})


def valid_strain_alias(iloop, project, alias):
    iloop.Strain.one(where={'alias': alias, 'project': project})


def valid_medium_name(iloop, project, name):
    iloop.Medium.one(where={'name': name})


def valid_reaction_identifier(iloop, project, identifier):
    if identifier not in XrefIdentifiers.IDENTIFIERS['reaction']:
        raise ValueError('not a valid reaction identifier')


def valid_protein_identifier(iloop, project, identifier):
    if identifier not in XrefIdentifiers.IDENTIFIERS['protein']:
        raise ValueError('not a valid protein identifier')


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


def identifier_unknown(iloop, project, entity, check_function, message,
                       errors, columns, row_number):
    for column in columns:
        if 'header' in column and entity in column['header']:
            try:
                if column['value']:
                    check_function(iloop, project, column['value'])
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
def compound_name_unknown(iloop, project, errors, columns, row_number, state):
    """ checker logging if any columns with name containing 'compound_name' has rows with unknown compounds """
    message = (
        'Row {row_number} has unknown compound name "{value}" '
        'in column {column_number}, expected '
        'valid chebi name, see https://www.ebi.ac.uk/chebi/ '
    )
    identifier_unknown(
        iloop,
        None,
        'compound_name',
        synonym_to_chebi_name,
        message,
        errors, columns, row_number
    )


@check('experiment-identifier-unknown', type='structure', context='body', after='duplicate-row')
def experiment_identifier_unknown(iloop, project, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown experiment "{value}" '
               'in column {column_number} '
               'definition perhaps not uploaded yet')
    identifier_unknown(
        iloop,
        project,
        'experiment',
        valid_experiment_identifier,
        message,
        errors, columns, row_number
    )


@check('strain-alias-unknown', type='structure', context='body', after='duplicate-row')
def strain_alias_unknown(iloop, project, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown strain alias "{value}" '
               'in column {column_number} '
               'definition perhaps not uploaded yet')
    identifier_unknown(
        iloop,
        project,
        'strain',
        valid_strain_alias,
        message,
        errors, columns, row_number
    )


@check('medium-name-unknown', type='structure', context='body', after='duplicate-row')
def medium_name_unknown(iloop, project, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown medium name "{value}" '
               'in column {column_number} '
               'definition perhaps not uploaded yet ')
    identifier_unknown(
        iloop,
        None,
        'medium',
        valid_medium_name,
        message,
        errors, columns, row_number
    )


@check('reaction-id-unknown', type='structure', context='body', after='duplicate-row')
def reaction_id_unknown(iloop, project, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown reaction identifier "{value}" '
               'in column {column_number} '
               'definition perhaps not known to iloop')
    identifier_unknown(
        iloop,
        None,
        'xref_id',
        valid_reaction_identifier,
        message,
        errors, columns, row_number
    )


@check('protein-id-unknown', type='structure', context='body', after='duplicate-row')
def protein_id_unknown(iloop, project, errors, columns, row_number, state):
    message = ('Row {row_number} has unknown protein identifier "{value}" '
               'in column {column_number} '
               'definition perhaps not known to iloop')
    identifier_unknown(
        iloop,
        None,
        'xref_id',
        valid_protein_identifier,
        message,
        errors, columns, row_number
    )
