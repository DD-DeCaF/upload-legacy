import asyncio
from aiohttp import web
from upload import iloop_client, logger
from upload.settings import Default
from goodtables import check
from functools import lru_cache
import numpy as np
import pandas as pd
import io
import requests
from upload.constants import skip_list, synonym_to_chebi_name_dict, compound_skip
from upload.upload import MediaUploader, StrainsUploader, ExperimentUploader
from tempfile import mkstemp

iloop = iloop_client(Default.ILOOP_API, Default.ILOOP_TOKEN)


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


@lru_cache(maxsize=2 ** 16)
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


def write_temp_csv(data_string):
    file_description, tmp_file_name = mkstemp(suffix='.csv')
    df = pd.read_csv(io.StringIO(data_string))
    with open(tmp_file_name, 'w') as tmp_file:
        df.to_csv(tmp_file, index=False)
    return tmp_file_name


async def upload(request):
    data = await request.post()
    try:
        project = iloop.Project.first(where={'code': data['project_id']})
    except requests.exceptions.HTTPError:
        raise web.HTTPBadRequest(text='{"status": "failed to resolve project identifier"}')
    if 'strains' not in data and 'media' not in data and not ('samples' in data and 'physiology' in data):
        raise web.HTTPBadRequest(text='{"status": "expected strains, media or samples/physiology component of post"}')
    uploader = None
    try:
        if 'media' in data:
            uploader = MediaUploader(project, write_temp_csv(data['media']),
                                     custom_checks=[compound_name_unknown],
                                     synonym_mapper=synonym_to_chebi_name)
        if 'strains' in data:
            uploader = StrainsUploader(project, write_temp_csv(data['strains']))
        if 'samples' in data and 'physiology' in data:
            uploader = ExperimentUploader(project, write_temp_csv(data['samples']),
                                          write_temp_csv(data['physiology']),
                                          custom_checks=[compound_name_unknown],
                                          synonym_mapper=synonym_to_chebi_name)
    except ValueError as error:
        raise web.HTTPBadRequest(text=str(error))
    uploader.upload(iloop=iloop)
    return web.json_response(data={'status': 'ok'})


async def hello(request):
    return web.Response(text='hello....')


app = web.Application()
app.router.add_route('POST', '/upload', upload)
app.router.add_route('GET', '/hello', hello)


async def start(loop):
    await loop.create_server(app.make_handler(), '0.0.0.0', 7000)
    logger.info('Web server is up')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start(loop))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
