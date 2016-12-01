import asyncio
from aiohttp import web
import aiohttp_cors
from upload import iloop_client, logger
from upload.settings import Default
from goodtables import check
from functools import lru_cache
import numpy as np
import pandas as pd
from pandas.io.common import CParserError
import io
import requests
import json
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
    if data['what'] not in ['strains', 'media', 'experiment']:
        raise web.HTTPBadRequest(text='{"status": "expected strains, media or samples/physiology component of post"}')
    uploader = None

    try:

        if data['what'] == 'media':
            content = data['file[0]'].file.read().decode()
            uploader = MediaUploader(project, write_temp_csv(content),
                                     custom_checks=[compound_name_unknown],
                                     synonym_mapper=synonym_to_chebi_name)
        if data['what'] == 'strains':
            content = data['file[0]'].file.read().decode()
            uploader = StrainsUploader(project, write_temp_csv(content))
        if data['what'] == 'experiment':
            content_samples = data['file[0]'].file.read().decode()
            content_physiology = data['file[1]'].file.read().decode()
            uploader = ExperimentUploader(project, write_temp_csv(content_samples),
                                          write_temp_csv(content_physiology),
                                          custom_checks=[compound_name_unknown],
                                          synonym_mapper=synonym_to_chebi_name)
    except CParserError:
        return web.json_response(
            data={'valid': False, 'tables': [{'errors': [{'message': 'failed to parse csv file '}]}]})
    except ValueError as error:
        return web.json_response(data=json.loads(str(error)))
    uploader.upload(iloop=iloop)
    return web.json_response(data={'valid': True})


async def hello(request):
    return web.Response(text='hello....')


app = web.Application()
app.router.add_route('POST', '/upload', upload)
app.router.add_route('GET', '/upload/hello', hello)

# Configure default CORS settings.
cors = aiohttp_cors.setup(app, defaults={
    "*": aiohttp_cors.ResourceOptions(
        allow_credentials=True,
        expose_headers="*",
        allow_headers="*",
    )
})

# Configure CORS on all routes.
for route in list(app.router.routes()):
    cors.add(route)


async def start(loop):
    await loop.create_server(app.make_handler(), '0.0.0.0', 8000)
    logger.info('Web server is up')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start(loop))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
