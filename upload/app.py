import asyncio
from aiohttp import web
import aiohttp_cors
import pandas as pd
import csv
import re
from pandas.io.common import CParserError
import io
import requests
import json
from functools import wraps, partial
from upload.upload import (MediaUploader, StrainsUploader, FermentationUploader, ScreenUploader,
                           XrefMeasurementUploader, get_schema)
from tempfile import mkstemp
from potion_client.exceptions import ItemNotFound
from upload import iloop_client, __version__, logger
from upload.settings import Default
from upload.checks import (compound_name_unknown, medium_name_unknown, strain_alias_unknown,
                           reaction_id_unknown, protein_id_unknown, synonym_to_chebi_name, check_safe_partial,
                           iloop_cache)

UPLOAD_TYPES = frozenset(['strains', 'media', 'fermentation', 'screen', 'fluxes', 'protein_abundances'])


def call_iloop_with_token(f):
    @wraps(f)
    async def wrapper(request):
        api, token = Default.ILOOP_API, Default.ILOOP_TOKEN
        if 'Authorization' in request.headers:
            if 'Origin' in request.headers and 'cfb' in request.headers['Origin']:
                api = Default.ILOOP_BIOSUSTAIN
            token = request.headers['Authorization'].replace('Bearer ', '')
        iloop = iloop_client(api, token)
        response = await f(request, iloop)
        assert response.status == 200, 'call to iloop failed with {}'.format(response.status)
        return response

    return wrapper


def guess_delimiter(string):
    """ guess a delimiter in a csv file

    csv sniffer only works for syntactically correct csv files, this is more relaxed
    """
    options = ',;\t|'
    try:
        s = csv.Sniffer()
        delimiter = s.sniff(string, delimiters=options).delimiter
    except csv.Error:
        substring = string[0:min(len(string), 2000)]
        counts = [substring.count(d) for d in options]
        delimiter = options[counts.index(max(counts))]
    logger.info('using %s as delimiter' % delimiter)
    return delimiter


def write_temp_csv(content):
    file_description, tmp_file_name = mkstemp(suffix='.csv')
    if content.content_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or re.match(
            '^.*.xlsx{0,1}$', content.filename, flags=re.IGNORECASE):
        df = pd.read_excel(content.file)
    else:
        data_string = content.file.read().decode()
        delimiter = guess_delimiter(data_string)
        df = pd.read_csv(io.StringIO(data_string), delimiter=delimiter)
    with open(tmp_file_name, 'w') as tmp_file:
        df.to_csv(tmp_file, index=False)
    return tmp_file_name


@call_iloop_with_token
async def list_projects(request, iloop):
    projects = [{'display': project.name, 'value': project.id} for project in iloop.Project.instances()]
    return web.json_response(data=projects)


@call_iloop_with_token
async def upload(request, iloop):
    data = await request.post()
    try:
        project = iloop.Project(data['project_id'])
    except requests.exceptions.HTTPError:
        raise web.HTTPBadRequest(text='{"status": "failed to resolve project identifier {}"}'.format(data['project_id']))
    if data['what'] not in UPLOAD_TYPES:
        raise web.HTTPBadRequest(text='{"status": "expected {} component of post"}'.format(', '.join(UPLOAD_TYPES)))
    uploader = None
    iloop_cache.update(iloop, lite=True)
    try:
        if data['what'] == 'media':
            content = data['file[0]']
            uploader = MediaUploader(project, write_temp_csv(content),
                                     custom_checks=[check_safe_partial(compound_name_unknown, None)],
                                     synonym_mapper=partial(synonym_to_chebi_name, None))
        if data['what'] == 'strains':
            content = data['file[0]']
            uploader = StrainsUploader(project, write_temp_csv(content))
        if data['what'] == 'screen':
            content = data['file[0]']
            uploader = ScreenUploader(project, write_temp_csv(content),
                                      custom_checks=[check_safe_partial(compound_name_unknown, None),
                                                     check_safe_partial(medium_name_unknown, None),
                                                     check_safe_partial(strain_alias_unknown, project)],
                                      synonym_mapper=partial(synonym_to_chebi_name, None))
        if data['what'] == 'fermentation':
            content_samples = data['file[0]']
            content_physiology = data['file[1]']
            uploader = FermentationUploader(project, write_temp_csv(content_samples),
                                            write_temp_csv(content_physiology),
                                            custom_checks=[check_safe_partial(compound_name_unknown, None),
                                                           check_safe_partial(medium_name_unknown, None),
                                                           check_safe_partial(strain_alias_unknown, project)],
                                            synonym_mapper=partial(synonym_to_chebi_name, None))
        if data['what'] == 'fluxes':
            content = data['file[0]']
            uploader = XrefMeasurementUploader(project, write_temp_csv(content),
                                               custom_checks=[check_safe_partial(medium_name_unknown, None),
                                                              check_safe_partial(reaction_id_unknown, None),
                                                              check_safe_partial(strain_alias_unknown, project)],
                                               subject_type='reaction')
        if data['what'] == 'protein_abundances':
            content = data['file[0]']
            uploader = XrefMeasurementUploader(project, write_temp_csv(content),
                                               custom_checks=[check_safe_partial(medium_name_unknown, None),
                                                              check_safe_partial(protein_id_unknown, None),
                                                              check_safe_partial(strain_alias_unknown, project)],
                                               subject_type='protein')
    except CParserError:
        return web.json_response(
            data={'valid': False, 'tables': [{'errors': [{'message': 'failed to parse csv file '}]}]})
    except ValueError as error:
        return web.json_response(data=json.loads(str(error)))
    try:
        uploader.upload(iloop=iloop)
    except (ItemNotFound, requests.exceptions.HTTPError) as error:
        return web.json_response(
            data={'valid': False, 'tables': [{'errors': [{'message': str(error)}]}]})
    else:
        return web.json_response(data={'valid': True})


async def hello(request):
    return web.Response(text='hi, this is upload v' + __version__)


async def schema(request):
    what = request.match_info.get('what', None)
    if not what:
        raise ValueError('bad schema request')
    with open(get_schema(what)) as schema_file:
        schema_object = json.load(schema_file)
    return web.json_response(data=schema_object)


ROUTE_CONFIG = [
    ('POST', '/upload', upload),
    ('GET', '/upload/hello', hello),
    ('GET', '/upload/list_projects', list_projects),
    ('GET', '/upload/schema/{what}', schema),
]

app = web.Application()
# Configure default CORS settings.
cors = aiohttp_cors.setup(app, defaults={
    "*": aiohttp_cors.ResourceOptions(
        expose_headers="*",
        allow_headers="*",
        allow_credentials=True,
    )
})

for method, path, handler in ROUTE_CONFIG:
    resource = app.router.add_resource(path)
    cors.add(resource)
    cors.add(resource.add_route(method, handler))


async def start(loop):
    await loop.create_server(app.make_handler(), '0.0.0.0', 8001)
    logger.info('Web server is up')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start(loop))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
