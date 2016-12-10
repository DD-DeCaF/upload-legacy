import asyncio
from aiohttp import web
import aiohttp_cors
import pandas as pd
from pandas.io.common import CParserError
import io
import requests
import json
from functools import wraps, partial
from upload.upload import MediaUploader, StrainsUploader, ExperimentUploader
from tempfile import mkstemp
from potion_client.exceptions import ItemNotFound
from upload import iloop_client, __version__, logger
from upload.settings import Default
from upload.checks import compound_name_unknown, medium_name_unknown, strain_alias_unknown, \
    experiment_identifier_unknown, synonym_to_chebi_name


def call_iloop_with_token(f):
    @wraps(f)
    async def wrapper(request):
        api, token = Default.ILOOP_API, Default.ILOOP_TOKEN
        if 'Authorization' in request.headers:
            if 'Origin' in request.headers and 'cfb' in request.headers['Origin']:
                api = Default.ILOOP_BIOSUSTAIN
            token = request.headers['Authorization'].replace('Bearer ', '')
        iloop = iloop_client(api, token)
        return await f(request, iloop)
    return wrapper


def write_temp_csv(data_string):
    file_description, tmp_file_name = mkstemp(suffix='.csv')
    df = pd.read_csv(io.StringIO(data_string))
    with open(tmp_file_name, 'w') as tmp_file:
        df.to_csv(tmp_file, index=False)
    return tmp_file_name


@call_iloop_with_token
async def upload(request, iloop):
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
                                     custom_checks=[partial(compound_name_unknown, iloop)],
                                     synonym_mapper=partial(synonym_to_chebi_name, iloop))
        if data['what'] == 'strains':
            content = data['file[0]'].file.read().decode()
            uploader = StrainsUploader(project, write_temp_csv(content))
        if data['what'] == 'experiment':
            content_samples = data['file[0]'].file.read().decode()
            content_physiology = data['file[1]'].file.read().decode()
            uploader = ExperimentUploader(project, write_temp_csv(content_samples),
                                          write_temp_csv(content_physiology),
                                          custom_checks=[partial(compound_name_unknown, iloop),
                                                         partial(experiment_identifier_unknown, iloop),
                                                         partial(medium_name_unknown, iloop),
                                                         partial(strain_alias_unknown, iloop)],
                                          synonym_mapper=partial(synonym_to_chebi_name, iloop))
    except CParserError:
        return web.json_response(
            data={'valid': False, 'tables': [{'errors': [{'message': 'failed to parse csv file '}]}]})
    except ValueError as error:
        return web.json_response(data=json.loads(str(error)))
    try:
        uploader.upload(iloop=iloop)
    except ItemNotFound as error:
        return web.json_response(
            data={'valid': False, 'tables': [{'errors': [{'message': str(error)}]}]})
    else:
        return web.json_response(data={'valid': True})


async def hello(request):
    return web.Response(text='hi, this is upload v' + __version__)


ROUTE_CONFIG = [
    ('POST', '/upload', upload),
    ('GET', '/upload/hello', hello),
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
    cors.add(resource.add_route("GET", handler))


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
