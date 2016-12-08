import asyncio
from aiohttp import web
import aiohttp_cors
from upload import iloop_client, logger
from upload.settings import Default
import pandas as pd
from pandas.io.common import CParserError
import io
import requests
import json
from upload.upload import MediaUploader, StrainsUploader, ExperimentUploader
from tempfile import mkstemp
from upload import __version__
from potion_client.exceptions import ItemNotFound
from upload.checks import compound_name_unknown, medium_name_unknown, strain_alias_unknown, \
    experiment_identifier_unknown, synonym_to_chebi_name

iloop = iloop_client(Default.ILOOP_API, Default.ILOOP_TOKEN)


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
                                          custom_checks=[compound_name_unknown,
                                                         experiment_identifier_unknown,
                                                         medium_name_unknown,
                                                         strain_alias_unknown],
                                          synonym_mapper=synonym_to_chebi_name)
    except CParserError:
        return web.json_response(
            data={'valid': False, 'tables': [{'errors': [{'message': 'failed to parse csv file '}]}]})
    except ValueError as error:
        return web.json_response(data=json.loads(str(error)))
    try:
        uploader.upload(iloop=iloop)
    except ItemNotFound as error:
        print('fooo')
        return web.json_response(
            data={'valid': False, 'tables': [{'errors': [{'message': str(error)}]}]})
    else:
        return web.json_response(data={'valid': True})


async def hello(request):
    return web.Response(text='hi, this is upload v' + __version__)


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
