import logging
import sys
from potion_client import Client
from potion_client.auth import HTTPBearerAuth
import requests
import numpy as np
from raven import Client as RavenClient
from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler

from . import settings


logger = logging.getLogger('upload')
logger.addHandler(logging.StreamHandler(stream=sys.stdout))  # Logspout captures logs from stdout if docker containers
logger.setLevel(logging.DEBUG)

# Configure Raven to capture warning logs
raven_client = RavenClient(settings.Default.SENTRY_DSN)
handler = SentryHandler(raven_client)
handler.setLevel(logging.WARNING)
setup_logging(handler)


def iloop_client(api, token):
    requests.packages.urllib3.disable_warnings()
    return Client(
        api,
        auth=HTTPBearerAuth(token),
        verify=False
    )


def _isnan(value):
    if isinstance(value, str):
        return False
    return np.isnan(value)


__author__ = 'Henning Redestig'
__version__ = '0.4.1'
