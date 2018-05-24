# Copyright 2018 Novo Nordisk Foundation Center for Biosustainability, DTU.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from potion_client import Client
from potion_client.auth import HTTPBearerAuth
import requests
import numpy as np
from raven import Client as RavenClient

from .settings import Default


logging.config.dictConfig(Default.LOGGING)
raven_client = RavenClient(Default.SENTRY_DSN)


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
