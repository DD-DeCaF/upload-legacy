import logging

from aiohttp import web
from raven import Client
from raven.conf import setup_logging
from raven.handlers.logging import SentryHandler

from . import settings


# Configure Raven to capture warning logs
client = Client(settings.Default.SENTRY_DSN)
handler = SentryHandler(client)
handler.setLevel(logging.WARNING)
setup_logging(handler)


async def raven_middleware(app, handler):
    """aiohttp middleware which captures any uncaught exceptions to Sentry before re-raising"""
    async def middleware_handler(request):
        try:
            return await handler(request)
        except Exception:
            client.captureException()
            raise
    return middleware_handler
