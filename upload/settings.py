import os


class Default(object):
    ILOOP_API = os.environ.get('ILOOP_API')
    ILOOP_TOKEN = os.environ.get('ILOOP_TOKEN')
    ILOOP_BIOSUSTAIN = 'https://iloop.biosustain.dtu.dk/api'
    NOT_PUBLIC = {'NPC'}
    SENTRY_DSN = os.environ.get('SENTRY_DSN', '')
