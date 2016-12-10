import os
import configparser
config = configparser.ConfigParser()
RC_FILE = os.path.expanduser('~/.camilorc')

if os.path.exists(os.path.expanduser(RC_FILE)):
    config.read(RC_FILE)

ILOOP_API = os.environ.get('ILOOP_API') or config.get('defaults', 'api', fallback=None)
ILOOP_TOKEN = os.environ.get('ILOOP_TOKEN') or config.get('defaults', 'token', fallback=None)


class Default(object):
    ILOOP_API = ILOOP_API
    ILOOP_TOKEN = ILOOP_TOKEN
    ILOOP_BIOSUSTAIN = 'https://iloop.biosustain.dtu.dk/api'
    NOT_PUBLIC = {'NPC'}
