import os
import json
from sqlalchemy import Integer, String, Boolean, DateTime, Numeric, \
    Float, Unicode
import logging

basedir = os.path.abspath(os.path.dirname(__file__))
logger = logging.getLogger('main.config')


class Config():
    def __init__(self):
        self.DEBUG = int(os.environ.get('DEBUG'))
        self.SQLALCHEMY_DATABASE_URI = '{}://{}:{}@localhost/{}'.format(
            os.environ.get('DB_DIALECT_DRIVER'),
            os.environ.get('DB_USER'),
            os.environ.get('DB_PASSWORD'),
            os.environ.get('DB_NAME'),
        )
        self.SQLALCHEMY_TRACK_MODIFICATIONS = bool(os.environ.get(
            'SQLALCHEMY_TRACK_MODIFICATIONS'))
        self.DATA_FILES = os.environ.get('DATA_FILES')
        self.DATAFILE_EXTS = json.loads(os.environ['DATAFILE_EXTS'])
        self.TYPE_JSON_PATH = os.environ.get('TYPE_JSON_PATH')
        logger.info('Config initialized')
        logger.info('DEBUG = %s', self.DEBUG)
        logger.info('SQLALCHEMY_DATABASE_URI = %s',
                    self.SQLALCHEMY_DATABASE_URI)
        logger.info('SQLALCHEMY_TRACK_MODIFICATIONS = %s',
                    self.SQLALCHEMY_TRACK_MODIFICATIONS)
        logger.info('DATA_FILES = %s', self.DATA_FILES)
        logger.info('DATAFILE_EXTS = %s', self.DATAFILE_EXTS)
        # Non-environment constants
        # Used for manual selection of type and updates from JSON
        self.sql_types = {
            'String': String,
            'Unicode': Unicode,
            'DateTime': DateTime,
            'Integer': Integer,
            'Float': Float,
            'Numeric': Numeric,
            'Boolean': Boolean,
        }
