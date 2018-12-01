import os
# import json
import logging
from logging.handlers import RotatingFileHandler
# import pandas as pd
# import datetime
# import numpy as np
# from sqlalchemy import create_engine  # , Integer, String, Boolean, \
#    DateTime, Numeric
from config import Config
from file_processor import FileInfo
from db_processor import DataProc

# -- Configure logger --
# Create a log directory if it does not exist
if not os.path.exists('logs'):
    os.mkdir('logs')

logger = logging.getLogger('main')

# Hitchhiker's Guide and Requests library best practise
logger.addHandler(logging.NullHandler())

# create a file handler with rotating logs
handler = RotatingFileHandler(
        'logs/data_processor.log',
        maxBytes=102400,
        backupCount=10,
    )
# create a logging format
formatter = logging.Formatter(
    '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)

# Instantiate Config Class
c = Config()
# Sets log level based on environment configuration
if c.DEBUG == 1:
    # Set logger log level.
    logger.setLevel(logging.DEBUG)
    # Set handler log level.
    handler.setLevel(logging.DEBUG)
    logger.info('Logging set for DEBUG and higher is %s',
                logging.getLogger('main').isEnabledFor(logging.DEBUG))
    logger.info('Logging set for INFO and higher is %s',
                logging.getLogger('main').isEnabledFor(logging.INFO))
else:
    # Set logger log level.
    logger.setLevel(logging.INFO)
    # Set handler log level.
    handler.setLevel(logging.INFO)
    logger.info('Logging set for DEBUG and higher is %s',
                logging.getLogger('main').isEnabledFor(logging.DEBUG))
    logger.info('Logging set for INFO and higher is %s',
                logging.getLogger('main').isEnabledFor(logging.INFO))

logger.info(
    'Logging set for DEBUG and higher in file_processor is %s',
    logging.getLogger('main.file_processor').isEnabledFor(logging.DEBUG),
)
logger.info(
    'Logging set for INFO and higher in file_processor is %s',
    logging.getLogger('main.file_processor').isEnabledFor(logging.INFO),
)
# -- End logger Configuration --

# Instantiate FileInfo and DataProc Classes
f = FileInfo()
d = DataProc()
logger.info('** Begin main application ** ')
logger.info('-- Begin file name processing -- ')
# Get the full path filenames to be loaded
path_filenames = f.get_data_filenames()
logger.info('-- Begin loading data files and write tables to database --')
for fn in path_filenames:
    filename = fn.split('/')[-1].lower()
    table_name = filename.split('.')[0]
    logger.info('Processing file %s', filename)
    contents = f.file_reader(fn)
    if contents is None:
        pass
    else:
        logger.info('Read in data from file %s', filename)
        pdtype_dict = f.df_column_dtypes(contents)
        logger.info('Created the dictionary of dtypes from pandas dataframe')
        sqltype_val_dict = d.pdtype2sqltype_default(pdtype_dict)
        logger.info('Created dictionary of sqlalchemy types with typical '
                    'column values using a default mapping to pandas dtypes')
        json_file = d.TYPE_JSON_PATH + table_name + '_sqltype.json'
        logger.info('JSON file for type storage is %s', json_file)
        logger.info('Try to see if the sql type dictionary is ready for '
                    'writing the sql table')
        sqltype_dict, is_sql_ready = d.make_sqltype_dict_json(sqltype_val_dict,
                                                              json_file)
        logger.info('SQL Ready is %s', is_sql_ready)
        if is_sql_ready:
            sqltype_dict = d.write_sqltables(table_name, contents,
                                             sqltype_dict)
            logger.info('Wrote SQL table with name %s', table_name)
        else:
            logger.info('Sending user to interactive session')
            print('\n\n** Processing Data for Table: "{}"'.format(table_name))
            sqltype_val_dict = d.type_update_interactive(sqltype_dict)
            logger.info('User complete making updates.')
            sqltype_dict = d.make_sqltype_dict_initial(sqltype_val_dict,
                                                       json_file)
            logger.info('Created the sqlalchemy type dictionary for writing '
                        'table')
            sqltype_dict = d.write_sqltables(table_name, contents,
                                             sqltype_dict)
        logger.info('Wrote SQL table with name %s', table_name)
logger.info('** End main application ** ')
