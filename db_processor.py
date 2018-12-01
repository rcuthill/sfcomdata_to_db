'''DB processing intro'''
import os
import json
from sqlalchemy import create_engine
#     Numeric, Float, Unicode
from config import Config
import logging

logger = logging.getLogger('main.db_processor')


class DataProc(Config):
    def __init__(self):
        Config.__init__(self)
        logger.info('DataProc initialized with Config inheritance')
        logger.info('DEBUG = %s', self.DEBUG)
        logger.info('SQLALCHEMY_DATABASE_URI = %s',
                    self.SQLALCHEMY_DATABASE_URI)
        logger.info('SQLALCHEMY_TRACK_MODIFICATIONS = %s',
                    self.SQLALCHEMY_TRACK_MODIFICATIONS)
        logger.info('DATA_FILES = %s', self.DATA_FILES)
        logger.info('DATAFILE_EXTS = %s', self.DATAFILE_EXTS)
        logger.info('self.sql_types = %s', self.sql_types)

    def pdtype2sqltype_default(self, pdtype_dict):
        '''Creates a dict of default mapping from pandas dtype to sqlalchemy
        type.

        Accepts a dict of pandas dtypes by column and returns a dict of
        sqlalchemy types as string versus object.
        Also includes includes a typical data value associated with the column
        carried in from the pandas type dict.

        Only mapping pandas default types.  Pandas accepts python and numpy
        dtypes but in this application, the dataframe will always use the
        default types.
        '''
        default_type_map = {
                    'bool': 'Boolean',
                    'datetime64': 'DateTime',
                    'timedelta64': 'DateTime',
                    'int64': 'Integer',
                    'float64': 'Float',
                    'category': 'String',
                    'object': 'String',
        }
        logger.info('-- Begin default mapping from pd types to sql types --')
        logger.debug('pdtype_dict=> %s', pdtype_dict)
        sqltype_val_dict = {k: (default_type_map[v[0]], v[1])
                            for k, v in pdtype_dict.items()}
        logger.debug('sqltype_val_dict=> %s', sqltype_val_dict)
        logger.info('-- End default mapping from pd types to sql types --')
        return sqltype_val_dict

    def type_update_interactive(self, sqltype_val_dict):
        '''A terminal iteractive session to update a verbose version of a dict
        to use with sqlalchemy for creating tables.

        This verbose version includes the string and object sqlalchemy types
        and a typical data value associated with the column.  This typical
        value is carried forward from the pandas type dictionary.
        The input and return value is a verbose sqlalchemy type dict with the
        return dict having updated types per the user input.
        '''
        logger.info('- Entering type_update function -')
        cols = list(sqltype_val_dict.keys())
        logger.debug('Columns of type_dict are %s', cols)
        leave_loop = False
        while not leave_loop:
            logger.debug('Request input for a selection of columns.')
            print('\nCurrent configuration of type dictionary:\n')
            for c in cols:
                print(
                    '{}. Column: "{}" | Type: "{}" | Typical Value: '
                    '"{}"'.format(cols.index(c) + 1, c, sqltype_val_dict[c][0],
                                  sqltype_val_dict[c][1])
                )
            print('\nSelect column numbers, separated by spaces, of the '
                  'column types to be updated with a single type.')
            selected = input(
                'Input 1 to {} separated by spaces or "Enter" for no changes:'
                ' '.format(len(cols))
            )
            sel_subrange = set(selected.split())
            sel_range = set([str(i) for i in range(1, len(cols) + 1)])
            leave_loop = sel_subrange == set()
            # Need to identify bad input versus leaving loop. The intersection
            # set of the full selection and the selected being equal indicates
            # valid input. This equality includes the empty set; set(), which
            # is the result of the selection set when "Enter" is pressed with
            # no input. The intersection of an empty with any other set is also
            # an empty set. In this case, intersection of selected and the full
            # selection set being equal is True means valid input and includes
            # the selected empty set.
            valid_input = sel_subrange == sel_subrange & sel_range
            if not valid_input:
                logger.debug('Bad input "%s" request again.', sel_subrange)
                print('Invalid selection. Please try again.')
            if not leave_loop:
                cols_selected = [cols[int(istr) - 1] for istr in sel_subrange]
                logger.debug('Selected columns to update are %s',
                             cols_selected)
                type_selected = self.type_select()
                for c in cols_selected:
                    sqltype_val_dict[c] = type_selected, sqltype_val_dict[c][1]
                    logger.debug('Column "%s" updated to "%s"', c,
                                 type_selected)
                    logger.debug('Current type dict is %s', sqltype_val_dict)
            else:
                logger.debug('Input completed by "%s" input.', selected)
                # TODO Maybe store a version of type + typ val in JSON to
                # continue interactive session from where terminated.
                print('No further updates. Proceed with creating final '
                      'sqlalchemy type dict with type obects and writing to '
                      'database.')
        logger.info('- Leaving type_update function, '
                    'return updated type_dict -')
        return sqltype_val_dict

    def type_select(self):
        '''A terminal iteractive session to select an sqlalchemy type

        There is no input as the function contains valid sqlalchemy types,
        which are provided as a selection list to the user.  The selected
        type is returned as a tuple of the string and object version of the
        type.  e.g. ('String', String)
        '''
        logger.info('- Entering type_select function -')
        print('\nList of SQLAlchemy Types:')
        types_list = list(self.sql_types.keys())
        logger.debug('types_list is %s', types_list)
        for t in types_list:
            print('{}. {}'.format(types_list.index(t) + 1, t))
        leave_loop = False
        while not leave_loop:
            logger.debug('Request input for type selection')
            selected = input('Input 1 to {}: '.format(len(types_list)))
            sel_range = [str(i) for i in range(1, len(types_list) + 1)]
            # Include the empty string by input of "Enter" so b is True.
            # selrange.append('')
            leave_loop = selected in sel_range
            if not leave_loop:
                logger.debug('Bad input "%s" request again.', selected)
                print('Invalid selection. Please try again.')
        # if selected == '':
        #     type_number_selection = None
        # else:
        type_selection = types_list[int(selected) - 1]
        logger.debug('Selected type in type_select is %s', type_selection)
        logger.info('- Leaving type_select function -')
        return type_selection

    def make_sqltype_dict_initial(self, sqltype_val_dict, json_file):
        '''Creates the final sqlalchemy type dict to use during table
        creation

        Input the sqlalchemy dict type with value and full path JSON file name.
        Replace type string with the type object and remove the typical value.
        Pass back the simplified and final type dict and save it as a JSON file
        for future updates.
        '''
        logger.info('- Entering make_sqltype_dict function -')
        sqltype_dict = {k: self.sql_types[v[0]] for k, v in
                        sqltype_val_dict.items()}
        sqltype_dict_json = {k: v[0] for k, v in sqltype_val_dict.items()}
        logger.debug('Current sqltype dict is %s', sqltype_dict)
        logger.debug('Current sqltype dict for JSON is %s', sqltype_dict_json)
        sqltype_json = json.dumps(sqltype_dict_json, indent=4)
        with open(json_file, "w") as json_f:
            json_f.write(sqltype_json)
        logger.debug('Wrote to JSON file %s', json_file)
        logger.debug('Created sqltype_dict for sqlalchemy table create %s',
                     sqltype_dict)
        logger.info('- Leaving make_sqltype_dict function -')
        return sqltype_dict

    def make_sqltype_dict_json(self, sqltype_val_dict, json_file):
        '''Creates the final sqlalchemy type dict to use during table
        creation from stored JSON if it is valid.

        Input the default sqlalchemy dict and validate.  if valid, replace
        type string with the type object and remove the typical value.
        Pass back the simplified and final type dict.

        The stored JSON is invalid if,
        - No file exists => sqltype_dict = None
        - The column name sets of the default type dict and the stored JSON
          type dict do not match, then update the intersesction column
          string types (NOT replacing with type object)
         Both above conditions can be used to trigger an interactive session.
        '''
        logger.info('- Entering make_sqltype_dict_json function -')
        file_exists = os.path.isfile(json_file)
        logger.info('JSON file exist is %s', file_exists)
        if file_exists:
            with open(json_file, "r") as json_f:
                sqltype_dict_json = json.load(json_f)
            logger.debug('sqltype_dict_json read in is %s', sqltype_dict_json)
            cols_default = set(list(sqltype_val_dict.keys()))
            cols_json = set(list(sqltype_dict_json.keys()))
            cols_match = cols_default == cols_json
            logger.debug('Columns match is %s', cols_match)
            cols_intersect = cols_default & cols_json
            logger.debug('Columns intersect is %s', cols_intersect)
        else:
            cols_match = False
            cols_intersect = set()
        if cols_match:
            # Create sqltype_dict with type object
            # Will be used for SQL table create
            sqltype_dict = {k: self.sql_types[v] for k, v in
                            sqltype_dict_json.items()}
            is_sql_ready = True
            logger.info('sqltype_dict made in cols_match == True')
        elif cols_intersect != set():
            # Create a copy of the sqltype_val_dict
            sqltype_dict = {k: v for k, v in sqltype_val_dict.items()}
            # Update the type from the JSON for common fields
            # but leave the typical value in place.
            # Will be used for additional interactive editing
            is_sql_ready = False
            for c in cols_intersect:
                val = (sqltype_dict_json[c], sqltype_val_dict[c][1])
                sqltype_dict[c] = val
            logger.info('sqltype_dict made in cols_intersect is not empty')
        else:
            # Create a copy of the sqltype_val_dict
            sqltype_dict = {k: v for k, v in sqltype_val_dict.items()}
            # Likely never processed. Will be used for interactive editing
            is_sql_ready = False
        logger.debug('Exiting sqltype dict is %s', sqltype_dict)
        logger.info('- Leaving make_sqltype_dict_json function -')
        return sqltype_dict, is_sql_ready

    def write_sqltables(self, table_name, contents, sqltype_dict):
        # make connection to the database
        engine = create_engine(self.SQLALCHEMY_DATABASE_URI)
        # create table and write to database.
        # if_exists='replace' drops table and adds
        contents.to_sql(table_name, con=engine, if_exists='replace',
                        dtype=sqltype_dict)
        # Add primary key and in sf.com this is always "Id", if new table
        engine.execute('alter table "{}" add primary key("Id")'.format(
            table_name))
