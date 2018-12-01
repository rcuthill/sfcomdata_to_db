'''Introduction of file processor'''
import os
import sys
import pandas as pd
from config import Config
import logging

logger = logging.getLogger('main.file_processor')


def iter_3sample(an_iterable):
    '''Accepts a list, tuple or any indexed iterable and returns a tuple with
    the length of the iterable, first, middle and last item in the iterable.
    '''
    length = len(an_iterable)
    if length >= 3:
        sample = (length, an_iterable[0], an_iterable[length//2],
                  an_iterable[length-1])
    elif length == 2:
        sample = (length, an_iterable[0], an_iterable[1])
    elif length == 1:
        sample = (length, an_iterable[0])
    else:
        sample = (length)
    return sample


class FileInfo(Config):
    def __init__(self):
        Config.__init__(self)
        logger.info('FileInfo initialized with Config inheritance')
        logger.info('DEBUG = %s', self.DEBUG)
        logger.info('SQLALCHEMY_DATABASE_URI = %s',
                    self.SQLALCHEMY_DATABASE_URI)
        logger.info('SQLALCHEMY_TRACK_MODIFICATIONS = %s',
                    self.SQLALCHEMY_TRACK_MODIFICATIONS)
        logger.info('DATA_FILES = %s', self.DATA_FILES)
        logger.info('DATAFILE_EXTS = %s', self.DATAFILE_EXTS)
        logger.info('self.sql_types = %s', self.sql_types)

    def get_data_filenames(self):
        '''Recursively walks through a *ix system file directory and returns
        a list of file names having file extensions found in the iterable,
        file_extensions.

        Arguments:
        root_dir is the full path as a String where the recursive walk using
        os.walk starts.

        file_extensions is an iterable of Strings, where each string is a
        desired file extension with no leading "."  e.g. "csv" not ".csv"
        Though these are stripped in any case.
        '''
        root_dir = self.DATA_FILES
        file_extensions = self.DATAFILE_EXTS
        logger.info('Using %s to retrieve data file names', root_dir)
        logger.debug('File ext before stripping "." is %s', file_extensions)
        file_extensions = [f.strip(".") for f in file_extensions]
        logger.info(
            'Filtering file names that have one of these extensions: %s',
            file_extensions,
        )
        logger.warning('File extension strings containg a "." has '
                       'been removed')

        def is_valid_ext(filename):
            '''Determines if a filename has extension a valid extension

            A file name and an iterable of acceptable extensions are passed to
            the function and returns True if the file has an extension and is
            a member of the acceptable extensions.
            '''
            return (filename.split('.')[-1] in file_extensions and
                    len(filename.split('.')) > 1)
        # list comprehension of candidate file names
        path_files = [str(p).rstrip('/') + '/' + str(f)
                      for p, d, fs in os.walk(root_dir) for f in fs]
        logger.debug(
            'Sample candidate file names are %s. Total, 1st, middle and last',
            iter_3sample(path_files))
        # filter only file names with a file extension in file extensions
        filtered_path_files = list(filter(is_valid_ext, path_files))
        logger.debug(
            'Sample filtered file names are %s. Total, 1st, middle and last',
            iter_3sample(filtered_path_files))
        # Ensuring there are no duplicate file names in data file directory
        # structure
        filenames_pathremoved = [x.split('/')[-1] for x in filtered_path_files]
        reduced_filenames = list(set(filenames_pathremoved))
        if len(filenames_pathremoved) != len(reduced_filenames):
            print('\nThere are one more data files with the same name in the '
                  'directory structure under "{}".'.format(root_dir))
            print('\nPlease inspect the data file structure for duplicate '
                  'file names.')
            sys.exit('and correct so all data files are named uniquely. '
                     'Exiting.')
        return filtered_path_files

    def df_column_dtypes(self, pd_df):
        '''Accepts a pandas dataframe and returns a dict of pd dtypes with
        a typical value for each column.'''
        # column names from df
        col_names = list(pd_df.columns.values)
        logger.debug(
            'Sample df column names are %s. Total, 1st, middle and last',
            iter_3sample(col_names))
        # for each column find a typical value
        col_typical_vals = []
        for c in col_names:
            vals = list(pd_df[c])
            for v in vals:
                if v == 'nan':
                    typical_val = 'None'
                    pass
                else:
                    typical_val = str(v)
                    break
            col_typical_vals.append(typical_val)
        logger.debug(
            'Sample typical df values per column are %s. Total, 1st, '
            'middle and last', iter_3sample(col_typical_vals))
        # create a single list of tuples (column name, typical value)
        r = range(len(col_names))
        col_names_vals = [(col_names[i], col_typical_vals[i]) for i in r]
        logger.debug(
            'Sample (column name, typical value) are %s. Total, 1st, '
            'middle and last', iter_3sample(col_names_vals))
        # return a dict with column name: (dtype, typical value)
        return {k[0]: (pd_df[k[0]].dtype.name, k[1])
                for k in col_names_vals}

    def file_reader(self, path_filename):
            ext = path_filename.split('.')[-1]
            if ext == 'csv':
                contents = pd.read_csv(path_filename, index_col='Id',
                                       encoding='utf-8')
            else:
                print("\nFile extension {} not yet implemented".format(ext))
                print('Continuing with other file types.')
                contents = None
            return contents


if __name__ == '__main__':
    f = FileInfo()
    fn = 'something.csv'
    file_extensions = f.DATAFILE_EXTS
    f.get_data_filenames()
