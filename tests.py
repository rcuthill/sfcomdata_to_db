import unittest
import os
import json
import file_processor as fp
import db_processor as dp
import pandas as pd


class FileTestCase(unittest.TestCase):
    def test_constants(self):
        c = fp.FileInfo()
        self.assertEqual(c.DEBUG, int(os.environ.get('DEBUG')))
        self.assertEqual(c.DATA_FILES, os.environ.get('DATA_FILES'))
        self.assertCountEqual(c.DATAFILE_EXTS,
                              json.loads(os.environ['DATAFILE_EXTS']))
        self.assertListEqual(c.DATAFILE_EXTS,
                             json.loads(os.environ['DATAFILE_EXTS']))

    def test_read_filenames_from_dirpath(self):
        f = fp.FileInfo()
        # Uncomment "f.DATA_FILES = 'test_data/no_duplicates/'"  AND
        # Comment out "f.DATA_FILES = 'test_data/no_duplicates/'
        # To confirm trapping an error for duplicate filenames stops excution
        # with suitable instructions.
        # f.DATA_FILES = 'test_data/duplicates/'
        #
        # The following setting tests the test cases below.
        f.DATA_FILES = 'test_data/no_duplicates/'
        expected_filenames = [
            f.DATA_FILES + 'one.csv',
            f.DATA_FILES + 'one.xls',
            f.DATA_FILES + 'one.xlsx',
            f.DATA_FILES + 'two.csv',
            f.DATA_FILES + 'mixed_ext/one_1.csv',
            f.DATA_FILES + 'mixed_ext/one.two.three_1.csv',
            f.DATA_FILES + 'mixed_ext/one_1.xls',
            f.DATA_FILES + 'mixed_ext/one_1.xlsx',
        ]
        f.DATAFILE_EXTS = [".csv", "csv", "xls", "xlsx"]
        # test to see if the same object type.  Only need to do once.
        self.assertTrue(
            type(f.get_data_filenames()) == type(expected_filenames)
        )
        self.assertCountEqual(f.get_data_filenames(), expected_filenames)
        f.DATAFILE_EXTS = [".csv", "xls", "xlsx"]
        self.assertCountEqual(f.get_data_filenames(), expected_filenames)
        f.DATAFILE_EXTS = ["csv", "xls", "xlsx"]
        self.assertCountEqual(f.get_data_filenames(), expected_filenames)
        f.DATAFILE_EXTS = [".csv"]
        expected_filenames = [
            f.DATA_FILES + 'one.csv',
            f.DATA_FILES + 'two.csv',
            f.DATA_FILES + 'mixed_ext/one_1.csv',
            f.DATA_FILES + 'mixed_ext/one.two.three_1.csv',
        ]
        self.assertCountEqual(f.get_data_filenames(), expected_filenames)
        f.DATAFILE_EXTS = ["csv"]
        self.assertCountEqual(f.get_data_filenames(), expected_filenames)

    def test_iter_3sample(self):
        from file_processor import iter_3sample
        iterable = [
            'one',
            'two',
            'three',
            'four',
            'five',
            'six',
            'seven',
            'eight',
        ]
        expected_results = (8, 'one', 'five', 'eight')
        self.assertTupleEqual(iter_3sample(iterable), expected_results)
        iterable = [
            'one',
            'two',
            'three',
            'four',
            'five',
            'six',
            'seven',
        ]
        expected_results = (7, 'one', 'four', 'seven')
        self.assertTupleEqual(iter_3sample(iterable), expected_results)
        iterable = [
            'one',
            'two',
            'three',
        ]
        expected_results = (3, 'one', 'two', 'three')
        self.assertTupleEqual(iter_3sample(iterable), expected_results)
        iterable = [
            'one',
            'two',
        ]
        expected_results = (2, 'one', 'two')
        self.assertTupleEqual(iter_3sample(iterable), expected_results)
        iterable = [
            'one',
        ]
        expected_results = (1, 'one')
        self.assertTupleEqual(iter_3sample(iterable), expected_results)

    def test_read_csvfile_using_pandas(self):
        f = fp.FileInfo()
        # f.DATA_FILES = 'test_data/duplicates/'
        path_filename = 'test_data/no_duplicates/one.csv'
        dict_for_pd = {
            'Id': ['0032A00002OVVEGQA5'],
            'Count': [12],
            'Subject': ['Shipments'],
            'IsActive': [0],
            'Dollars': [123.45],
            'CreateDate': ['2018-05-09 18:10:39'],
        }
        contents_pd = pd.DataFrame.from_dict(dict_for_pd).set_index('Id')
        contents_csv = f.file_reader(path_filename)
        pd.testing.assert_frame_equal(contents_pd, contents_csv)
        pd.testing.assert_index_equal(contents_pd.index,
                                      contents_csv.index)
        pd.testing.assert_series_equal(contents_pd.Count,
                                       contents_csv.Count)
        pd.testing.assert_series_equal(contents_pd.Subject,
                                       contents_csv.Subject)
        pd.testing.assert_series_equal(contents_pd.IsActive,
                                       contents_csv.IsActive)
        pd.testing.assert_series_equal(contents_pd.Dollars,
                                       contents_csv.Dollars)
        pd.testing.assert_series_equal(contents_pd.CreateDate,
                                       contents_csv.CreateDate)

    def test_dict_of_pdtype_typval_returned_from_df(self):
        f = fp.FileInfo()
        dict_for_pd = {
            'Id': ['0032A00002OVVEGQA5'],
            'Count': [12],
            'Subject': ['Shipments'],
            'IsActive': [0],
            'Dollars': [123.45],
            'CreateDate': ['2018-05-09 18:10:39'],
        }
        input_df = pd.DataFrame.from_dict(dict_for_pd).set_index('Id')
        expected_results = {
            'Count': ('int64', '12'),
            'Subject': ('object', 'Shipments'),
            'IsActive': ('int64', '0'),
            'Dollars': ('float64', '123.45'),
            'CreateDate': ('object', '2018-05-09 18:10:39')
        }
        self.assertDictEqual(f.df_column_dtypes(input_df), expected_results)


# TODO Complete testcases for db_processor
class DBTestCase(unittest.TestCase):
    def test_constants(self):
        c = dp.DataProc()
        self.assertEqual(c.DEBUG, int(os.environ.get('DEBUG')))
        self.assertEqual(c.DATA_FILES, os.environ.get('DATA_FILES'))
        self.assertCountEqual(c.DATAFILE_EXTS,
                              json.loads(os.environ['DATAFILE_EXTS']))
        self.assertListEqual(c.DATAFILE_EXTS,
                             json.loads(os.environ['DATAFILE_EXTS']))


if __name__ == '__main__':

    unittest.main(verbosity=2)
