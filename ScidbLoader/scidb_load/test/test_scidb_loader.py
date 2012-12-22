'''
Created on Nov 27, 2012

@author: dlahr
'''
import sys
sys.path.append('/opt/scidb/12.10/lib/') 
import scidbapi
sys.path.append('..')
#import scidb_load.scidb_loader as scidb_loader
import scidb_load.scidb_loader as scidb_loader
import unittest
#import scidb_load.scidb_load_column as scidb_load_column
import scidb_load.scidb_load_column as scidb_load_column
import commands
import scidb_read.scidb_reader as scidb_reader
import scidb_load._utils_scidb_load as _utils_scidb_load

class TestScidbLoader(unittest.TestCase):
    def test_load(self):
        array_name = "testScidbLoaderArray"

        scidb = scidbapi.connect("localhost", 1239)
        
        utils = _utils_scidb_load.UtilsScidbLoad(scidb)
        utils.remove_array_if_present(array_name) 
        
        first_dimension = "a"
        second_dimension = "b"
        attribute = "c"
        #create the list of columns to be loaded
        scidb_load_column_list = [scidb_load_column.ScidbLoadColumn(first_dimension, "int64", False),
                                  scidb_load_column.ScidbLoadColumn(second_dimension, "int64", False),
                                  scidb_load_column.ScidbLoadColumn(attribute, "double", True)]

        loader = scidb_loader.ScidbLoader(scidb)
        
        #get the path to the csv holding the test data
        csv_file = "".join([commands.getoutput("pwd"), "/test_data.csv"])
        
        #load the data from the csv file into the database
        loader.load(scidb_load_column_list, csv_file, array_name)

        #compare what was loaded to what we expected to be loaded
        #first check format of created array
        reader = scidb_reader.ScidbReader(scidb)
        reader.read("show({})".format(array_name))
        result = reader.next()[1][0]
        reader.complete_query()
        
        self.assertTrue(result.find("[{}=".format(first_dimension)) >= 0)
        self.assertTrue(result.find(",{}=".format(second_dimension)) >= 0)
        self.assertTrue(result.find("<{}:double>".format(attribute)) >= 0)
        
        #second check values stored in array
        attribute_dimension_list = [first_dimension, second_dimension, attribute]
        #expected values from test_data.csv
        expected = {first_dimension:(1,13), second_dimension:(2,17), attribute:(3,19)}

        result = utils.get_min_and_max(attribute_dimension_list, array_name)
        
        for attr_dim in attribute_dimension_list:
            min_max_expected = expected[attr_dim]
            min_max_found = result[attr_dim]
            self.assertTupleEqual(min_max_expected, min_max_found)

        scidb.disconnect()


if __name__ == '__main__':
    unittest.main()
