"""Copyright (C) 2012 David L. Lahr

Permission is hereby granted, free of charge, to any person obtaining a copy of 
this software and associated documentation files (the "Software"), to deal in the 
Software without restriction, including without limitation the rights to use, copy, 
modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the
following conditions:

The above copyright notice and this permission notice shall be included in all copies
or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR 
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
DEALINGS IN THE SOFTWARE."""

import unittest
import scidb_load._utils_scidb_load
import sys
sys.path.append('/opt/scidb/12.10/lib/') 
import scidbapi

class TestUtilsScidbLoad(unittest.TestCase):
    __utils = None
    
    __scidb = None


    def setUp(self):
        self.__scidb = scidbapi.connect("localhost", 1239)
        self.__utils = scidb_load._utils_scidb_load.UtilsScidbLoad(self.__scidb)


    def tearDown(self):
        self.__scidb.disconnect()


    def test_remove_array_if_present(self):
        #attempt to remove an array that is not present
        result = self.__utils.remove_array_if_present("ok this is not a valid array name")
        assert False == result
    
        #build a an array for testing then remove it
        array_name = "test_array"
        result = self.__scidb.executeQuery("create array {} <val:double> [i=0:1,1,0]".format(array_name))
        self.__scidb.completeQuery(result.queryID)
        
        result = self.__utils.remove_array_if_present(array_name)
        assert True == result


    def test__build_min_max_statement(self):
        statement = self.__utils._build_min_max_statement(["a", "b"], "c")
        assert statement == "select min(a), max(a), min(b), max(b) from c"


    def test_get_min_and_max(self):
        array_name = "test_array"
        attribute_list = ["a", "b"]
        
        #remove the test array if it is present
        self.__utils.remove_array_if_present(array_name)
        
        #create the array to run the test on
        result = self.__scidb.executeQuery("create array {} <{}:double, {}:double> [i=0:1,1,0]"
                                           .format(array_name, attribute_list[0], attribute_list[1]))
        self.__scidb.completeQuery(result.queryID)
        
        #populate the test array with unique values
        #i, a, b
        #0, 1.0, 11.0
        #1, 3.0, 13.0
        result = self.__scidb.executeQuery("store( apply( build( <{}:double> [i=0:1,1,0], 2*i+1), {}, 10+{}), {})"
                                           .format(attribute_list[0], attribute_list[1], attribute_list[0], array_name))
        self.__scidb.completeQuery(result.queryID)
        #populate a dictionary with the expected results
        expected = {attribute_list[0]:(1.0, 3.0), attribute_list[1]:(11.0, 13.0)}
        
        #run the method under test
        result = self.__utils.get_min_and_max(attribute_list, array_name);
    
        #compare the expected results to those returned from the method under test
        for attribute in attribute_list:
            for i in range(2):
                self.assertAlmostEqual(expected[attribute][i], result[attribute][i])

        #clean up by removing the test array
        self.__utils.remove_array_if_present(array_name)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()