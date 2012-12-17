'''
Created on Nov 27, 2012

@author: dlahr
'''
import sys
sys.path.append('/opt/scidb/12.10/lib/') 
import scidbapi
import unittest
import scidb_read.scidb_reader
import libscidbpython as swig
import commands

class TestScidbReader(unittest.TestCase):
    array_name = "test_scidb_read"
                
    def setUp(self):
        unittest.TestCase.setUp(self)
        
#        commands.getoutput("./setup.sh {}".format(self.array_name))
        
        
    def tearDown(self):
        unittest.TestCase.tearDown(self)
#        commands.getoutput("iquery -naq \"remove({})\"".format(TestScidbReader.array_name))
        
        
    def test_read(self):
        #connect to SciDB database
        scidb = scidbapi.connect("localhost", 1239)
        
        #instantiate reader by passing reference to database connection
        reader = scidb_read.scidb_reader.ScidbReader(scidb)
        
        #read for a specific query - in this case all the data in a previously setup array
        reader.read("scan({})".format(TestScidbReader.array_name))
        
        #iterate over the data returned by the query, printing it out
        #(data is returned as a list)
        for data in reader:
            print data
            
        #end the query
        reader.complete_query()
        
        #close the database connection
        scidb.disconnect()


if __name__ == '__main__':
    unittest.main()

