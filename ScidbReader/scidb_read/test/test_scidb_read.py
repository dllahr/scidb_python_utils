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
        
        commands.getoutput("./setup.sh {}".format(self.array_name))
        
        
    def tearDown(self):
        commands.getoutput("iquery -naq \"remove({})\"".format(TestScidbReader.array_name))
        
        
    def test_read(self):
        scidb = scidbapi.connect("localhost", 1239)
        
        reader = scidb_read.scidb_reader.ScidbReader(scidb)
        
        reader.read("scan({})".format(TestScidbReader.array_name))
        
        for data in reader:
            print data
            
        reader.complete_query()
        
        scidb.disconnect()


if __name__ == '__main__':
    unittest.main()

