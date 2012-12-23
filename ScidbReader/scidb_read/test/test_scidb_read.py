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

import sys
sys.path.append('/opt/scidb/12.10/lib/') 
import scidbapi
import unittest
import scidb_read.scidb_reader
import commands

class TestScidbReader(unittest.TestCase):
    array_name = "test_scidb_read"
                
    def setUp(self):
        unittest.TestCase.setUp(self)
        commands.getoutput("./setup.sh {}".format(self.array_name))
        
        
    def tearDown(self):
        unittest.TestCase.tearDown(self)
        commands.getoutput("iquery -naq \"remove({})\"".format(TestScidbReader.array_name))
        
        
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

