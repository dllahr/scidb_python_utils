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
        print "setup test array in database"
        unittest.TestCase.setUp(self)
        
        commands.getoutput("./setup.sh {}".format(self.array_name))
        
        
    def tearDown(self):
        print "tearDown"
#        commands.getoutput("iquery -naq \"remove({})\"".format(TestScidbReader.array_name))
        
#    def test_setup(self):
#        self.setUp()
        
    def test_read(self):
        print "run test reading array"
        scidb = scidbapi.connect("localhost", 1239)
        
        reader = scidb_read.scidb_reader.ScidbReader(scidb)
        
        reader.read("scan({})".format(TestScidbReader.array_name))
        
        for data in reader:
            print data
            
        reader.complete_query()
        
        scidb.disconnect()

    def no_test_experiment(self):
        scidb = scidbapi.connect("localhost", 1239)

#        create array test <val1:double> [i=0:3,2,0, j=0:3,1,0];
#        Query was executed successfully
#        AFL% store(build(test, i*4+j), test);
#        [[(0)],[(4)]];[[(1)],[(5)]];[[(2)],[(6)]];[[(3)],[(7)]];[[(8)],[(12)]];[[(9)],[(13)]];[[(10)],[(14)]];[[(11)],[(15)]]

        result = scidb.executeQuery("scan(test2)")
        
        desc = result.array.getArrayDesc()

        attr_iter_tuple_list = [(attr, result.array.getConstIterator(attr.getId())) 
                          for attr in desc.getAttributes()]
        first_iter = attr_iter_tuple_list[0][1]

        nc = 0
        while not first_iter.end():

            for attr_iter_tuple in attr_iter_tuple_list:
                attr = attr_iter_tuple[0] 
                attr_iter = attr_iter_tuple[1]
                
                if (attr.getName() == "EmptyTag"):
                    continue
                
                print "Getting iterator for attribute %d, chunk %d.\n" % (attr.getId(), nc)
                current_chunk = attr_iter.getChunk()
                chunk_iter = current_chunk.getConstIterator((scidbapi.swig.ConstChunkIterator.IGNORE_OVERLAPS) |
                                                          (scidbapi.swig.ConstChunkIterator.IGNORE_EMPTY_CELLS))
        
                printed=False
                while not chunk_iter.end():
                    if not chunk_iter.isEmpty(): 
                        dataitem = chunk_iter.getItem()
                        #print_dataitem(attr_coll[i].getType(), dataitem)
                        print swig.Value.getDouble(dataitem)
                        
                        printed=True
      
                    chunk_iter.increment_to_next()
                if printed:
                    print       # add an extra newline to separate the data from the next query

            nc += 1;
            for attr_iter_tuple in attr_iter_tuple_list:
                attr_iter_tuple[1].increment_to_next()


        scidb.completeQuery(result.queryID)
        scidb.disconnect()

if __name__ == '__main__':
    unittest.main()

