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


class ScidbReader:
    """provde methods to make queries to scidb and read results"""
    
    __scidb = None
    
    __query_result = None

    __attr_iter_tuple_list = None
    
    __attr_chunk_iter_tuple_list = None
    
    def __init__(self, scidb):
        """scidb is a scidb connection e.g. from scidbapi"""
        self.__scidb = scidb

    def read(self, query, scidb_lang="afl"):
        """execute the provided query against scidb and provide an iterator
        over all the results
        """
        self.__query_result = self.__scidb.executeQuery(query, scidb_lang)
        
        desc = self.__query_result.array.getArrayDesc()
        
        self.__attr_iter_tuple_list = [(attr, self.__query_result.array.getConstIterator(attr.getId())) 
                                       for attr in desc.getAttributes()]
        
        self.__update_attr_chunk_iter()


    def complete_query(self):
        self.__scidb.completeQuery(self.__query_result.queryID)
        

    def __iter__(self):
        return self
    
    def next(self):
        """return the next row of data if it is available otherwise raise StopIteration"""
        
        first_chunk_iter = self.__attr_chunk_iter_tuple_list[0][1]
        
        #if the chunk is finished, try to increment to the next chunk
        if (first_chunk_iter.end()):
            #increment each attribute iterator to the next chunk
            for attr_iter_tuple in self.__attr_iter_tuple_list:
                attr_iter_tuple[1].increment_to_next()

            #attempt to update the chunk iterators based on the incremented
            #attribute iterators
            if self.__update_attr_chunk_iter():
                raise StopIteration

        #get dimension values at this position
        coordinates = self.__attr_chunk_iter_tuple_list[0][1].getPosition() #get the first tuple from the list,
                                                                            #then get the second item in the tuple
        pos_list = list()
        for i in range(len(coordinates)):
            pos_list.append(coordinates[i])

        #get attribute values at this position
        attr_values = []
        for  attr_chunk_iter in self.__attr_chunk_iter_tuple_list:
            attr = attr_chunk_iter[0]
            chunk_iter = attr_chunk_iter[1]

            if (not chunk_iter.isEmpty()):
                attr_values.append(scidbapi.getTypedValue(chunk_iter.getItem(), attr.getType()))
            else:
                attr_values.append(None)
                    
            chunk_iter.increment_to_next()
                
        return (pos_list, attr_values)


    def __update_attr_chunk_iter(self):
        """return True if the end has been reached otherwise return False"""
        self.__attr_chunk_iter_tuple_list = []
        
        end = self.__attr_iter_tuple_list[0][1].end()
        
        if not end:
            for attr_iter_tuple in self.__attr_iter_tuple_list:
                attr = attr_iter_tuple[0]
                attr_iter = attr_iter_tuple[1]
            
                current_chunk = attr_iter.getChunk()
                chunk_iter = current_chunk.getConstIterator((scidbapi.swig.ConstChunkIterator.IGNORE_OVERLAPS) |
                                                          (scidbapi.swig.ConstChunkIterator.IGNORE_EMPTY_CELLS))
                self.__attr_chunk_iter_tuple_list.append((attr, chunk_iter))
        
        return end

