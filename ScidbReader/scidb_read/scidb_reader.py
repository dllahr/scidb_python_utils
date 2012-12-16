'''
Created on Nov 27, 2012

@author: dlahr
'''
import sys
sys.path.append('/opt/scidb/12.10/lib/') 
import scidbapi
import csv

class ScidbReader:
    """provde methods to make queries to scidb and read results"""
    
    __scidb = None
    
    __query_result = None

    __attr_iter_tuple_list = None
    
    __attr_chunk_iter_tuple_list = None
    
    def __init__(self, scidb):
        """scidb is a scidb connection e.g. from scidbapi"""
        self.__scidb = scidb

    def read(self, query):
        """execute the provided query against scidb and provide an iterator
        over all the results
        """
        self.__query_result = self.__scidb.executeQuery(query)
        
        desc = self.__query_result.array.getArrayDesc()
        
        self.__attr_iter_tuple_list = [(attr, self.__query_result.array.getConstIterator(attr.getId())) 
                                       for attr in desc.getAttributes()]
        
        self.__update_attr_chunk_iter()


    def complete_query(self):
        self.__scidb.completeQuery(self.__query_result.queryID)
        

    def __iter__(self):
        return self
    
    def next(self):
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

        result = []
        for  attr_chunk_iter in self.__attr_chunk_iter_tuple_list:
            attr = attr_chunk_iter[0]
            chunk_iter = attr_chunk_iter[1]
                
            if (not chunk_iter.isEmpty()):
                result.append(scidbapi.getTypedValue(chunk_iter.getItem(), attr.getType()))
            else:
                result.append(None)
                    
            chunk_iter.increment_to_next()
                
        return result


    def __update_attr_chunk_iter(self):
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

