'''
Created on Dec 22, 2012

@author: dlahr
'''
import scidb_read.scidb_reader

class UtilsScidbLoad:
    __scidb = None
    
    def __init__(self, scidb):
        """scidb is an instance of a connection to scidb from scidbapi"""
        self.__scidb = scidb
        
    def remove_array_if_present(self, array_name):
        """if array_name is present in the database, remove it.  Return True if it was found in the
        database, otherwise return False"""
        reader = scidb_read.scidb_reader.ScidbReader(self.__scidb)
        
        reader.read("count( filter( list(), name='{}'))".format(array_name))
        is_present = (1 == reader.next()[1][0])
        reader.complete_query()
        
        if is_present:
            result = self.__scidb.executeQuery("remove({})".format(array_name))
            self.__scidb.completeQuery(result.queryID)
            
        return is_present
    
    def get_min_and_max(self, attribute_dimension_list, array_name):
        """get the minimum and maximum values for each of the attributes specified
        in attributes_list from the array array_name.  Return as dictionary where
        the keys are the attributes from attribute_dimension_list and the values are tuples
        containing the min and max values found for that attribute"""
        statement = self._build_min_max_statement(attribute_dimension_list, array_name)
        
        reader = scidb_read.scidb_reader.ScidbReader(self.__scidb)
        reader.read(statement, "aql")
        
        values = reader.next()[1]
        
        reader.complete_query()
        
        result = dict()
        for i in range(len(attribute_dimension_list)):
            min_val = values[2*i]
            max_val = values[2*i + 1]
            
            result[attribute_dimension_list[i]] = min_val, max_val
        
        return result

        
    def _build_min_max_statement(self, attribute_list, array_name):
        statement = ", ".join(["min({}), max({})".format(x,x) for x in attribute_list])
        statement = "select {} from {}".format(statement, array_name)
        
        return statement
        
    
    