import csv_to_scidb
import _utils_scidb_load

class ScidbLoader:
    __target_chunk_size = 1000000
    __raw_array = "raw"

    __scidb = None

    __do_calculate_dimension_params = True

    _utils = None

    def __init__(self, scidb):
        self.__scidb = scidb
        self._utils = _utils_scidb_load.UtilsScidbLoad(self.__scidb)


    def load(self, scidb_load_col_list, csv_file, array_name):
        """load data from the indicated csv file (csv_file) into an array in SciDB
        named array_name with the attributes and dimensions specified by 
        scidb_load_col_list (list of ScidbLoadColumn)
        csv_file is assumed to have a header row
        """
        
        print "generate scidb file from csv file"
        scidb_file = csv_to_scidb.convert_csv_to_scidb(csv_file)

        self._utils.remove_array_if_present(self.__raw_array)
        
        raw_attr_list = [scidb_load_col.as_attribute() for scidb_load_col in scidb_load_col_list]

        raw_attr_query = "create array {} <{}> [line=0:*,1000000,0]".format(self.__raw_array, ", ".join(raw_attr_list))

        result = self.__scidb.executeQuery(raw_attr_query)
        self.__scidb.completeQuery(result.queryID)

        print "load data into raw in scidb"
        result = self.__scidb.executeQuery("load ({}, '{}')".format(self.__raw_array, scidb_file))
        self.__scidb.completeQuery(result.queryID)

        if (self.__do_calculate_dimension_params):
            self._calculate_dimensions(scidb_load_col_list)

        attribute_string = ", ".join([attr_col.as_attribute() for attr_col in 
                                      filter(lambda col: col.is_attribute, scidb_load_col_list)])
        #print attribute_string
        dimension_string = ", ".join([dim_col.as_dimension() for dim_col in
                                      filter(lambda col: not col.is_attribute, scidb_load_col_list)])
        #print dimension_string

        create_query = "create array {} <{}> [{}]".format(array_name, 
                                                          attribute_string, dimension_string)
        #print create_query

        result = self.__scidb.executeQuery(create_query)
        self.__scidb.completeQuery(result.queryID)

        print "redimension_store"
        result = self.__scidb.executeQuery("redimension_store({}, {})"
                                           .format(self.__raw_array, array_name))
        self.__scidb.completeQuery(result.queryID)


    def _calculate_dimensions(self, scidb_load_col_list):
        scidb_dim_col_list = filter(lambda col: not col.is_attribute, scidb_load_col_list)
        min_max_dim_list = [col.name for col in scidb_dim_col_list]
        
        min_max_dict = self._utils.get_min_and_max(min_max_dim_list, self.__raw_array)

        dim_size_list = list()

        #fill in know paramters for dimension: min, max, 0 overlap
        for scidb_dim_col in scidb_dim_col_list:
            min_max_tuple = min_max_dict[scidb_dim_col.name]
            scidb_dim_col.dimension_params = [min_max_tuple[0], min_max_tuple[1], None, 0]

            dim_size_list.append(min_max_tuple[1] - min_max_tuple[0] + 1)

        chunk_size_list = ScidbLoader._calculate_chunk_sizes(dim_size_list, self.__target_chunk_size)
    
        #fill in calculated chunk sizes into dimension columns
        for i in range(0, len(chunk_size_list)):
            scidb_dim_col_list[i].dimension_params[2] = chunk_size_list[i]
            


    @staticmethod
    def _calculate_chunk_sizes(dimension_sizes, target_chunk_size):
        """for the provided list of dimension sizes and the target total size of a chunk
        calculate the individual chunk sizes for each dimension.  Will attempt to make
        the earliest dimensions in the the dimension_sizes list have the largest chunk
        sizes
        """
        chunk_size_list = list()

        size = 1

        all_remaining_are_ones = False

        for dim_size in dimension_sizes:
            #calculate the chunk size for each dimension if we have not already
            #reached the target chunk size
            if (not all_remaining_are_ones):
                
                #calculate what the current size of the chunk would be if the current
                #dimension were used for the chunk
                size = size * dim_size

                if (size < target_chunk_size):
                    #since the size is below the target size, make the chunk size for
                    #this dimension the same size as the dimension itself
                    #print "chunk size: {}    size: {}".format(dim_size, size)
                    chunk_size_list.append(dim_size)
                else:
                    #since the size using the entirety of the current dimension is bigger
                    #than the target_chunk_size, calculate the chunk size for this dimension
                    #that gets us close to the target_chunk_size (but over)
                     
                    last_chunk_size = int(round(float(target_chunk_size) / float(size/dim_size)))
                    if (0 == last_chunk_size):
                        last_chunk_size = 1

                    #total_size = last_chunk_size*(size/dim_size)
                    #print "chunk size: {}    size: {}".format(last_chunk_size, total_size)
                    
                    chunk_size_list.append(last_chunk_size)
                    
                    #since this dimension created the target_chunk_size, the chunk sizes for the
                    #remaining dimensions must be 1
                    all_remaining_are_ones = True
            else:
                #print "chunk size: 1"
                chunk_size_list.append(1)

        return chunk_size_list
