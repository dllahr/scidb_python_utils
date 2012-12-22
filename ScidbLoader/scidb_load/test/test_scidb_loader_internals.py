import scidb_load.scidb_loader as scidb_loader
import unittest
import scidb_load.scidb_load_column as scidb_load_column



class TestScidbLoaderInternals(unittest.TestCase):
    """
    for testing the ScidbLoader class, specifically the calculateChunkSizes and
    calculateDimensions methods
    """

    __dim_size_and_expected_chunk_size = [([232, 87, 19, 10, 13], [232, 87, 19, 3, 1]),
                                          ([2320000, 87, 19, 10, 13], [1000000, 1, 1, 1, 1]),
                                          ([100], [100]),
                                          ([100, 100], [100, 100]),
                                          ([1000000], [1000000]),
                                          ([999999], [999999]),
                                          ([1000001], [1000000]),
                                          ([1000, 1000], [1000, 1000]),
                                          ([333, 3003], [333, 3003]),
                                          ([101, 9901], [101, 9901])]

    class __MockUtilsScidbLoad:
        """
        used to mock the method get_min_and_max within the UtilsScidbLoad class
        """
        __min_max_list = None
        __index = 0

        def __init__(self, minMaxList):
            self.__min_max_list = minMaxList

        def get_min_and_max(self, attribute_dimension_list, array_name):
            result = dict()
                
            for attr in attribute_dimension_list:
                result[attr] = self.__min_max_list[self.__index][0], self.__min_max_list[self.__index][1]
                self.__index += 1

            return result


    def test_calculate_chunk_sizes(self):
        target_chunk_size = 1000000

        for dim_and_expected in self.__dim_size_and_expected_chunk_size:
            dim_size_list = dim_and_expected[0]
            expected = dim_and_expected[1]

            chunk_sizes = scidb_loader.ScidbLoader._calculate_chunk_sizes(dim_size_list, target_chunk_size)

            for i in range(0,len(expected)):
                self.assertEqual(chunk_sizes[i], expected[i])
                


    def test_calculate_dimensions_simple(self):
        """use 0 for min and size to calculated max dimension value"""

        loader = scidb_loader.ScidbLoader(None)

        for dim_and_expected in self.__dim_size_and_expected_chunk_size:
            dim_size_list = dim_and_expected[0]

            #create the list of columns that we need to calculate dimensions for
            col_list = list()
            min_max_list = list()
            for dim_size in dim_size_list:
                min_max_list.append((0,dim_size-1))
                
                col_list.append(scidb_load_column.ScidbLoadColumn("name{}".format(dim_size),
                                                               "dataType{}".format(dim_size),
                                                               False))

            #substitute in the Mock version of UtilsScidbLoad that will provide 
                #dimension values
            loader._utils = self.__MockUtilsScidbLoad(min_max_list)

            #run the calculation to determine the dimension sizes
            loader._calculate_dimensions(col_list)

            #compare the dimensions calculated and assigned to each column to the expected
            expected_chunk_size_list = dim_and_expected[1]
            for i in range(0, len(col_list)):
                col = col_list[i]
                expected_chunk_size = expected_chunk_size_list[i]
                expected_max = dim_size_list[i] - 1

                self.assertEqual(col.dimension_params[0], 0)
                self.assertEqual(col.dimension_params[1], expected_max)
                self.assertEqual(col.dimension_params[2], expected_chunk_size)
                self.assertEqual(col.dimension_params[3], 0)


    def test_calculate_dimensions_ramp_min(self):
        """use ramp for min and size to calculated max dimension value"""
        loader = scidb_loader.ScidbLoader(None)

        for dim_and_expected in self.__dim_size_and_expected_chunk_size:
            dim_size_list = dim_and_expected[0]

            #create the list of columns that we need to calculate dimensions for
            #use an increasing (ramp) minimum value for each dimension
            min_val = 0
            col_list = list()
            min_max_list = list()
            for dim_size in dim_size_list:
                max_val = dim_size + min_val - 1

                min_max_list.append((min_val, max_val))
                
                col_list.append(scidb_load_column.ScidbLoadColumn("name{}".format(max_val),
                                                               "dataType{}".format(max_val),
                                                               False))
                min_val += 1

            #substitute in the Mock version of UtilsScidbLoad that will provide 
            #dimension values
            loader._utils = self.__MockUtilsScidbLoad(min_max_list)

            #run the calculation to determine the dimension sizes
            loader._calculate_dimensions(col_list)

            #compare the dimensions calculated and assigned to each column to the expected
            expected_chunk_size_list = dim_and_expected[1]
            for i in range(0, len(col_list)):
                expected_min_val = i
                col = col_list[i]
                expected_chunk_size = expected_chunk_size_list[i]
                expected_max = dim_size_list[i] - 1 + i

                self.assertEqual(col.dimension_params[0], expected_min_val)
                self.assertEqual(col.dimension_params[1], expected_max)
                self.assertEqual(col.dimension_params[2], expected_chunk_size)
                self.assertEqual(col.dimension_params[3], 0)


    def test_calculate_dimensions_with_attributes(self):
        """use 0 for min and size to calculated max dimension value"""
        loader = scidb_loader.ScidbLoader(None)

        #based on the attribute_pos __index, insert an attribute column into the list
        #of columns that the calculation is run on.  The attribute column should
        #not have any dimension values calculated for it
        for attribute_pos in range(0,3):
            for dim_and_expected in self.__dim_size_and_expected_chunk_size:
                dim_size_list = dim_and_expected[0]

                #create the list of columns that we need to calculate dimensions for
                col_list = list()
                min_max_list = list()
                for dim_size in dim_size_list:
                    min_max_list.append((0,dim_size-1))
                
                    col_list.append(scidb_load_column.ScidbLoadColumn("name{}".format(dim_size),
                                                                   "dataType{}".format(dim_size),
                                                                   False))

                #create the attribute column that will be inserted
                attribute_col = scidb_load_column.ScidbLoadColumn("name_IsAttribute", 
                                                               "dataType_IsAttribute",
                                                               True)
                if 0 == attribute_pos: #insert at beginning of list
                    col_list.insert(0, attribute_col)
                elif 1 == attribute_pos: #insert in middle of list
                    col_list.insert(len(col_list)/2, attribute_col)
                else: #insert at end of list
                    col_list.append(attribute_col)

                #substitute in the Mock version of UtilsScidbLoad that will provide 
                    #dimension values
                loader._utils = self.__MockUtilsScidbLoad(min_max_list)

                #run the calculation to determine the dimension sizes
                loader._calculate_dimensions(col_list)

                #compare the dimensions calculated and assigned to each column to the expected
                expected_chunk_size_list = dim_and_expected[1]
                i = 0
                for col in col_list:
                    if (not col.is_attribute):
                        expected_chunk_size = expected_chunk_size_list[i]
                        expected_max = dim_size_list[i] - 1

                        self.assertEqual(col.dimension_params[0], 0)
                        self.assertEqual(col.dimension_params[1], expected_max)
                        self.assertEqual(col.dimension_params[2], expected_chunk_size)
                        self.assertEqual(col.dimension_params[3], 0)
                    
                        i += 1


if __name__ == '__main__':
    unittest.main()
