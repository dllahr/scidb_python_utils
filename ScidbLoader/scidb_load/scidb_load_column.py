
class ScidbLoadColumn:
    """
    contains meta data about columns that are being loaded into SciDB arrays
    name is the name of the column
    data_type is the type of data in the column (int64, double, string, etc.)
    is_attribute is a True/False flag indicating whether it is to be loaded as
            an attribute (True) or as a dimension (False)
    """
    name = None
    data_type = None
    is_attribute = None
    dimension_params = None
    

    def __init__(self, name, data_type, is_attribute):
        self.name = name
        self.data_type = data_type
        self.is_attribute = is_attribute
        self.dimension_params = list()

    def as_attribute(self):
        return "{}:{}".format(self.name, self.data_type)

    def as_dimension(self):
        rangeString = "{}={}:{},".format(self.name, self.dimension_params[0], self.dimension_params[1])
        chunkOverlapString = "{},{}".format(self.dimension_params[2], self.dimension_params[3])
        return rangeString + chunkOverlapString

    def __repr__(self):
        return "{}, {}, {}, {}".format(self.name, self.data_type, self.is_attribute, 
                                       ", ".join([str(x) for x in self.dimension_params]))

