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

