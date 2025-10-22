"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from collections import OrderedDict

# data structure used to store the index
class OrderedDictList:
    def __init__(self):
         self.data = OrderedDict()
    
    def add(self, key, value):
        if key not in self.data:
            self.data[key] = []
        self.data[key].append(value)

    def value_in_range(self, begin, end):
        res = []
        for key, values in self.data.items():
            if begin <= key <= end:
                res.append(self.data[key])
        return res

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table
        pass
        
    """
    # returns the location of all records with the given value on column "column"
    """     

    def locate(self, column, value):
        if column > len(self.indices):
            ValueError('Invalid column index')
        
        if self.indices[column]:
            res = self.indices[column][value]
            return res
        else:
            ValueError('No such index existed')

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if column > len(self.indices):
            ValueError('Invalid column index')
        
        if self.indices[column]:
            return self.indices[column].value_in_range(begin, end)
        else:
            ValueError('No such index existed')

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indices[column_number]:
            raise ValueError(f"Key {column_number} already has a index")
        
        # use OrderedDict for index
        self.indices[column_number] = OrderedDictList()

        # get column value and rid from table
        res = list(self.table.col_iterator(column_number))
        res.sort(lambda res: res[0])
        
        # insert rid and value into dict
        for rid, value in res:
            self.indices[column_number].add(value, rid)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
