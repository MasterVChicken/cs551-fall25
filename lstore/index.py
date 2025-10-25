"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from collections import OrderedDict

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
USER_COLUMN_START = 4

# data structure used to store the index
class OrderedDictList:
    def __init__(self):
         self.data = OrderedDict()
    
    def add(self, key, value):
        if key not in self.data:
            self.data[key] = []
        self.data[key].append(value)

        # print("After add index value: ", key, self.data[key])

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

        self.create_index(self.table.key)
        pass
        
    """
    # returns the location of all records with the given value on column "column"
    """     

    def locate(self, column, value):
        if column > len(self.indices):
            ValueError('Invalid column index')
        
        if self.indices[column]:
            res = self.indices[column].data[value]
            # print('locate func: ', value, res)
            return res
        else:
            return None

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if column > len(self.indices):
            ValueError('Invalid column index')
        
        if self.indices[column]:
            return self.indices[column].value_in_range(begin, end)
        else:
            return None

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        # print("creat index begin")
        if self.indices[column_number]:
            raise ValueError(f"Key {column_number} already has a index")
        
        # use OrderedDict for index
        self.indices[column_number] = OrderedDictList()

        # get column value and rid from table (return nothing)
        res = list(self.table.col_iterator(column_number))
        # print(res)
        res.sort(key = lambda res: res[0])
        
        # insert rid and value into dict
        for rid, value in res:
            self.indices[column_number].add(value, rid)
        
        # print("creat index begin end")
        # print(self.indices)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None

    def delete_value(self, primary_key):
        rids = self.locate(self.table.key, primary_key)
        
        # primary ket only should have only one rid
        if rids == None or len(rids) > 2:
            return False
        
        self.indices[self.table.key].data[primary_key].pop(rids[0])
        return True
    
    def insert_value(self, columns, rid):
        for col_idx, col_value in enumerate(columns):
            if self.indices[col_idx] != None:
                # print(col_idx, col_value, rid)
                self.indices[col_idx].add(col_value, rid)
        return True
    
    def update_index(self, column_number, key, value):
        self.indices[column_number].add(key, value)
