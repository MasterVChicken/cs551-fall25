"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

from collections import OrderedDict
from lstore.config import Config
import threading

# INDIRECTION_COLUMN = 0
# RID_COLUMN = 1
# TIMESTAMP_COLUMN = 2
# SCHEMA_ENCODING_COLUMN = 3
# BASE_RID_COLUMN = 4
# USER_COLUMN_START = 5

# data structure used to store the index
class OrderedDictList:
    """
        self.data = {
            key1: [[base_rid1, base_rid2, ...], [tail_rid1, tail_rid2, ...]],
            key2: [[base_rid3, base_rid4, ...], [tail_rid3, tail_rid4, ...]],
            ...
        }
    """
    def __init__(self):
         self.data = OrderedDict()
    
    def add(self, key, value, page_type):
        if key not in self.data:
            # self.data[key] = []
            self.data[key] = [[],[]]
        
        if(page_type == 'Base'):
            self.data[key][0].append(value)
        elif(page_type == 'Tail'):
            self.data[key][1].append(value)
        else:
            return False

        # print("After add index value: ", key, self.data[key])

    def value_in_range(self, begin, end):
        res = []
        for key, values in self.data.items():
            if begin <= key <= end:
                res.append(self.data[key])
        return res
    
    # remove rid from key
    def remove_rid(self, key, rid):
        """
        Remove the given rid from the lists associated with the key.
        If both lists become empty after removal, the key is deleted from the data.
        Returns True if the rid was found and removed, False otherwise.
        """
        if key not in self.data:
            return False
        
        removed = False
        
        # Remove from base
        if rid in self.data[key][0]:
            self.data[key][0].remove(rid)
            removed = True
        
        # Remove from tail
        if rid in self.data[key][1]:
            self.data[key][1].remove(rid)
            removed = True
        
        # If both empty, delete the key
        if not self.data[key][0] and not self.data[key][1]:
            del self.data[key]
        
        return removed

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table

        self.create_index(self.table.key, "Base")
        
        self.lock = threading.Lock()
        pass
        
    """
    # returns the location of all records with the given value on column "column"
    """     

    # def locate(self, column, value):
    #     if column > len(self.indices):
    #         ValueError('Invalid column index')
        
    #     if self.indices[column]:
    #         # print(111, self.indices[column].data)
    #         try:
    #             res = self.indices[column].data[value]
    #             # print('locate func: ', value, res)
    #         except KeyError:
    #             res = None
    #         return res
    #     else:
    #         return None

    def locate(self, column, value):
        with self.lock:
            # testM2
            if column >= len(self.indices):
                ValueError('Invalid column index')
            
            if self.indices[column]:
                if value not in self.indices[column].data.keys():
                    return [[], []]
                res = self.indices[column].data[value]
                # print('locate func: ', value, res)
                return res
            else:
                # return None
                # testM2: if column index not exist, return all records with the target value
                res = [[],[]]
                for rid, col_value in self.table.col_iterator(column):
                    if value == col_value:
                        res[0].append(rid)
                return res

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        with self.lock:
            if column > len(self.indices):
                ValueError('Invalid column index')
            
            if self.indices[column]:
                return self.indices[column].value_in_range(begin, end)
            else:
                return None

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number, page_tye="Base"):
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
            self.indices[column_number].add(value, rid, page_tye)

        # insert tail rid and tail value into dict 
        if(self.table.page_directory.num_tail_records != 0):
            res = list(self.table.col_iterator(column_number, "Tail"))
            res.sort(key = lambda res: res[0])

            for rid, value in res:
                self.indices[column_number].add(value, rid, "Tail")
        
        # print("creat index begin end")
        # print(self.indices)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None

    # def delete_value(self, primary_key):
    #     rids = self.locate(self.table.key, primary_key)
        
    #     # primary ket only should have only one rid
    #     if rids == None or len(rids) > 2:
    #         return False
        
    #     self.indices[self.table.key].data[primary_key].pop(rids[0])
    #     return True
    
    # test M2
    def delete_value(self, primary_key):
        rids = self.locate(self.table.key, primary_key)
        # print(rids)
        
        base_rids = rids[0]
        tail_rids = rids[1]

        if len(base_rids) != 0:
            for rid in base_rids:
                self.indices[self.table.key].data[primary_key][0].remove(rid)
        
        # if len(tail_rids) != 0:
        #     for rid in base_rids:
        #         self.indices[self.table.key].data[primary_key][0].remove(rid)
        
        # Yanliang's Modification here: Fix typo of tail_rids here
        if len(tail_rids) != 0:
            for rid in tail_rids:
                self.indices[self.table.key].data[primary_key][1].remove(rid)
        
        return True
    
    def insert_value(self, columns, rid, page_tye):
        with self.lock:
            for col_idx, col_value in enumerate(columns):
                if self.indices[col_idx] != None:
                    # print(col_idx, col_value, rid)
                    self.indices[col_idx].add(col_value, rid, page_tye)
            return True
    
    def update_index(self, column_number, key, value, page_tye):
        with self.lock:
            self.indices[column_number].add(key, value, page_tye)
            
            
    def remove_from_index(self, rid, columns):
        """
        Remove the given rid from the index for the specified columns.
        """
        with self.lock:
            for col_idx, col_value in enumerate(columns):
                if self.indices[col_idx] != None:
                    self.indices[col_idx].remove_rid(col_value, rid)
            return True
