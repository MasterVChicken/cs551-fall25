from lstore.table import Table, Record
from lstore.index import Index
from datetime import datetime

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
USER_COLUMN_START = 4

COL_OFFSET = 5

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # get rid first
        rid = self.table.key_to_rid.get(primary_key, None)
        if rid is None:
            return False
        # delete the record from table
        self.table.delete_record_by_rid(rid)
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # # first 5 for rid, indirection, timestamp, schema, col start
        # total_num_col = len(columns) + COL_OFFSET
        # new_columns = [None for _ in range(total_num_col)]

        # # schema_encoding = '0' * self.table.num_columns
        # new_columns[INDIRECTION_COLUMN] = -1
        # new_columns[RID_COLUMN] = self.table.page_directory.num_base_records
        # new_columns[TIMESTAMP_COLUMN] = int(datetime.now().current_datetime.timestamp())
        # new_columns[COL_OFFSET:] = columns[:]

        # add the record to table
        self.table.add_record(columns, "Base")

        # create a index for the record

        pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        self.select_version(search_key, search_key_index, projected_columns_index, 0)
        

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        selected_rids = self.table.index.locate(search_key_index, search_key)
        records_list = []
        for rid in selected_rids:
            res_col = []
            for col_idx, projected_value in enumerate(projected_columns_index):
                # if projected_value is 1, get data of this column from base page
                if projected_value:
                    col_value = self.table.page_directory.get_col_value(rid, col_idx, 'Base')
                    res_col.append(col_value)
                # for 0 value, should we append None?
                # else:
                #     res_col.appned()

            # get data matched the relative version from base value
            next_rid, page_type = self.table.get_version_rid(rid, relative_version)

            if page_type == 'Tail':
                # need to edit schema?
                # schema = self.tabel.get_col_value(next_rid, SCHEMA_ENCODING_COLUMN, page_type)
                for col_idx in range(len(projected_columns_index)):
                    col_value = self.table.page_directory.get_col_value(next_rid, col_idx, page_type)
                    res_col.append(col_value)

            record = Record(next_rid, self.table.key, res_col)
            records_list.append(record)
        
        return records_list

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
