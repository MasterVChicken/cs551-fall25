from lstore.table import Table, Record
from lstore.index import Index
from datetime import datetime

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
USER_COLUMN_START = 4

COL_OFFSET = 5

PAGE_CAPACITY = 4096 // 8

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

        new_rid = self.table.page_directory.num_base_records
        new_timestamp = int(datetime.now().current_datetime.timestamp())

        # add the record to table
        # res1 = self.table.add_record(columns, "Base")
        res1 = self.page_directory.insert_base_record(new_rid, new_timestamp, columns)
        if res1 == None:
            return False

        # create a index for the record
        res2 = self.table.index.insert_value(columns, new_rid)

        return res2

    
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
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)
        

    
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

        rid = self.table.index.locate(self.table.key, primary_key)

        if rid == None:
            return False
        
        base_indirection = self.table.page_directory.get_col_value(rid, INDIRECTION_COLUMN, 'Base')
        base_schema = self.table.page_directory.get_col_value(rid, SCHEMA_ENCODING_COLUMN, 'Base')

        # updated record columns
        updated_columns = [0 for _ in range(columns)]
        # indirection for the updated record
        updated_indirection = base_indirection
        # rid for the updated record
        updated_rid = self.table.page_directory.num_tail_records
        # timestamp for the updated record
        updated_timestamp = int(datetime.now().current_datetime.timestamp())
        # schema for the updated record
        updated_schema = base_schema
        for i in range(len(columns)):
            if columns[i] != None:
                updated_columns[i] = columns[i]
                updated_schema = (updated_schema | (1 << i))

        # if has another update record, head insert, replace the current record 
        # if base_indirection != -1:
        if base_indirection != 0:
            cur_version_record = self.select(primary_key, self.table.key, [1 for _ in range(len(columns))])[0]
            for i in range(len(columns)):
                # find the updated columns
                if ((base_schema >> i) & 1):
                    updated_columns[i] = cur_version_record.columns[i]
        
        # add the updated record to tail page
        self.table.page_directory.append_tail_record(updated_rid, updated_indirection, updated_timestamp, updated_schema, updated_columns)

        # update the base page
        base_page_idx = rid // PAGE_CAPACITY
        base_record_idx = rid % PAGE_CAPACITY
        self.table.page_directory.update_base_indirection(base_page_idx, base_record_idx, updated_rid)
        self.table.page_directory.update_base_schema_encoding(base_page_idx, base_record_idx, updated_schema)
        # do we need to update the timestamp for base page?

        # update the index for the updated record
        self.table.index.update_index(primary_key, columns[primary_key], updated_rid)

        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)

    
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
        selected_rids = self.table.index.locate_range(start_range, end_range, self.table.key)

        res = 0
        for rid in selected_rids:
            # get value from base page
            value = self.table.get_col_value(rid, aggregate_column_index, 'Base')

            # select version
            updated_rid, updated_page_type = self.table.get_version_rid(rid, relative_version)

            # has the updated record
            if updated_page_type == "Tail":
                updated_schema = self.table.get_col_value(updated_rid, SCHEMA_ENCODING_COLUMN, updated_page_type)
                if ((updated_schema >> aggregate_column_index) & 1):
                    value =  self.table.get_col_value(updated_rid, aggregate_column_index, updated_page_type)
            
            res += value
        
        return res

    
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
