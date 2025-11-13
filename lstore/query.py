from lstore.table import Table, Record
from lstore.index import Index
from datetime import datetime
from lstore.config import Config

# INDIRECTION_COLUMN = 0
# RID_COLUMN = 1
# TIMESTAMP_COLUMN = 2
# SCHEMA_ENCODING_COLUMN = 3
# BASE_RID_COLUMN = 4
# USER_COLUMN_START = 5

# # COL_OFFSET = 5

# PAGE_CAPACITY = 4096 // 8

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
        new_timestamp = int(datetime.now().timestamp())

        # add the record to table
        # res1 = self.table.add_record(columns, "Base")
        res1 = self.table.page_directory.insert_base_record(new_rid, new_timestamp, columns)
        if res1 == None:
            return False

        # print(new_rid, columns)
        
        res2 = self.table.index.insert_value(columns, new_rid, "Base")

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
        # selected_rids = self.table.index.locate(search_key_index, search_key)
        rids_list = self.table.index.locate(search_key_index, search_key)
        # tmp_rid = [e[0] for e in rids_list]
        # selected_rids = [x for row in tmp_rid for x in row]
        selected_rids = rids_list[0]
        
        # selected_rids = [selected_rids[0]]
        # print(f"Select version: search_key_index: {search_key_index}, search_key: {search_key}, selected_rids: {selected_rids}")

        records_list = []
        for rid in selected_rids:
            res_col = []
            for col_idx, projected_value in enumerate(projected_columns_index):
                # if projected_value is 1, get data of this column from base page
                if projected_value:
                    col_value = self.table.page_directory.read_base_record(rid//Config.PAGE_CAPACITY, rid%Config.PAGE_CAPACITY)['columns'][col_idx]
                    res_col.append(col_value)
                # for 0 value, should we append None?
                # else:
                #     res_col.appned()

            # get data matched the relative version from base value
            # print("get_version: ", rid)
            next_rid, page_type = self.table.get_version_rid(rid, relative_version)

            if page_type == 'Tail':
                # need to edit schema?
                for col_idx in range(len(projected_columns_index)):
                    col_value = self.table.page_directory.read_tail_record(next_rid//Config.PAGE_CAPACITY, next_rid%Config.PAGE_CAPACITY)['columns'][col_idx]
                    res_col[col_idx] = col_value

            record = Record(next_rid, self.table.key, res_col)
            records_list.append(record)
        
        return records_list

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    # issue exist
    def update(self, primary_key, *columns):

        # selected_rids = self.table.index.locate(self.table.key, primary_key)
       
        rids_list = self.table.index.locate(self.table.key, primary_key)
        # tmp_rid = [e[0] for e in rids_list]
        # selected_rids = [x for row in tmp_rid for x in row]
        selected_rids = rids_list[0]

        if selected_rids == None:
            return False
        
        # print(f"\nupdate func, primary_key_value: {primary_key}, rids: {selected_rids}, input column: {columns}")
        rid = selected_rids[0]

        base_page_idx = rid // Config.PAGE_CAPACITY
        base_record_idx = rid % Config.PAGE_CAPACITY

        base_indirection = self.table.page_directory.read_base_record(base_page_idx, base_record_idx)['indirection']
        base_schema = self.table.page_directory.read_base_record(base_page_idx, base_record_idx)['schema_encoding']
        base_rid = self.table.page_directory.read_base_record(base_page_idx, base_record_idx)['base_rid']

        # updated record columns
        updated_columns = self.table.page_directory.read_base_record(base_page_idx, base_record_idx)['columns']
        # print(f"base_ind: {base_indirection}, base_schema: {base_schema}, init columns: {updated_columns}")

        # indirection for the updated record
        updated_indirection = base_indirection
        # rid for the updated record
        updated_rid = self.table.page_directory.num_tail_records
        # timestamp for the updated record
        updated_timestamp = int(datetime.now().timestamp())
        # updated base rid
        updated_base_rid = base_rid
        # # schema for the updated record
        # updated_schema = base_schema
        # for i in range(len(columns)):
        #     if columns[i] != None:
        #         updated_columns[i] = columns[i]
        #         updated_schema = (updated_schema | (1 << i))

        # # print("from input columns: ", updated_columns)

        # if has another update record, head insert, replace the current record 
        if base_indirection != -1:
        # if base_indirection != 0:
            cur_version_record = self.select(primary_key, self.table.key, [1 for _ in range(len(columns))])[0]
            
            # print("old record: ", cur_version_record.columns)
            
            for i in range(len(columns)):
                # find the updated columns
                if ((base_schema >> i) & 1):
                    # print("   updated col idx: ", i)
                    updated_columns[i] = cur_version_record.columns[i]
        
        # print("from existed tailed columns: ", updated_columns)

        # schema for the updated record
        updated_schema = base_schema
        for i in range(len(columns)):
            if columns[i] != None:
                updated_columns[i] = columns[i]
                updated_schema = (updated_schema | (1 << i))

        # print("from input columns: ", updated_columns)

        # add the updated record to tail page
        self.table.page_directory.append_tail_record(updated_rid, updated_indirection, updated_timestamp, updated_schema, updated_base_rid, updated_columns)

        # update the base page
        self.table.page_directory.update_base_indirection(base_page_idx, base_record_idx, updated_rid)
        self.table.page_directory.update_base_schema_encoding(base_page_idx, base_record_idx, updated_schema)
        # do we need to update the timestamp for base page?

        # update the index for the updated record
        # print(f'Before update index: primary_key_idx: {self.table.key}, primary_key_value: {primary_key}, updated_record_rid: {updated_rid}')
        self.table.index.update_index(self.table.key, primary_key, updated_rid, "Tail")

        # merge add here
        merge_threshold = 15
        if (self.table.page_directory.num_tail_records // Config.PAGE_CAPACITY) % merge_threshold == 0:
            # print("Call merge")
            self.table.merge()

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
        
        rids_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        tmp_rid = [e[0] for e in rids_list]
        # selected_rids = [e[0] for e in rids_list]
        selected_rids = [x for row in tmp_rid for x in row]

        # print(f"\n range: ({start_range}, {end_range}), sum func. selected_rids: {selected_rids}")
        res = 0
        for rid in selected_rids:
            # page and record idx
            page_idx = rid // Config.PAGE_CAPACITY
            record_idx = rid % Config.PAGE_CAPACITY

            # get value from base page
            value = self.table.page_directory.read_base_record(page_idx, record_idx)['columns'][aggregate_column_index]

            # select version
            updated_rid, updated_page_type = self.table.get_version_rid(rid, relative_version)

            # page and record idx for relative version
            updated_page_idx = updated_rid // Config.PAGE_CAPACITY
            updated_record_idx = updated_rid % Config.PAGE_CAPACITY

            # has the updated record
            if updated_page_type == "Tail":
                updated_schema = self.table.page_directory.read_tail_record(updated_page_idx, updated_record_idx)['schema_encoding']
                
                if ((updated_schema >> aggregate_column_index) & 1):
                    value = self.table.page_directory.read_tail_record(updated_page_idx, updated_record_idx)['columns'][aggregate_column_index]

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
