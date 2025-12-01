from lstore.table import Table, Record
from lstore.index import Index
from datetime import datetime
from lstore.config import Config
from lstore.lock_manager import LockType

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
    # def delete(self, primary_key, transaction=None):
    #     # get rid first
    #     rid = self.table.key_to_rid.get(primary_key, None)
    #     if rid is None:
    #         return False
        
    #     # Acquire locks and record logs
    #     if transaction is not None:
    #         lock_acquired = self.table.lock_manager.acquire_lock(
    #             lock_id=rid,
    #             lock_type=LockType.EXCLUSIVE,
    #             transaction_id=transaction.transaction_id
    #         )
    #         if not lock_acquired:
    #             return False
            
    #         transaction.log_operation(
    #             table=self.table,
    #             op_type='delete',
    #             rollback_data={'rid': rid}
    #         )
            
    #     # delete the record from table
    #     self.table.delete_record_by_rid(rid)
    #     pass

    # testM2
    def delete(self, primary_key, transaction=None):
        # self.table.delete(primary_key)
        
        # Yanliang's Modification here: acquire lock, log, transaction
        rids_list = self.table.index.locate(self.table.key, primary_key)
        selected_rids = rids_list[0]
        
        if selected_rids is None or len(selected_rids) == 0:
            return False
        
        rid = selected_rids[0]
        
        # Save info before deletion
        page_idx = rid // Config.PAGE_CAPACITY
        record_idx = rid % Config.PAGE_CAPACITY
        record = self.table.page_directory.read_base_record(page_idx, record_idx)
    
        if record is None:
            return False
        
        old_indirection = record['indirection']
        old_columns = record['columns']
        
        if transaction is not None:
            lock_acquired = self.table.lock_manager.acquire_lock(
                lock_id=rid,
                lock_type=LockType.EXCLUSIVE,
                transaction_id=transaction.transaction_id
            )
            if not lock_acquired:
                return False
            
            transaction.log_operation(
                table=self.table,
                op_type='delete',
                rollback_data={
                'rid': rid,
                'old_indirection': old_indirection, 
                'old_columns': old_columns
            }
            )
        
        self.table.delete(primary_key)
        return True
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns, transaction=None):
        # # first 5 for rid, indirection, timestamp, schema, col start
        
        # Check if the primary key already exists
        primary_key_value = columns[self.table.key]
        rids_list = self.table.index.locate(self.table.key, primary_key_value)
        if rids_list is not None and len(rids_list[0]) > 0:
            # Primary key already exists
            # Refuse to insert
            return False

        # new_rid = self.table.page_directory.num_base_records
        new_timestamp = int(datetime.now().timestamp())
        
        # Allocate rid atomically and insert under lock protection
        result = self.table.page_directory.insert_base_record_with_rid_alloc(new_timestamp, columns)
        if result is None:
            return False
        
        new_rid, page_index, record_index = result
        
        # Accquire locks
        if transaction is not None:
            lock_acquired = self.table.lock_manager.acquire_lock(
                lock_id=new_rid,
                lock_type=LockType.EXCLUSIVE,
                transaction_id=transaction.transaction_id
            )
            if not lock_acquired:
                return False
            transaction.log_operation(
                table=self.table,
                op_type='insert',
                rollback_data={'rid': new_rid}
            )

        # # add the record to table
        # # res1 = self.table.add_record(columns, "Base")
        # res1 = self.table.page_directory.insert_base_record(new_rid, new_timestamp, columns)
        # if res1 == None:
        #     return False

        # # print(new_rid, columns)
        
        res2 = self.table.index.insert_value(columns, new_rid, "Base")
        
        # # Record logs
        # if transaction is not None:
        #     transaction.log_operation(
        #         table=self.table,
        #         op_type='insert',
        #         rollback_data={'rid': new_rid}
        #     )

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
    def select(self, search_key, search_key_index, projected_columns_index, transaction=None):
        return self.select_version(search_key, search_key_index, projected_columns_index, 0, transaction)
        

    
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
    # def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction=None):
    #     # selected_rids = self.table.index.locate(search_key_index, search_key)
    #     rids_list = self.table.index.locate(search_key_index, search_key)
        
    #     # return empty if no record found
    #     if rids_list is None:
    #         return []
        
    #     # tmp_rid = [e[0] for e in rids_list]
    #     # selected_rids = [x for row in tmp_rid for x in row]
    #     selected_rids = rids_list[0]
        
    #     # selected_rids = [selected_rids[0]]
    #     # print(f"Select version: search_key_index: {search_key_index}, search_key: {search_key}, selected_rids: {selected_rids}")
        
    #     # Acquire locks
    #     if transaction is not None:
    #         for rid in selected_rids:
    #             lock_acquired = self.table.lock_manager.acquire_lock(
    #                 lock_id=rid,
    #                 lock_type=LockType.SHARED,
    #                 transaction_id=transaction.transaction_id
    #             )
    #             if not lock_acquired:
    #                 return False

    #     records_list = []
    #     for rid in selected_rids:
    #         version_rid, page_type = self.table.get_version_rid(rid, relative_version)
    #         res_col = []
    #         for col_idx, projected_value in enumerate(projected_columns_index):
    #             # if projected_value is 1, get data of this column from base page
    #             if projected_value:
    #                 col_value = self.table.page_directory.read_base_record(rid//Config.PAGE_CAPACITY, rid%Config.PAGE_CAPACITY)['columns'][col_idx]
    #                 res_col.append(col_value)
    #             # for 0 value, should we append None?
    #             # else:
    #             #     res_col.appned()

    #         # get data matched the relative version from base value
    #         # print("get_version: ", rid)
    #         next_rid, page_type = self.table.get_version_rid(rid, relative_version)

    #         if page_type == 'Tail':
    #             # need to edit schema?
    #             for col_idx in range(len(projected_columns_index)):
    #                 col_value = self.table.page_directory.read_tail_record(next_rid//Config.PAGE_CAPACITY, next_rid%Config.PAGE_CAPACITY)['columns'][col_idx]
    #                 res_col[col_idx] = col_value

    #         # record = Record(next_rid, self.table.key, res_col)
    #         # records_list.append(record)
    #         # testM2
    #         if res_col[search_key_index] == search_key:
    #             record = Record(next_rid, self.table.key, res_col) 
    #             records_list.append(record)
        
    #     return records_list
    
    
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction=None):
        rids_list = self.table.index.locate(search_key_index, search_key)
        
        if rids_list is None:
            return []
        
        selected_rids = rids_list[0]
        
        # 获取锁
        if transaction is not None:
            for rid in selected_rids:
                lock_acquired = self.table.lock_manager.acquire_lock(
                    lock_id=rid,
                    lock_type=LockType.SHARED,
                    transaction_id=transaction.transaction_id
                )
                if not lock_acquired:
                    return False

        records_list = []
        for rid in selected_rids:
            page_idx = rid // Config.PAGE_CAPACITY
            record_idx = rid % Config.PAGE_CAPACITY

            # Step 1: Read base record to get initial values for all columns
            base_record = self.table.page_directory.read_base_record(page_idx, record_idx)
            if base_record is None:
                continue

            # Start with base record columns
            result_columns = base_record['columns'].copy()
            indirection = base_record['indirection']

            # Step 2: Apply updates based on relative_version
            if indirection != -1:
                if relative_version == 0:
                    # Version 0: Apply all updates from tail chain (latest version)
                    # Collect all tail records from newest to oldest
                    tail_chain = []
                    current_tail_rid = indirection

                    while current_tail_rid != -1:
                        tail_page_idx = current_tail_rid // Config.PAGE_CAPACITY
                        tail_record_idx = current_tail_rid % Config.PAGE_CAPACITY
                        tail_record = self.table.page_directory.read_tail_record(tail_page_idx, tail_record_idx)

                        if tail_record is None:
                            break

                        tail_chain.append(tail_record)
                        current_tail_rid = tail_record['indirection']

                    # Apply updates from oldest to newest (reverse order)
                    for tail_record in reversed(tail_chain):
                        schema = tail_record['schema_encoding']
                        for col_idx in range(len(result_columns)):
                            # Check if this column was updated in this tail record
                            if (schema >> col_idx) & 1:
                                result_columns[col_idx] = tail_record['columns'][col_idx]

                elif relative_version < 0:
                    # Negative version: Apply updates up to a certain depth
                    # relative_version = -1 means 1 version back, -2 means 2 versions back, etc.
                    # Version -1 means skip the latest 1 update
                    # Version -2 means skip the latest 2 updates
                    depth = abs(relative_version)

                    # Collect tail chain
                    tail_chain = []
                    current_tail_rid = indirection

                    while current_tail_rid != -1:
                        tail_page_idx = current_tail_rid // Config.PAGE_CAPACITY
                        tail_record_idx = current_tail_rid % Config.PAGE_CAPACITY
                        tail_record = self.table.page_directory.read_tail_record(tail_page_idx, tail_record_idx)

                        if tail_record is None:
                            break

                        tail_chain.append(tail_record)
                        current_tail_rid = tail_record['indirection']

                    # tail_chain is ordered from newest to oldest
                    # If depth >= len(tail_chain), we want the base version (no updates)
                    # Otherwise, skip the latest 'depth' updates and apply the rest
                    if depth < len(tail_chain):
                        # Skip the latest 'depth' updates
                        # Apply updates from oldest up to (len - depth)
                        updates_to_apply = tail_chain[depth:]  # Skip first 'depth' elements (newest ones)

                        # Apply from oldest to newest
                        for tail_record in reversed(updates_to_apply):
                            schema = tail_record['schema_encoding']
                            for col_idx in range(len(result_columns)):
                                if (schema >> col_idx) & 1:
                                    result_columns[col_idx] = tail_record['columns'][col_idx]
                    # else: depth >= len(tail_chain), keep base version (no updates applied)

            # Step 3: Project columns based on projected_columns_index
            res_col = []
            for col_idx, projected in enumerate(projected_columns_index):
                if projected:
                    res_col.append(result_columns[col_idx])

            # Step 4: Filter by search key
            if res_col[search_key_index] == search_key:
                record_obj = Record(rid, self.table.key, res_col)
                records_list.append(record_obj)

        return records_list

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns, transaction=None):
        rids_list = self.table.index.locate(self.table.key, primary_key)
        selected_rids = rids_list[0]
        if selected_rids == None or len(selected_rids) > 1 or len(selected_rids) == 0:
            return False
        rid = selected_rids[0]

        # Check uniqueness
        update_primary_key = columns[self.table.key]
        if update_primary_key is not None and update_primary_key != primary_key:
            rids_list = self.table.index.locate(self.table.key, update_primary_key)
            selected_rids = rids_list[0]
            if selected_rids is not None and len(selected_rids) > 0:
                return False
        
        # acquire locks
        if transaction is not None:
            lock_acquired = self.table.lock_manager.acquire_lock(
                lock_id=rid,
                lock_type=LockType.EXCLUSIVE,
                transaction_id=transaction.transaction_id
            )
            if not lock_acquired:
                return False

        base_page_idx = rid // Config.PAGE_CAPACITY
        base_record_idx = rid % Config.PAGE_CAPACITY

        base_record = self.table.page_directory.read_base_record(base_page_idx, base_record_idx)
        base_indirection = base_record['indirection']
        base_schema = base_record['schema_encoding']
        base_rid = base_record['base_rid']
        
        # record logs
        if transaction is not None:
            transaction.log_operation(
                table=self.table,
                op_type='update',
                rollback_data={
                    'rid': rid,
                    'old_indirection': base_indirection,
                    'old_primary_key': primary_key if update_primary_key != primary_key else None  # 新增
                }
            )

        updated_columns = base_record['columns'].copy()

        if base_indirection != -1:            
            # get the latest rid (version 0)
            latest_rid, page_type = self.table.get_version_rid(rid, 0)
            
            # get record from latest rid
            if page_type == 'Base':
                latest_record = self.table.page_directory.read_base_record(
                    latest_rid // Config.PAGE_CAPACITY, 
                    latest_rid % Config.PAGE_CAPACITY
                )
            else:
                latest_record = self.table.page_directory.read_tail_record(
                    latest_rid // Config.PAGE_CAPACITY, 
                    latest_rid % Config.PAGE_CAPACITY
                )
            
            if latest_record:
                latest_columns = latest_record['columns']
                for i in range(len(columns)):
                    # if the column is not updated in the latest record, use the latest value
                    if ((base_schema >> i) & 1):
                        updated_columns[i] = latest_columns[i]
        
        updated_schema = base_schema
        for i in range(len(columns)):
            if columns[i] is not None:
                updated_columns[i] = columns[i]
                updated_schema = (updated_schema | (1 << i))

        updated_indirection = base_indirection
        updated_timestamp = int(datetime.now().timestamp())
        updated_base_rid = base_rid
        
        # create tail record
        result = self.table.page_directory.append_tail_record_with_rid_alloc(
            updated_indirection, 
            updated_timestamp, 
            updated_schema, 
            updated_base_rid, 
            updated_columns
        )
        
        if result is None:
            return False
        
        updated_rid, tail_page_index, tail_record_index = result

        # update base record
        self.table.page_directory.update_base_indirection(base_page_idx, base_record_idx, updated_rid)
        self.table.page_directory.update_base_schema_encoding(base_page_idx, base_record_idx, updated_schema)

        # Update index only when the primary key changes
        if update_primary_key is not None and update_primary_key != primary_key:
            # Remove old primary key
            if self.table.index.indices[self.table.key]:
                self.table.index.indices[self.table.key].remove_rid(primary_key, rid)
            # Add new primary key
            if self.table.index.indices[self.table.key]:
                self.table.index.indices[self.table.key].add(update_primary_key, rid, "Base")

        return True

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index, transaction=None):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0, transaction)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, transaction=None):
        
        rids_list = self.table.index.locate_range(start_range, end_range, self.table.key)
        tmp_rid = [e[0] for e in rids_list]
        # selected_rids = [e[0] for e in rids_list]
        selected_rids = [x for row in tmp_rid for x in row]
        
        # Acquire locks
        if transaction is not None:
            for rid in selected_rids:
                lock_acquired = self.table.lock_manager.acquire_lock(
                    lock_id=rid,
                    lock_type=LockType.SHARED,
                    transaction_id=transaction.transaction_id
                )
                if not lock_acquired:
                    return False

        # print(f"\n range: ({start_range}, {end_range}), sum func. selected_rids: {selected_rids}")
        res = 0
        for rid in selected_rids:
            # page and record idx
            page_idx = rid // Config.PAGE_CAPACITY
            record_idx = rid % Config.PAGE_CAPACITY

            # Read base record
            base_record = self.table.page_directory.read_base_record(page_idx, record_idx)
            if base_record is None:
                continue

            # Start with base value for the aggregate column
            value = base_record['columns'][aggregate_column_index]
            indirection = base_record['indirection']

            # Apply updates based on relative_version
            if indirection != -1:
                if relative_version == 0:
                    # Version 0: Apply all updates from tail chain (latest version)
                    tail_chain = []
                    current_tail_rid = indirection

                    while current_tail_rid != -1:
                        tail_page_idx = current_tail_rid // Config.PAGE_CAPACITY
                        tail_record_idx = current_tail_rid % Config.PAGE_CAPACITY
                        tail_record = self.table.page_directory.read_tail_record(tail_page_idx, tail_record_idx)

                        if tail_record is None:
                            break

                        tail_chain.append(tail_record)
                        current_tail_rid = tail_record['indirection']

                    # Apply updates from oldest to newest (reverse order)
                    for tail_record in reversed(tail_chain):
                        schema = tail_record['schema_encoding']
                        # Check if the aggregate column was updated
                        if (schema >> aggregate_column_index) & 1:
                            value = tail_record['columns'][aggregate_column_index]

                elif relative_version < 0:
                    # Negative version: Apply updates up to a certain depth
                    # Version -1 means skip the latest 1 update
                    depth = abs(relative_version)

                    # Collect tail chain
                    tail_chain = []
                    current_tail_rid = indirection

                    while current_tail_rid != -1:
                        tail_page_idx = current_tail_rid // Config.PAGE_CAPACITY
                        tail_record_idx = current_tail_rid % Config.PAGE_CAPACITY
                        tail_record = self.table.page_directory.read_tail_record(tail_page_idx, tail_record_idx)

                        if tail_record is None:
                            break

                        tail_chain.append(tail_record)
                        current_tail_rid = tail_record['indirection']

                    # Apply updates if within the tail chain
                    # Skip the latest 'depth' updates
                    if depth < len(tail_chain):
                        updates_to_apply = tail_chain[depth:]  # Skip first 'depth' elements

                        for tail_record in reversed(updates_to_apply):
                            schema = tail_record['schema_encoding']
                            if (schema >> aggregate_column_index) & 1:
                                value = tail_record['columns'][aggregate_column_index]

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
    def increment(self, key, column, transaction=None):
        r = self.select(key, self.table.key, [1] * self.table.num_columns, transaction)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns, transaction)
            return u
        return False
