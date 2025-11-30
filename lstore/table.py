from lstore.index import Index
from time import sleep, time
from lstore.page import *
from lstore.cache_policy import LRUCache
from lstore.config import Config
from lstore.lock_manager import LockManager
from datetime import datetime
import threading

import math
import copy
import os
import struct

# INDIRECTION_COLUMN = 0
# RID_COLUMN = 1
# TIMESTAMP_COLUMN = 2
# SCHEMA_ENCODING_COLUMN = 3
# BASE_RID_COLUMN = 4
# USER_COLUMN_START = 5

# PAGE_CAPACITY = 4096 // 8

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

# According to assignment 1's description:
# "These invalidated records will be removed during the next merge cycle for the corresponding page range."
class PageRange:
    # Manage a set of base pages and tail pages
    def __init__(self, table_path, range_id, num_columns, num_base_records = 0, num_tail_records = 0):
        self.table_path = table_path

        self.range_id = range_id
        self.num_columns = num_columns
                
        # LRU cache
        self.cache_capacity = 1000 # enlarged the cache capacity
        self.Buffer = LRUCache(self.cache_capacity)
        
        self.current_base_page = None
        self.current_tail_page = None
        
        # self.num_base_records = 0
        # self.num_tail_records = 0
        self.num_base_records = num_base_records
        self.num_tail_records = num_tail_records
        
        self.lock = threading.Lock()
        
    def _allocate_base_page(self):
        new_page = BasePage(self.num_columns)
        # self.base_pages.append(new_page)
        
        # LRU cache
        idx = self.num_base_records // Config.PAGE_CAPACITY
        # (page_idx, page)
        # evict = self.base_pages.put(idx, new_page)
        evict = self.Buffer.put(idx, new_page, "Base")
        if evict != None: 
            self.save_one_page_to_disk(evict[0], evict[1], "Base")
        self.current_base_page = new_page
        return new_page
    
    def _allocate_tail_page(self):
        new_page = TailPage(self.num_columns)
        # self.tail_pages.append(new_page)

        # LRU cache
        idx = self.num_tail_records // Config.PAGE_CAPACITY
        # (page_idx, page)
        # evict = self.tail_pages.put(idx, new_page)
        evict = self.Buffer.put(idx, new_page, "Tail")
        if evict != None: 
            self.save_one_page_to_disk(evict[0], evict[1], "Tail")

        self.current_tail_page = new_page
        return new_page
    
    def has_base_capacity(self):
        # return self.current_base_page.has_capacity()
        return False if self.current_base_page == None else self.current_base_page.has_capacity()
    
    def has_tail_capacity(self):
        # return self.current_tail_page.has_capacity()
        return False if self.current_tail_page == None else self.current_tail_page.has_capacity()
    
    # def insert_base_record(self, rid, timestamp, columns):
    #     with self.lock:
    #         if not self.has_base_capacity():
    #             self._allocate_base_page()
        
    #         success = self.current_base_page.insert_record(rid, timestamp, columns)
    #         if success:
    #             # We need to re-write the index part
    #             # page_index = len(self.base_pages) - 1
    #             page_index = self.num_base_records // Config.PAGE_CAPACITY
    #             record_index = self.current_base_page.num_records - 1
    #             self.num_base_records += 1
    #             return (page_index, record_index)
    #         return None
        
    
    # # Some specical prpcess here I think?
    # def append_tail_record(self, rid, indirection, timestamp, schema_encoding, base_rid, columns):
    #     with self.lock:
    #         if not self.has_tail_capacity():
    #             self._allocate_tail_page()
        
    #         success = self.current_tail_page.append_update(
    #             rid, indirection, timestamp, schema_encoding, base_rid, columns
    #         )
    #         if success:
    #             # page_index = len(self.tail_pages) - 1
    #             page_index = self.num_tail_records // Config.PAGE_CAPACITY
    #             record_index = self.current_tail_page.num_records - 1
    #             self.num_tail_records += 1
    #             return (page_index, record_index)
    #         return None
    
    def insert_base_record_with_rid_alloc(self, timestamp, columns):
        # Atomically allocate rid and insert base record
        # return (rid, page_index, record_index) or None
        with self.lock:
            rid = self.num_base_records
            
            if not self.has_base_capacity():
                self._allocate_base_page()
        
            success = self.current_base_page.insert_record(rid, timestamp, columns)
            if success:
                page_index = self.num_base_records // Config.PAGE_CAPACITY
                record_index = self.current_base_page.num_records - 1
                self.num_base_records += 1
                return (rid, page_index, record_index)
            return None
    
    def append_tail_record_with_rid_alloc(self, indirection, timestamp, schema_encoding, base_rid, columns):
        # Atomically allocate rid and insert base record
        # return (rid, page_index, record_index) or None
        with self.lock:
            rid = self.num_tail_records
            
            if not self.has_tail_capacity():
                self._allocate_tail_page()
        
            success = self.current_tail_page.append_update(
                rid, indirection, timestamp, schema_encoding, base_rid, columns
            )
            if success:
                page_index = self.num_tail_records // Config.PAGE_CAPACITY
                record_index = self.current_tail_page.num_records - 1
                self.num_tail_records += 1
                return (rid, page_index, record_index)
            return None
    
    def read_base_record(self, page_index, record_index):
        # if page_index >= len(self.base_pages):
        #     return None

        # if page_index not in self.base_pages.cache:
        if page_index not in self.Buffer.base_cache:
            base_page = self.load_one_base_page_from_disk(page_index)
            if base_page == None:
                return None
            evict = self.Buffer.put(page_index, base_page, "Base")
            if evict != None: 
                self.save_one_page_to_disk(evict[0], evict[1], "Base")
        else:
            # base_page = self.base_pages[page_index]
            # base_page = self.base_pages.get(page_index)
            base_page = self.Buffer.get(page_index, "Base")
            if record_index >= base_page.num_records:
                # print("read_base_record: ", page_index, record_index, base_page.num_records)
                return None
        
        # read every column independently
        return {
            'indirection': base_page.physical_pages[Config.INDIRECTION_COLUMN].read(record_index),
            'rid': base_page.physical_pages[Config.RID_COLUMN].read(record_index),
            'timestamp': base_page.physical_pages[Config.TIMESTAMP_COLUMN].read(record_index),
            'schema_encoding': base_page.physical_pages[Config.SCHEMA_ENCODING_COLUMN].read(record_index),
            'base_rid': base_page.physical_pages[Config.BASE_RID_COLUMN].read(record_index),
            'columns': [
                base_page.physical_pages[Config.USER_COLUMN_START + i].read(record_index)
                for i in range(self.num_columns)
            ]
        }
    
    def read_tail_record(self, page_index, record_index):
        # if page_index >= len(self.tail_pages):
        #     return None
        # if page_index not in self.tail_pages.cache:
        if page_index not in self.Buffer.tail_cache:
            tail_page = self.load_one_tail_page_from_disk(page_index)
            if tail_page == None:
                return None
            
            # evict = self.tail_pages.put(page_index, tail_page)
            evict = self.Buffer.put(page_index, tail_page, "Tail")
            if evict != None: 
                self.save_one_page_to_disk(evict[0], evict[1], "Tail")
        else:
            # tail_page = self.tail_pages[page_index]
            # tail_page = self.tail_pages.get(page_index)
            tail_page = self.Buffer.get(page_index, "Tail")
            if record_index >= tail_page.num_records:
                return None
        
        # read every column independently
        return {
            'indirection': tail_page.physical_pages[Config.INDIRECTION_COLUMN].read(record_index),
            'rid': tail_page.physical_pages[Config.RID_COLUMN].read(record_index),
            'timestamp': tail_page.physical_pages[Config.TIMESTAMP_COLUMN].read(record_index),
            'schema_encoding': tail_page.physical_pages[Config.SCHEMA_ENCODING_COLUMN].read(record_index),
            'base_rid': tail_page.physical_pages[Config.BASE_RID_COLUMN].read(record_index),
            'columns': [
                tail_page.physical_pages[Config.USER_COLUMN_START + i].read(record_index)
                for i in range(self.num_columns)
            ]
        }
    
    def set_base_record_value(self, page_index, record_index, column_idx, value):
        # if page_index >= len(self.base_pages):
        #     return None

        # if page_index not in self.base_pages.cache:
        if page_index not in self.Buffer.base_cache:
            base_page = self.load_one_base_page_from_disk(page_index)
            if base_page == None:
                return None
            
            # evict = self.base_pages.put(page_index, base_page)
            evict = self.Buffer.put(page_index, base_page, "Base")
            if evict != None: 
                self.save_one_page_to_disk(evict[0], evict[1], "Base")
        else:
            # base_page = self.base_pages[page_index]
            # base_page = self.base_pages.get(page_index)
            base_page = self.Buffer.get(page_index, "Base")
            if record_index >= base_page.num_records:
                # print("read_base_record: ", page_index, record_index)
                return None
        
        # set value based on column_idx
        base_page.physical_pages[column_idx].update(record_index, value)

    def set_tail_record_value(self, page_index, record_index, column_idx, value):
        # if page_index >= len(self.tail_pages):
        #     return None

        # if page_index not in self.tail_pages.cache:
        if page_index not in self.Buffer.tail_cache:
            tail_page = self.load_one_tail_page_from_disk(page_index)
            if tail_page == None:
                return None
            
            # evict = self.tail_pages.put(page_index, tail_page)
            evict = self.Buffer.put(page_index, tail_page, "Tail")
            if evict != None: 
                self.save_one_page_to_disk(evict[0], evict[1], "Tail")
        else:
            # tail_page = self.tail_pages[page_index]
            # tail_page = self.tail_pages.get(page_index)
            tail_page = self.Buffer.get(page_index, "Tail")

            if record_index >= tail_page.num_records:
                return None
        
        # set value based on column_idx
        tail_page.physical_pages[column_idx].update(record_index, value)

    
    def update_base_indirection(self, page_index, record_index, new_indirection):
        # if page_index >= len(self.base_pages):
        #     return False

        # if page_index not in self.base_pages.cache:
        if page_index not in self.Buffer.base_cache:
            base_page = self.load_one_base_page_from_disk(page_index)
            if base_page == None:
                return None

            # evict = self.base_pages.put(page_index, base_page)
            evict = self.Buffer.put(page_index, base_page, "Base")
            if evict != None: 
                self.save_one_page_to_disk(evict[0], evict[1], "Base")
        else:
            # base_page = self.base_pages[page_index]
            # base_page = self.base_pages.get(page_index)
            base_page = self.Buffer.get(page_index, "Base")
        return base_page.physical_pages[Config.INDIRECTION_COLUMN].update(record_index, new_indirection)
    
    def update_base_schema_encoding(self, page_index, record_index, new_encoding):
        # if page_index >= len(self.base_pages):
        #     return False

        # if page_index not in self.base_pages.cache:
        if page_index not in self.Buffer.base_cache:
            base_page = self.load_one_base_page_from_disk(page_index)
            if base_page == None:
                return None

            # evict = self.base_pages.put(page_index, base_page)
            evict = self.Buffer.put(page_index, base_page, "Base")
            if evict != None: 
                self.save_one_page_to_disk(evict[0], evict[1], "Base")
        else:
            # base_page = self.base_pages[page_index]
            # base_page = self.base_pages.get(page_index)
            base_page = self.Buffer.get(page_index, "Base")
        return base_page.physical_pages[Config.SCHEMA_ENCODING_COLUMN].update(record_index, new_encoding)
    
    def update_base_tsp(self, page_index, record_index, new_tsp):
        # if page_index >= len(self.base_pages):
        #     return False
        # if page_index not in self.base_pages.cache:
        if page_index not in self.Buffer.base_cache:
            base_page = self.load_one_base_page_from_disk(page_index)
            if base_page == None:
                return None

            # evict = self.base_pages.put(page_index, base_page)
            evict = self.Buffer.put(page_index, base_page, "Base")
            if evict != None: 
                self.save_one_page_to_disk(evict[0], evict[1], "Base")
        else:
            # base_page = self.base_pages[page_index]
            base_page = self.Buffer.get(page_index, "Base")
    
        return base_page.physical_pages[Config.BASE_RID_COLUMN].update(record_index, new_tsp)
    
    def save_one_page_to_disk(self, page_idx, page, page_type):
        for column_idx in range(self.num_columns + Config.USER_COLUMN_START):
            page_data = page.get_page_data(column_idx)
            # if idx == 0 and column_idx == 1: print(list(page_data))
            
            page_path = os.path.join(self.table_path, str(column_idx))
            page_path = os.path.join(page_path, page_type)

            if not os.path.exists(page_path):
                os.makedirs(page_path)

            # for record_byte in page_data:
            file_path = os.path.join(page_path, str(page_idx))
            with open(file_path, "wb") as fp:
                fp.write(page_data)

    def load_one_base_page_from_disk(self, page_idx):
        base_page = BasePage(self.num_columns)
        for column_idx in range(self.num_columns + Config.USER_COLUMN_START):
            file_path = f"{self.table_path}/{column_idx}/Base/{page_idx}"
            if not os.path.exists(file_path):
                return None
            with open(file_path, "rb") as fp:
                page_data = fp.read()

            # if idx == 0 and column_idx == 1: print(list(page_data))
            base_page.set_page_data(column_idx, page_data, len(page_data)//8)
            # print(len(page_data)//8, base_page.num_records)
        
        # evict = self.base_pages.put(page_idx, base_page)
        evict = self.Buffer.put(page_idx, base_page, "Base")
        if evict != None: 
            self.save_one_page_to_disk(evict[0], evict[1], "Base")
        
        return base_page

    def load_one_tail_page_from_disk(self, page_idx):
        tail_page = TailPage(self.num_columns)
        for column_idx in range(self.num_columns + Config.USER_COLUMN_START):
            file_path = f"{self.table_path}/{column_idx}/Tail/{page_idx}"
            if not os.path.exists(file_path):
                # print("no directory", page_idx)
                return None
            with open(file_path, "rb") as fp:
                page_data = fp.read()
            
            tail_page.set_page_data(column_idx, page_data, len(page_data)//8)
       
        # evict = self.tail_pages.put(page_idx, tail_page)
        evict = self.Buffer.put(page_idx, tail_page, "Tail")
        if evict != None: 
            self.save_one_page_to_disk(evict[0], evict[1], "Tail")

        return tail_page

    def save_to_disk(self):
        # save all base pages
        # for idx, base_page in enumerate(self.base_pages):
        # for idx in self.base_pages.cache:
        for idx in self.Buffer.base_cache:
            for column_idx in range(self.num_columns + Config.USER_COLUMN_START):
                # base_page = self.base_pages.cache[idx]
                base_page = self.Buffer.base_cache[idx]
                page_data = base_page.get_page_data(column_idx)
                # if idx == 0 and column_idx == 1: print(list(page_data))
                
                page_path = os.path.join(self.table_path, str(column_idx))
                page_path = os.path.join(page_path, "Base")

                if not os.path.exists(page_path):
                    os.makedirs(page_path)

                # for record_byte in page_data:
                file_path = os.path.join(page_path, str(idx))
                with open(file_path, "wb") as fp:
                    fp.write(page_data)

        # save all tail pages
        # for idx, tail_page in enumerate(self.tail_pages):
        # for idx in self.tail_pages.cache:
        for idx in self.Buffer.tail_cache:
            for column_idx in range(self.num_columns + Config.USER_COLUMN_START):
                # tail_page = self.tail_pages.cache[idx]
                tail_page = self.Buffer.tail_cache[idx]
                page_data = tail_page.get_page_data(column_idx)
                
                page_path = os.path.join(self.table_path, str(column_idx))
                page_path = os.path.join(page_path, "Tail")

                if not os.path.exists(page_path):
                    os.makedirs(page_path)

                # for record_byte in page_data:
                file_path = os.path.join(page_path, str(idx))
                with open(file_path, "wb") as fp:
                    fp.write(page_data)

    def load_from_disk(self, num_base_pages, num_tail_pages):
        # load all base records
        for idx in range(num_base_pages):
            # print(idx)
            base_page = BasePage(self.num_columns)
            for column_idx in range(self.num_columns + Config.USER_COLUMN_START):
                file_path = f"{self.table_path}/{column_idx}/Base/{idx}"
                with open(file_path, "rb") as fp:
                    page_data = fp.read()
                
                # if idx == 0 and column_idx == 1: print(list(page_data))
                base_page.set_page_data(column_idx, page_data, len(page_data)//8)

                # print(len(page_data)//8, base_page.num_records)

            # self.base_pages.append(base_page)
            evict = self.Buffer.put(idx, base_page, "Base")
            if evict != None:
                self.save_one_page_to_disk(evict[0], evict[1], "Base") 
        # self.current_base_page = self.base_pages[-1]

        # load all tail records
        for idx in range(num_tail_pages):
            tail_page = TailPage(self.num_columns)
            for column_idx in range(self.num_columns + Config.USER_COLUMN_START):
                

                file_path = f"{self.table_path}/{column_idx}/Tail/{idx}"
                with open(file_path, "rb") as fp:
                    page_data = fp.read()
                
                tail_page.set_page_data(column_idx, page_data, len(page_data)//8)
            # self.tail_pages.append(tail_page)
            evict = self.Buffer.put(idx, tail_page, "Tail")
            if evict != None:
                self.save_one_page_to_disk(evict[0], evict[1], "Tail") 

        # self.current_tail_page = self.tail_pages[-1]            

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, dp_path, num_columns, key, num_base_records = 0, num_tail_records = 0):
        self.name = name
        self.key = key  # Which column is primary key?
        self.num_columns = num_columns
        # self.page_directory = {}
        self.table_path = os.path.join(dp_path, name)
        self.page_directory = PageRange(self.table_path, 0, num_columns, num_base_records, num_tail_records) # set range_id to 0
        self.index = Index(self)
        
        # ***** new added
        self.key_to_rid = {} # primary key value -> rid mapping
        self.key_to_rid_lock = threading.Lock()
        
        self.lock_manager = LockManager()
        # Yanliang's Modification here: Add RID allocation lock
        self.rid_lock = threading.Lock()
        
        # === new added ===
        self.is_merging = True # control the merge thread
        self.merge_thread = threading.Thread(target=self.__merge_worker)
        self.merge_thread.daemon = True # set as daemon thread, it will exit when the main program exits
        self.merge_thread.start()
        
        pass
    
    def __merge_worker(self):
        # This is the function that runs in the background thread
        while self.is_merging:
            # Check if there are tail records to merge 
            if self.page_directory.num_tail_records > 0:
                #TODO: the merge condition can be more sophisticated
                self.merge()
            
            # pause for a while before next check
            sleep(1) # try to merge every 1 second
    
    def allocate_base_rid(self):
        # Allocate a new base RID atomically
        with self.rid_lock:
            rid = self.page_directory.num_base_records
            # The real incrementation is transfered to insert_base_record()
            return rid
        
    def allocate_tail_rid(self):
        # Allocate a new tail RID atomically
        with self.rid_lock:
            rid = self.page_directory.num_tail_records
            # The real incrementation is transfered to insert_tail_record()
            return rid
    
    def get_version_rid(self, rid, relative_version):
        page_idx = rid // Config.PAGE_CAPACITY
        record_idx = rid % Config.PAGE_CAPACITY

        # Read base record
        base_record = self.page_directory.read_base_record(page_idx, record_idx)
        if base_record is None:
            return rid, 'Base'
        
        indirection = base_record['indirection']

        # if no update records
        if indirection == -1:
            return rid, 'Base'
        
        # if relative_version == 0, which means the latest version, return indirection
        if relative_version == 0:
            return indirection, 'Tail'
            
        # collect all tail rids, from latest to oldest
        tail_rids = []
        current_tail_rid = indirection
        
        while current_tail_rid != -1:
            tail_rids.append(current_tail_rid)
            
            tail_page_idx = current_tail_rid // Config.PAGE_CAPACITY
            tail_record_idx = current_tail_rid % Config.PAGE_CAPACITY
            
            tail_record = self.page_directory.read_tail_record(tail_page_idx, tail_record_idx)
            if tail_record is None:
                break
            
            current_tail_rid = tail_record['indirection']
        
        # calculate depth, absolute value, 
        depth = abs(relative_version)
        
        if depth < len(tail_rids):
            # if the depth is within the Tail chain, return the corresponding tail rid
            return tail_rids[depth], 'Tail'
        else:
            # if the depth exceeds the Tail chain (e.g., 1 update, request v-1 or v-2),
            # it means the version is in the Base Page.
            return rid, 'Base'

    # may exist issues, change to read_tail_record and read_base_record
    def get_col_value(self, rid, column_idx, page_type = 'Base'):
        if column_idx > self.num_columns:
            raise ValueError("Invalid column idx")
        
        if page_type != 'Base' and page_type != 'Tail':
            raise ValueError("invalid page type")
        
        page_idx = rid // Config.PAGE_CAPACITY
        record_index =  rid % Config.PAGE_CAPACITY

        if page_type == 'Base':
            cols = self.page_directory.read_base_record(page_idx, record_index)
            return cols['columns'][column_idx]
        else:
            cols = self.page_directory.read_tail_record(page_idx, record_index)
            return cols['columns'][column_idx]

    # return a column iteratively
    def col_iterator(self, column_idx, page_type = 'Base'):
        if column_idx > self.num_columns:
            raise ValueError("Invalid column idx")
        
        if page_type != 'Base' and page_type != 'Tail':
            raise ValueError("invalid page type")

        num_records = self.page_directory.num_base_records if page_type == 'Base' else self.page_directory.num_tail_records        
        for i in range(num_records):
            # get page index and local record index (in one page)
            page_idx = i // Config.PAGE_CAPACITY
            record_index = i % Config.PAGE_CAPACITY
            # read from page
            if(page_type == 'Base'):
                res = self.page_directory.read_base_record(page_idx, record_index)
            else:
                res = self.page_directory.read_tail_record(page_idx, record_index)
            col_value = res['columns'][column_idx]
            rid = res['rid']

            # print(res)

            # return rid, col_value Iteratively
            yield rid, col_value

    # don't need this (?) directly call page range func: insert_base_record() & append_tail_record()
    def add_record(self, columns, page_type = 'Base'):
        if(len(columns) != self.num_columns):
            ValueError("Input columns length not matches table columns")

        if(page_type == 'Base'):
            rid = self.page_directory.num_base_records
            timestamp = int(datetime.now().timestamp())
            # schema_encoding?
            res = self.page_directory.insert_base_record(rid, timestamp, columns)

        elif(page_type == 'Tail'):
            rid = self.page_directory.num_tail_records
            timestamp = int(datetime.now().timestamp())
            schema_encoding = '0'
            # indirection?
            res = self.page_directory.append_tail_record(rid, timestamp, schema_encoding, columns)

        else:
            ValueError("invalid page type.")
            
            
        # add to key_to_rid mapping if primary key column
        if self.key is not None:
            key_value = columns[self.key]
            self.key_to_rid[key_value] = rid
    
    # TODO: use a dictionary to store all records or using page_directory?
    def get_record_by_rid(self, rid):
        pass
    
    # TODO: implement delete by rid
    def delete(self, primary_key):
        # # get rid list [base_rid, tail_rid1, tail_rid2, ...]
        # rids = self.indexlocate(self.key, primary_key)
        # for idx, rid in enumerate(rid):
        #     # set rid to -1 (or other invalidate value) ?
        #     page_idx = rid // Config.PAGE_CAPACITY
        #     record_idx = rid % Config.PAGE_CAPACITY
        #     if idx == 0:
        #         self.page_directory.set_base_record_value(page_idx, record_idx, Config.RID_COLUMN, -1)
        #     else:
        #         self.page_directory.set_tail_record_value(page_idx, record_idx, Config.RID_COLUMN, -1)
        rids_list = self.index.locate(self.key, primary_key)
        base_rids = rids_list[0]
        tail_rids = rids_list[1]

        # print(rids_list)

        if len(base_rids) != 0:
            for idx, rid in enumerate(base_rids):
                # set rid to -1 (or other invalidate value) ?
                page_idx = rid // Config.PAGE_CAPACITY
                record_idx = rid % Config.PAGE_CAPACITY
                self.page_directory.set_base_record_value(page_idx, record_idx, Config.RID_COLUMN, -1)
                # print(self.page_directory.read_base_record(page_idx, record_idx))

                self.index.delete_value(primary_key)
        
        if len(tail_rids) != 0:
            for idx, rid in enumerate(tail_rids):
                # set rid to -1 (or other invalidate value) ?
                page_idx = rid // Config.PAGE_CAPACITY
                record_idx = rid % Config.PAGE_CAPACITY
                self.page_directory.set_tail_record_value(page_idx, record_idx, Config.RID_COLUMN, -1)
    
    

    # # Is merge not required?
    # def merge(self):
    #     # print("merge is happening")
    #     # suppose we merge first 3 tail pages once merge.
    #     merge_tail_page_indices = [0, 1, 2]
        
    #     for tail_page_idx in merge_tail_page_indices:
    #         num_tail_pages = math.ceil(self.page_directory.num_tail_records / Config.PAGE_CAPACITY)
    #         # check tail_page_idx not out of range
    #         if(tail_page_idx >= num_tail_pages):
    #             return False

    #         # base rids for all current tail pages
    #         # tail_page_base_rids = self.page_directory.get_tail_page(tail_page_idx, Config.BASE_RID_COLUMN)
    #         # tail_page_schema = self.page_directory.get_tail_page(tail_page_idx, Config.SCHEMA_ENCODING_COLUMN)
    #         # tail_page_rid = self.page_directory.get_tail_page(tail_page_idx, Config.RID_COLUMN)

    #         # tail_page = self.page_directory.tail_pages.get(tail_page_idx)
    #         tail_page = self.page_directory.Buffer.get(tail_page_idx, "Tail")
    #         if tail_page == None:
    #             tail_page = self.page_directory.load_one_tail_page_from_disk(tail_page_idx)
    #             if tail_page == None:
    #                 continue
    #             # evict = self.page_directory.tail_pages.put(tail_page_idx, tail_page)
    #             evict = self.page_directory.Buffer.put(tail_page_idx, tail_page, "Tail")
    #             if evict != None: 
    #                 self.page_directory.save_one_page_to_disk(evict[0], evict[1], "Tail")

    #         tail_page_base_rids = tail_page.get_a_page(Config.BASE_RID_COLUMN)
    #         tail_page_schema = tail_page.get_a_page(Config.SCHEMA_ENCODING_COLUMN)
    #         tail_page_rid = tail_page.get_a_page(Config.RID_COLUMN)
            
    #         base_page_copies = [{} for _ in range(self.num_columns)]

    #         # for each column
    #         for col_idx in range(self.num_columns):
    #             # get the tail page for this column
    #             # tail_page = self.page_directory.get_tail_page(tail_page_idx, col_idx)
    #             tail_page_column = tail_page.get_a_page(col_idx+Config.USER_COLUMN_START)

    #             updated = []
    #             # from the last updated tail record
    #             for rec_idx in range((tail_page_column.num_items - 1), -1, -1):
    #                 column_value = tail_page_column.read(rec_idx)
    #                 base_rid = tail_page_base_rids.read(rec_idx)
    #                 # find the corresponding base page
    #                 base_page_idx = base_rid // Config.PAGE_CAPACITY

    #                 if base_page_idx not in base_page_copies[col_idx]:
    #                     # base_page = copy.deepcopy(self.page_directory.get_base_page(base_page_idx, col_idx))
    #                     # base_page = (self.page_directory.base_pages.get(base_page_idx))
    #                     base_page = (self.page_directory.Buffer.get(base_page_idx, "Base"))
    #                     # print(base_page)
    #                     if base_page == None:
    #                         base_page = self.page_directory.load_one_base_page_from_disk(base_page_idx)
    #                         if base_page == None:
    #                             continue
    #                         # evict = self.base_pages.put(base_page_idx, base_page)
    #                         evict = self.page_directory.Buffer.put(base_page_idx, base_page, "Base")
    #                         if evict != None: 
    #                             self.page_directory.save_one_page_to_disk(evict[0], evict[1], "Base")
                        
    #                     base_page_copies[col_idx][base_page_idx] = copy.deepcopy(base_page.physical_pages[col_idx+Config.USER_COLUMN_START])

    #                 # base record has not been updated
    #                 if base_rid not in updated:
    #                     updated.append(base_rid)
    #                     if (tail_page_schema.read(rec_idx) >> col_idx) & 1:
    #                         base_rec_idx = base_rid % Config.PAGE_CAPACITY
    #                         base_page_copies[col_idx][base_page_idx].update(base_rec_idx, column_value)
                    
    #                 # get the tsp of current base page
    #                 base_record_idx = base_rid % Config.PAGE_CAPACITY
    #                 tsp = self.page_directory.read_base_record(base_page_idx, base_record_idx)['base_rid']
    #                 tid = tail_page_rid.read(rec_idx)
    #                 # update current tsp for the base record
    #                 if tid > tsp:
    #                     self.page_directory.update_base_tsp(base_page_idx, base_record_idx, tid)

    #         # overwrite the original base page with the updated copied base page
    #         for col_idx in range(self.num_columns):
    #             for page_idx in base_page_copies[col_idx].keys():
    #                 # self.page_directory.base_pages[page_idx] = base_page_copies[col_idx][page_idx]
    #                 self.page_directory.Buffer.set(page_idx, base_page_copies[col_idx][page_idx], (col_idx+Config.USER_COLUMN_START), "Base")
    
    
    # In lstore/table.py -> class Table

    def merge(self):
        # obtain the number of tail pages
        num_tail_pages = math.ceil(self.page_directory.num_tail_records / Config.PAGE_CAPACITY)
        if num_tail_pages == 0:
            return

        # reverse order of tail pages to merge from latest to oldest
        merge_tail_page_indices = list(range(num_tail_pages))
        merge_tail_page_indices.reverse() # [N, N-1, ... 0]
        # create a list to hold base page copies for each column
        base_page_copies = [{} for _ in range(self.num_columns)]
        
        # record RIDs that have been consolidated
        consolidated_rids = set()

        # for each column, process all tail pages
        for col_idx in range(self.num_columns):
            updated_in_this_col = set() # record RIDs that have been updated in this column

            for tail_page_idx in merge_tail_page_indices:
                # Load Tail Page
                tail_page = self.page_directory.Buffer.get(tail_page_idx, "Tail")
                if tail_page is None:
                    tail_page = self.page_directory.load_one_tail_page_from_disk(tail_page_idx)
                if tail_page is None: continue

                # Get physical page data
                tail_col_data = tail_page.get_a_page(col_idx + Config.USER_COLUMN_START)
                tail_base_rids = tail_page.get_a_page(Config.BASE_RID_COLUMN)
                tail_schemas = tail_page.get_a_page(Config.SCHEMA_ENCODING_COLUMN)

                # Traverse records in reverse order (ensure latest records are processed first)
                for rec_idx in range(tail_col_data.num_items - 1, -1, -1):
                    base_rid = tail_base_rids.read(rec_idx)

                    # If the RID for this column is already the latest value, skip old updates
                    if base_rid in updated_in_this_col:
                        continue

                    # Check Schema to confirm if the Tail Record has updated the current column
                    schema_val = tail_schemas.read(rec_idx)
                    if (schema_val >> col_idx) & 1:
                        # Load or get Base Page copy
                        base_page_idx = base_rid // Config.PAGE_CAPACITY
                        if base_page_idx not in base_page_copies[col_idx]:
                            base_page = self.page_directory.Buffer.get(base_page_idx, "Base")
                            if base_page is None:
                                base_page = self.page_directory.load_one_base_page_from_disk(base_page_idx)
                            
                            if base_page is None:
                                continue
                            
                            base_page_copies[col_idx][base_page_idx] = copy.deepcopy(base_page.physical_pages[col_idx + Config.USER_COLUMN_START])

                        # Update Base Page copy with Tail value
                        base_rec_idx = base_rid % Config.PAGE_CAPACITY
                        val = tail_col_data.read(rec_idx)
                        base_page_copies[col_idx][base_page_idx].update(base_rec_idx, val)
                        
                        # tag the RID as updated in this column
                        updated_in_this_col.add(base_rid)
                        consolidated_rids.add(base_rid)

        for col_idx in range(self.num_columns):
            for base_page_idx, phy_page in base_page_copies[col_idx].items():
                base_page = self.page_directory.Buffer.get(base_page_idx, "Base")
                if base_page:
                     base_page.physical_pages[col_idx + Config.USER_COLUMN_START] = phy_page

        # reset indirection and schema encoding for consolidated RIDs
        for rid in consolidated_rids:
            page_idx = rid // Config.PAGE_CAPACITY
            rec_idx = rid % Config.PAGE_CAPACITY
            
            # Indirection -> -1 (the base record is now the latest)
            self.page_directory.update_base_indirection(page_idx, rec_idx, -1)
            # Schema -> 0 (no Tail Update)
            self.page_directory.update_base_schema_encoding(page_idx, rec_idx, 0)
                    
    # more methods coupled with saving DB
    # ---------- persistence helpers ----------

    # close function for Table class
    def close(self):
        self.is_merging = False # close the thread
        if self.merge_thread:
            self.merge_thread.join()
        # save all records
        self.page_directory.save_to_disk()
        pass
    
    # TODO: Write the following rollback logic
    def rollback_insert(self, rid):
        """
        Undoes an insert operation.
        1. Removes the RID from the page directory.
        2. Removes the RID from the index.
        """
        page_idx = rid // Config.PAGE_CAPACITY
        record_idx = rid % Config.PAGE_CAPACITY
        
        # get record to find key value
        record = self.page_directory.read_base_record(page_idx, record_idx)
        if record is None:
            return
        # if record:
        columns = record['columns']
        key_value = record['columns'][self.key]
            
        # remove from index
        if self.index:
            self.index.remove_from_index(rid, columns)
        
        # set base record as ineffective
        self.page_directory.set_base_record_value(page_idx, record_idx, Config.RID_COLUMN, -1)
        
        # restore indirection
        self.page_directory.update_base_indirection(page_idx, record_idx, -1)
        
        # remove from key_to_rid if used
        if hasattr(self, 'key_to_rid_lock'):
            with self.key_to_rid_lock:
                if key_value in self.key_to_rid:
                    del self.key_to_rid[key_value]
        elif key_value in self.key_to_rid:
            del self.key_to_rid[key_value]

    
    
    def rollback_update(self, rid, old_indirection, old_primary_key=None):
        """
        Undoes an update operation.
        1. Locates the base record using RID.
        2. Reverts the INDIRECTION column to point to 'old_indirection'.
        """
        # page_idx = rid // Config.PAGE_CAPACITY
        # record_idx = rid % Config.PAGE_CAPACITY
        
        # # update indirection to old_indirection
        # self.page_directory.update_base_indirection(page_idx, record_idx, old_indirection)
        
        # Yanliang's Modification here: We only restore indirection here, leaving newly created tail records
        # New design here:
        # 1. Restore indirection
        # 2. Mark new tail record as ineffective (RID => -1)
        # 3. Remove newly created tail record from index
        page_idx = rid // Config.PAGE_CAPACITY
        record_idx = rid % Config.PAGE_CAPACITY
        
        record = self.page_directory.read_base_record(page_idx, record_idx)
        if record is None:
            return
        
        new_tail_rid = record['indirection']
        
        # Restore indirection
        self.page_directory.update_base_indirection(page_idx, record_idx, old_indirection)
        
        # Mark new tail record as ineffective (RID => -1)
        if new_tail_rid != -1 and new_tail_rid != old_indirection:
            tail_page_idx = new_tail_rid // Config.PAGE_CAPACITY
            tail_record_idx = new_tail_rid % Config.PAGE_CAPACITY
            
            tail_record = self.page_directory.read_tail_record(tail_page_idx, tail_record_idx)
            if tail_record is not None:
                self.page_directory.set_tail_record_value(
                    tail_page_idx, 
                    tail_record_idx, 
                    Config.RID_COLUMN, 
                    -1
                )

        # Restore index only when the primary key changes
        if old_primary_key is not None:
            current_primary_key = record['columns'][self.key]
            if old_primary_key != current_primary_key:
                # Remove new
                if self.index.indices[self.key]:
                    self.index.indices[self.key].remove_rid(current_primary_key, rid)
                # Restore old
                if self.index.indices[self.key]:
                    self.index.indices[self.key].add(old_primary_key, rid, "Base")
    
    def rollback_delete(self, rid):
        """
        Undoes a delete operation.
        1. Locates the record using RID.
        2. Restores the RID column to its original value.
        """
        page_idx = rid // Config.PAGE_CAPACITY
        record_idx = rid % Config.PAGE_CAPACITY
        
        # restore RID column to original value
        self.page_directory.set_base_record_value(page_idx, record_idx, Config.RID_COLUMN, rid)
    
        # restore index
        record = self.page_directory.read_base_record(page_idx, record_idx)
        if record is None:
            return

        columns = record['columns']
        key_value = columns[self.key]
        
        if self.index:
            self.index.insert_value(columns, rid, "Base")
            
        if hasattr(self, 'key_to_rid_lock'):
            with self.key_to_rid_lock:
                self.key_to_rid[key_value] = rid
        else:
            self.key_to_rid[key_value] = rid
            