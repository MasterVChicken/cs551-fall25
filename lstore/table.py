from lstore.index import Index
from time import time
from lstore.page import *
from lstore.cache_policy import LRUCache
from lstore.config import Config
from datetime import datetime

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
        self.cache_capacity = 20
        self.Buffer = LRUCache(self.cache_capacity)
        
        self.current_base_page = None
        self.current_tail_page = None
        
        # self.num_base_records = 0
        # self.num_tail_records = 0
        self.num_base_records = num_base_records
        self.num_tail_records = num_tail_records
        
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
    
    def insert_base_record(self, rid, timestamp, columns):
        if not self.has_base_capacity():
            self._allocate_base_page()
        
        success = self.current_base_page.insert_record(rid, timestamp, columns)
        if success:
            # We need to re-write the index part
            # page_index = len(self.base_pages) - 1
            page_index = self.num_base_records // Config.PAGE_CAPACITY
            record_index = self.current_base_page.num_records - 1
            self.num_base_records += 1
            return (page_index, record_index)
        return None
    
    # Some specical prpcess here I think?
    def append_tail_record(self, rid, indirection, timestamp, schema_encoding, base_rid, columns):
        if not self.has_tail_capacity():
            self._allocate_tail_page()
        
        success = self.current_tail_page.append_update(
            rid, indirection, timestamp, schema_encoding, base_rid, columns
        )
        if success:
            # page_index = len(self.tail_pages) - 1
            page_index = self.num_tail_records // Config.PAGE_CAPACITY
            record_index = self.current_tail_page.num_records - 1
            self.num_tail_records += 1
            return (page_index, record_index)
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
        base_page.physical_pages[column_idx].write(value)

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
        tail_page.physical_pages[column_idx].write(value)

    
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
        
        pass
    
    def get_version_rid(self, rid, relative_version):

        # base record
        page_idx =  rid // Config.PAGE_CAPACITY
        record_idx = rid % Config.PAGE_CAPACITY

        record = self.page_directory.read_base_record(page_idx, record_idx)
        indirection = record['indirection']

        # not indirection
        if indirection == -1:
            return rid, 'Base'
        
        # 1st updated record
        rid = indirection
        page_idx = rid // Config.PAGE_CAPACITY
        record_idx = rid % Config.PAGE_CAPACITY

        record = self.page_directory.read_tail_record(page_idx, record_idx)
        indirection = record['indirection']
        
        version = 0
        while version > relative_version and indirection != -1:
            rid = indirection
            page_idx = rid // Config.PAGE_CAPACITY
            record_idx = rid % Config.PAGE_CAPACITY

            record = self.page_directory.read_tail_record(page_idx, record_idx)
            indirection = record['indirection']

            version -= 1

        # find relative tail record
        if version == relative_version:
            return rid, 'Tail'
        # relative version is base record
        elif indirection == -1:
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
        # get rid list [base_rid, tail_rid1, tail_rid2, ...]
        rids = self.indexlocate(self.key, primary_key)
        for idx, rid in enumerate(rid):
            # set rid to -1 (or other invalidate value) ?
            page_idx = rid // Config.PAGE_CAPACITY
            record_idx = rid % Config.PAGE_CAPACITY
            if idx == 0:
                self.page_directory.set_base_record_value(page_idx, record_idx, Config.RID_COLUMN, -1)
            else:
                self.page_directory.set_tail_record_value(page_idx, record_idx, Config.RID_COLUMN, -1)
    

    # Is merge not required?
    def merge(self):
        # print("merge is happening")
        # suppose we merge first 2 tail pages once merge.
        merge_tail_page_indices = [0, 1]
        
        for tail_page_idx in merge_tail_page_indices:
            num_tail_pages = math.ceil(self.page_directory.num_tail_records / Config.PAGE_CAPACITY)
            # check tail_page_idx not out of range
            if(tail_page_idx >= num_tail_pages):
                return False

            # base rids for all current tail pages
            # tail_page_base_rids = self.page_directory.get_tail_page(tail_page_idx, Config.BASE_RID_COLUMN)
            # tail_page_schema = self.page_directory.get_tail_page(tail_page_idx, Config.SCHEMA_ENCODING_COLUMN)
            # tail_page_rid = self.page_directory.get_tail_page(tail_page_idx, Config.RID_COLUMN)

            # tail_page = self.page_directory.tail_pages.get(tail_page_idx)
            tail_page = self.page_directory.Buffer.get(tail_page_idx, "Tail")
            if tail_page == None:
                tail_page = self.page_directory.load_one_tail_page_from_disk(tail_page_idx)
                if tail_page == None:
                    continue
                # evict = self.page_directory.tail_pages.put(tail_page_idx, tail_page)
                evict = self.page_directory.Buffer.put(tail_page_idx, tail_page, "Tail")
                if evict != None: 
                    self.page_directory.save_one_page_to_disk(evict[0], evict[1], "Tail")

            tail_page_base_rids = tail_page.get_a_page(Config.BASE_RID_COLUMN)
            tail_page_schema = tail_page.get_a_page(Config.SCHEMA_ENCODING_COLUMN)
            tail_page_rid = tail_page.get_a_page(Config.RID_COLUMN)
            
            base_page_copies = [{} for _ in range(self.num_columns)]

            # for each column
            for col_idx in range(self.num_columns):
                # get the tail page for this column
                # tail_page = self.page_directory.get_tail_page(tail_page_idx, col_idx)
                tail_page_column = tail_page.get_a_page(col_idx)

                updated = []
                # from the last updated tail record
                for rec_idx in range((tail_page_column.num_items - 1), -1, -1):
                    column_value = tail_page_column.read(rec_idx)
                    base_rid = tail_page_base_rids.read(rec_idx)
                    # find the corresponding base page
                    base_page_idx = base_rid // Config.PAGE_CAPACITY

                    if base_page_idx not in base_page_copies[col_idx]:
                        # base_page = copy.deepcopy(self.page_directory.get_base_page(base_page_idx, col_idx))
                        # base_page = (self.page_directory.base_pages.get(base_page_idx))
                        base_page = (self.page_directory.Buffer.get(base_page_idx, "Base"))
                        if base_page == None:
                            base_page = self.page_directory.load_one_base_page_from_disk(base_page_idx)
                            if base_page == None:
                                continue
                            # evict = self.base_pages.put(base_page_idx, base_page)
                            evict = self.page_directory.Buffer.put(base_page_idx, base_page, "Base")
                            if evict != None: 
                                self.page_directory.save_one_page_to_disk(evict[0], evict[1], "Base")
                        
                        base_page_copies[col_idx][base_page_idx] = copy.deepcopy(base_page.physical_pages[col_idx])
                    
                    # base record has not been updated
                    if base_rid not in updated:
                        updated.append(base_rid)
                        if (tail_page_schema.read(rec_idx) >> col_idx) & 1:
                            base_page_copies[col_idx][base_page_idx].write(column_value)
                    
                    # get the tsp of current base page
                    base_record_idx = base_rid % Config.PAGE_CAPACITY
                    tsp = self.page_directory.read_base_record(base_page_idx, base_record_idx)['base_rid']
                    tid = tail_page_rid.read(rec_idx)
                    # update current tsp for the base record
                    if tid > tsp:
                        self.page_directory.update_base_tsp(base_page_idx, base_record_idx, tid)

            # overwrite the original base page with the updated copied base page
            for col_idx in range(len(base_page_copies)):
                for page_idx in base_page_copies[col_idx].keys():
                    # self.page_directory.base_pages[page_idx] = base_page_copies[col_idx][page_idx]
                    self.page_directory.Buffer.set(page_idx, base_page_copies[col_idx][page_idx], "Base")
        
    # more methods coupled with saving DB
    # ---------- persistence helpers ----------

    # close function for Table class
    def close(self):
        # save all records
        self.page_directory.save_to_disk()
        pass