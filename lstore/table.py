from lstore.index import Index
from time import time
from page import *

from datetime import datetime

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

PAGE_CAPACITY = 4096 // 8

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

# According to assignment 1's description:
# "These invalidated records will be removed during the next merge cycle for the corresponding page range."
class PageRange:
    # Manage a set of base pages and tail pages
    def __init__(self, range_id, num_columns):
        self.range_id = range_id
        self.num_columns = num_columns
        
        self.base_pages = []
        self.tail_pages = []
        
        self.current_base_page = None
        self.current_tail_page = None
        
        self.num_base_records = 0
        self.num_tail_records = 0
        
        # Initialize 1st base page and tail page
        self._allocate_base_page()
        self._allocate_tail_page()
    
    def _allocate_base_page(self):
        new_page = BasePage(self.num_columns)
        self.base_pages.append(new_page)
        self.current_base_page = new_page
        return new_page
    
    def _allocate_tail_page(self):
        new_page = TailPage(self.num_columns)
        self.tail_pages.append(new_page)
        self.current_tail_page = new_page
        return new_page
    
    def has_base_capacity(self):
        return self.current_base_page.has_capacity()
    
    def has_tail_capacity(self):
        return self.current_tail_page.has_capacity()
    
    def insert_base_record(self, rid, timestamp, columns):
        if not self.has_base_capacity():
            self._allocate_base_page()
        
        success = self.current_base_page.insert_record(rid, timestamp, columns)
        if success:
            # We need to re-write the index part
            page_index = len(self.base_pages) - 1
            record_index = self.current_base_page.num_records - 1
            self.num_base_records += 1
            return (page_index, record_index)
        return None
    
    # Some specical prpcess here I think?
    def append_tail_record(self, rid, indirection, timestamp, schema_encoding, columns):
        if not self.has_tail_capacity():
            self._allocate_tail_page()
        
        success = self.current_tail_page.append_update(
            rid, indirection, timestamp, schema_encoding, columns
        )
        if success:
            page_index = len(self.tail_pages) - 1
            record_index = self.current_tail_page.num_records - 1
            self.num_tail_records += 1
            return (page_index, record_index)
        return None
    
    def read_base_record(self, page_index, record_index):
        if page_index >= len(self.base_pages):
            return None
        
        base_page = self.base_pages[page_index]
        if record_index >= base_page.num_records:
            return None
        
        # read every column independently
        return {
            'indirection': base_page.physical_pages[INDIRECTION_COLUMN].read(record_index),
            'rid': base_page.physical_pages[RID_COLUMN].read(record_index),
            'timestamp': base_page.physical_pages[TIMESTAMP_COLUMN].read(record_index),
            'schema_encoding': base_page.physical_pages[SCHEMA_ENCODING_COLUMN].read(record_index),
            'columns': [
                base_page.physical_pages[base_page.USER_COLUMN_START + i].read(record_index)
                for i in range(self.num_columns)
            ]
        }
    
    def read_tail_record(self, page_index, record_index):
        if page_index >= len(self.tail_pages):
            return None
        
        tail_page = self.tail_pages[page_index]
        if record_index >= tail_page.num_records:
            return None
        
        # read every column independently
        return {
            'indirection': tail_page.physical_pages[INDIRECTION_COLUMN].read(record_index),
            'rid': tail_page.physical_pages[RID_COLUMN].read(record_index),
            'timestamp': tail_page.physical_pages[TIMESTAMP_COLUMN].read(record_index),
            'schema_encoding': tail_page.physical_pages[SCHEMA_ENCODING_COLUMN].read(record_index),
            'columns': [
                tail_page.physical_pages[tail_page.USER_COLUMN_START + i].read(record_index)
                for i in range(self.num_columns)
            ]
        }
    
    def update_base_indirection(self, page_index, record_index, new_indirection):
        if page_index >= len(self.base_pages):
            return False
        
        base_page = self.base_pages[page_index]
        return base_page.physical_pages[INDIRECTION_COLUMN].update(record_index, new_indirection)
    
    def update_base_schema_encoding(self, page_index, record_index, new_encoding):
        if page_index >= len(self.base_pages):
            return False
        
        base_page = self.base_pages[page_index]
        return base_page.physical_pages[SCHEMA_ENCODING_COLUMN].update(record_index, new_encoding)



class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key  # Which column is primary key?
        self.num_columns = num_columns
        # self.page_directory = {}
        self.page_directory = PageRange(0, num_columns) # set range_id to 0
        self.index = Index(self)
        pass
    
    def get_version_rid(self, rid, relative_version):

        # base record
        page_idx = PAGE_CAPACITY // rid
        record_idx = PAGE_CAPACITY % rid

        record = self.page_directory.read_base_record(page_idx, record_idx)
        indirection = record['indirection']

        # not indirection
        if indirection == 0:
            return rid, 'Base'
        
        # 1st updated record
        rid = indirection
        page_idx = PAGE_CAPACITY // rid
        record_idx = PAGE_CAPACITY % rid

        record = self.page_directory.read_base_record(page_idx, record_idx)
        indirection = record['indirection']
        
        version = 0
        while version > relative_version and indirection != -1:
            rid = indirection
            page_idx = PAGE_CAPACITY // rid
            record_idx = PAGE_CAPACITY % rid

            record = self.page_directory.read_base_record(page_idx, record_idx)
            indirection = record['indirection']

            version -= 1
        
        return rid, 'Tail'


    def get_col_value(self, rid, column_idx, page_type = 'Base'):
        if column_idx > len(self.num_columns):
            raise ValueError("Invalid column idx")
        
        if page_type != 'Base' and page_type != 'Tail':
            raise ValueError("invalid page type")
        
        page_idx = rid // PAGE_CAPACITY
        record_index =  rid % PAGE_CAPACITY

        if page_type == 'Base':
            cols = self.page_directory.read_base_record(page_idx, record_index)
            return cols['columns'][column_idx]
        else:
            cols = self.page_directory.read_tail_record(page_idx, record_index)
            return cols['columns'][column_idx]

    # return a column iteratively
    def col_iterator(self, column_idx, page_type = 'Base'):
        if column_idx > len(self.num_columns):
            raise ValueError("Invalid column idx")
        
        if page_type != 'Base' and page_type != 'Tail':
            raise ValueError("invalid page type")

        num_records = self.page_directory.num_base_records if page_type == 'Base' else self.page_directory.num_tail_records
        
        for i in range(num_records):
            # get page index and local record index (in one page)
            page_idx = i // PAGE_CAPACITY
            record_index = i % PAGE_CAPACITY
            # read from page
            if(page_type == 'Base'):
                res = self.page_directory.read_base_record(page_idx, record_index)
            else:
                res = self.page_directory.read_tail_record(page_idx, record_index)
            col_value = res['columns'][column_idx]
            rid = res['rid']

            # return rid, col_value Iteratively
            yield rid, col_value

    
    def add_record(self, columns, page_type = 'Base'):
        if(len(columns) != self.num_columns):
            ValueError("Input columns length not matches table columns")

        if(page_type == 'Base'):
            rid = self.page_directory.num_base_records
            timestamp = int(datetime.now().current_datetime.timestamp())
            # schema_encoding?
            res = self.page_directory.insert_base_record(rid, timestamp, columns)

        elif(page_type == 'Tail'):
            rid = self.page_directory.num_tail_records
            timestamp = int(datetime.now().current_datetime.timestamp())
            schema_encoding = '0'
            # indirection?
            res = self.page_directory.append_tail_record(rid, timestamp, schema_encoding, columns)

        else:
            ValueError("invalid page type.")

    def delete(self):
        pass

    # Is merge not required?
    def __merge(self):
        print("merge is happening")
        pass