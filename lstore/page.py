# BasePage is only a container in physical view
from lstore.config import Config

class Page:
    # Page has a fixed size of 4096 bytes
    # All columns are 64-bit integers
    # 1 Page only store 1 column
    # Simulate as pages in hardware view
    # Using little-endian and signed for byte stream by default
    def __init__(self):
        self.num_items = 0
        self.data = bytearray(4096)
        # I want to add header during storage
        # Considering set the first 8 bytes as header within the file
        # self.max_items = 4096 // 8
        self.max_items = Config.PAGE_CAPACITY

    def has_capacity(self):
        return self.num_items < self.max_items
    
    def get_capacity(self):
        return self.max_items - self.num_items
        
    def is_full(self):
        return self.num_items >= self.max_items

    def write(self, value):
        # Make sure we have space
        if not self.has_capacity():
            return False

        offset = self.num_items * 8

        value_bytes = value.to_bytes(8, byteorder="little", signed=True)
        self.data[offset : offset + 8] = value_bytes

        self.num_items += 1
        return True
    
    def read(self, index):
        if index < 0 or index >= self.num_items:
            return None
        offset = index * 8
        return int.from_bytes(
            self.data[offset:offset + 8],
            byteorder='little',
            signed=True
        )
        
    def update(self, index, value):
        if index < 0 or index >= self.num_items:
            return False
        offset = index * 8
        value_bytes = value.to_bytes(8, byteorder='little', signed=True)
        self.data[offset:offset + 8] = value_bytes
        return True
    
    # Persistence helper functions for read and write raw bytearray
    def get_data(self):
        return self.data
    
    def set_data(self, data, num_items):
        self.data = bytearray(data)
        self.num_items = num_items
        

    
# BasePage and TailPage are identical in physical view but different in logic view
# Notice: We also import metadata columns from L-store design
class BasePage():
    # Only write when insert new records
    def __init__(self, num_columns):
        self.num_columns = num_columns
        
        # # column indexes
        # self.INDIRECTION_COLUMN = 0
        # self.RID_COLUMN = 1
        # self.TIMESTAMP_COLUMN = 2
        # self.SCHEMA_ENCODING_COLUMN = 3

        # self.BASE_RID_COLUMN = 4
        # self.USER_COLUMN_START = 5
        
        self.physical_pages = [Page() for _ in range(Config.USER_COLUMN_START + num_columns)]
        self.num_records = 0
        
    def has_capacity(self):
        return self.physical_pages[0].has_capacity()

    def insert_record(self, rid, timestamp, columns):
        if not self.has_capacity():
            return False
        
        self.physical_pages[Config.INDIRECTION_COLUMN].write(-1)
        self.physical_pages[Config.RID_COLUMN].write(rid)
        self.physical_pages[Config.TIMESTAMP_COLUMN].write(timestamp)
        self.physical_pages[Config.SCHEMA_ENCODING_COLUMN].write(0)

        self.physical_pages[Config.BASE_RID_COLUMN].write(-1)
        
        for i, value in enumerate(columns):
            self.physical_pages[Config.USER_COLUMN_START + i].write(value)
        
        self.num_records += 1
        return True
    
    # Persistence helper functions for BasePage
    def get_a_page(self, column_index):
        if column_index >= len(self.physical_pages):
            return None
        return self.physical_pages[column_index]
    
    def get_page_data(self, column_index):
        if column_index >= len(self.physical_pages):
            return None
        return self.physical_pages[column_index].get_data()
    
    def set_page_data(self, column_index, data, num_records):
        if column_index >= len(self.physical_pages):
            return False
        
        self.physical_pages[column_index].set_data(data, num_records)
        # Align num of records with the first column
        # if column_index == 0:
        #   self.num_records = num_records
        self.num_records = num_records
        return True
    
    
    # # Maybe we should define read()
    # def read_record():
    

class TailPage():
    # Only write when update existing records
    def __init__(self, num_columns):
        self.num_columns = num_columns
        
        # column indexes
        # self.INDIRECTION_COLUMN = 0
        # self.RID_COLUMN = 1
        # self.TIMESTAMP_COLUMN = 2
        # self.SCHEMA_ENCODING_COLUMN = 3

        # self.BASE_RID_COLUMN = 4
        # self.USER_COLUMN_START = 5

        
        self.physical_pages = [Page() for _ in range(Config.USER_COLUMN_START + num_columns)]
        self.num_records = 0
        
    def has_capacity(self):
        return self.physical_pages[0].has_capacity()
    
    def append_update(self, rid, indirection, timestamp, schema_encoding, base_rid, columns):
        if not self.has_capacity():
            return False
        
        self.physical_pages[Config.INDIRECTION_COLUMN].write(indirection)
        self.physical_pages[Config.RID_COLUMN].write(rid)
        self.physical_pages[Config.TIMESTAMP_COLUMN].write(timestamp)
        self.physical_pages[Config.SCHEMA_ENCODING_COLUMN].write(schema_encoding)
        self.physical_pages[Config.BASE_RID_COLUMN].write(base_rid)
        
        for i, value in enumerate(columns):
            actual_value = value if value is not None else 0
            self.physical_pages[Config.USER_COLUMN_START + i].write(actual_value)
        
        self.num_records += 1
        return True
    
    # Persistence helper functions for TailPage
    def get_a_page(self, column_index):
        if column_index >= len(self.physical_pages):
            return None
        return self.physical_pages[column_index]

    def get_page_data(self, column_index):
        if column_index >= len(self.physical_pages):
            return None
        return self.physical_pages[column_index].get_data()
    
    def set_page_data(self, column_index, data, num_records):
        if column_index >= len(self.physical_pages):
            return False
        
        self.physical_pages[column_index].set_data(data, num_records)
        # Align num of records with the first column
        # if column_index == 0:
            # self.num_records = num_records
        self.num_records = num_records
        return True
    
    # # Maybe we should define read()
    # def read_record():


# Questions: how to represent NULL?
# I use 0 in insert_record() and append_update()
