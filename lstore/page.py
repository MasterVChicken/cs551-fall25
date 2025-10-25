# BasePage is only a container in physical view
class Page:
    # Page has a fixed size of 4096 bytes
    # All columns are 64-bit integers
    # 1 Page only store 1 column
    # Simulate as pages in hardware view
    # Using little-endian and unsigned for byte stream by default
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.max_records = 4096 // 8

    def has_capacity(self):
        return self.num_records < self.max_records
    
    def get_capacity(self):
        return self.max_records - self.num_records
        
    def is_full(self):
        return self.num_records >= self.max_records

    def write(self, value):
        # Make sure we have space
        if not self.has_capacity():
            return False

        offset = self.num_records * 8

        # value_bytes = value.to_bytes(8, byteorder="little", signed=False)
        value_bytes = value.to_bytes(8, byteorder="little", signed=True)
        self.data[offset : offset + 8] = value_bytes

        self.num_records += 1
        return True
    
    def read(self, index):
        if index < 0 or index >= self.num_records:
            return None
        offset = index * 8
        # return int.from_bytes(
        #     self.data[offset:offset + 8],
        #     byteorder='little',
        #     signed=False
        # )
        return int.from_bytes(
            self.data[offset:offset + 8],
            byteorder='little',
            signed=True
        )
        
    def update(self, index, value):
        if index < 0 or index >= self.num_records:
            return False
        offset = index * 8
        # value_bytes = value.to_bytes(8, byteorder='little', signed=False)
        value_bytes = value.to_bytes(8, byteorder='little', signed=True)
        self.data[offset:offset + 8] = value_bytes
        return True
        

    
# BasePage and TailPage are identical in physical view but is different in logic view
# Notice: We also import metadata columns from L-store design
class BasePage():
    # Only write when insert new records
    def __init__(self, num_columns):
        self.num_columns = num_columns
        
        # column indexes
        self.INDIRECTION_COLUMN = 0
        self.RID_COLUMN = 1
        self.TIMESTAMP_COLUMN = 2
        self.SCHEMA_ENCODING_COLUMN = 3
        self.USER_COLUMN_START = 4
        
        self.physical_pages = [Page() for _ in range(self.USER_COLUMN_START + num_columns)]
        self.num_records = 0
        
    def has_capacity(self):
        return self.physical_pages[0].has_capacity()

    def insert_record(self, rid, timestamp, columns):
        if not self.has_capacity():
            return False
        
        self.physical_pages[self.INDIRECTION_COLUMN].write(-1)
        self.physical_pages[self.RID_COLUMN].write(rid)
        self.physical_pages[self.TIMESTAMP_COLUMN].write(timestamp)
        self.physical_pages[self.SCHEMA_ENCODING_COLUMN].write(0)
        
        for i, value in enumerate(columns):
            self.physical_pages[self.USER_COLUMN_START + i].write(value)
        
        self.num_records += 1
        return True
    
    # # Maybe we should define read()
    # def read_record():
    

class TailPage():
    # Only write when update existing records
    def __init__(self, num_columns):
        self.num_columns = num_columns
        
        # column indexes
        self.INDIRECTION_COLUMN = 0
        self.RID_COLUMN = 1
        self.TIMESTAMP_COLUMN = 2
        self.SCHEMA_ENCODING_COLUMN = 3
        self.USER_COLUMN_START = 4
        
        self.physical_pages = [Page() for _ in range(self.USER_COLUMN_START + num_columns)]
        self.num_records = 0
        
    def has_capacity(self):
        return self.physical_pages[0].has_capacity()
    
    def append_update(self, rid, indirection, timestamp, schema_encoding, columns):
        if not self.has_capacity():
            return False
        
        self.physical_pages[self.INDIRECTION_COLUMN].write(indirection)
        self.physical_pages[self.RID_COLUMN].write(rid)
        self.physical_pages[self.TIMESTAMP_COLUMN].write(timestamp)
        self.physical_pages[self.SCHEMA_ENCODING_COLUMN].write(schema_encoding)
        
        for i, value in enumerate(columns):
            actual_value = value if value is not None else 0
            self.physical_pages[self.USER_COLUMN_START + i].write(actual_value)
        
        self.num_records += 1
        return True
    
    # # Maybe we should define read()
    # def read_record():


# Questions: how to represent NULL?
# I use 0 in insert_record() and append_update()