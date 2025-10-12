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

        value_bytes = value.to_bytes(8, bytearray="little", signed=False)
        self.data[offset : offset + 8] = value_bytes

        self.num_records += 1
        return True
    
    def read(self, index):
        if index < 0 or index >= self.num_records:
            return None
        offset = index * 8
        return int.from_bytes(
            self.data[offset:offset + 8],
            byteorder='little',
            signed=False
        )
        
    def update(self, index, value):
        if index < 0 or index >= self.num_records:
            return False
        offset = index * 8
        value_bytes = value.to_bytes(8, byteorder='little', signed=False)
        self.data[offset:offset + 8] = value_bytes
        return True
        

    
# BasePage and TailPage are identical in physical view but is different in logic view
class BasePage():
    # Only write when insert new records
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.physical_pages = [Page() for _ in range(num_columns + 3)]
        
        # column indexes
        self.RID_COLUMN = 0
        self.INDIRECTION_COLUMN = 1
        self.SCHEMA_ENCODING_COLUMN = 2
        self.USER_COLUMN_START = 3
        
        self.num_columns = 0
        
    def has_capacity(self):
        return self.physical_pages[0].has_capacity()

    def insert_record(self, rid, columns):
        if not self.has_capacity():
            return False
        
        self.physical_pages[self.RID_COLUMN].write(rid)
        self.physical_pages[self.INDIRECTION_COLUMN].write(0)
        self.physical_pages[self.SCHEMA_ENCODING_COLUMN].write(0)
        
        for i, value in enumerate(columns):
            self.physical_pages[self.USER_COLUMN_START + i].write(value)
        
        self.num_records += 1
        return True
    

class TailPage():
    # Only write when update existing records
    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.physical_pages = [Page() for _ in range(num_columns + 3)]
        
        # column indexes
        self.RID_COLUMN = 0
        self.INDIRECTION_COLUMN = 1
        self.SCHEMA_ENCODING_COLUMN = 2
        self.USER_COLUMN_START = 3
        
        self.num_columns = 0
        
    def has_capacity(self):
        return self.physical_pages[0].has_capacity()
    
    def append_update(self, rid, indirection, schema_encoding, columns):
        if not self.has_capacity():
            return False
        
        self.physical_pages[self.RID_COLUMN].write(rid)
        self.physical_pages[self.INDIRECTION_COLUMN].write(indirection)
        self.physical_pages[self.SCHEMA_ENCODING_COLUMN].write(schema_encoding)
        
        for i, value in enumerate(columns):
            actual_value = value if value is not None else 0
            self.physical_pages[self.USER_COLUMN_START + i].write(actual_value)
        
        self.num_records += 1
        return True

