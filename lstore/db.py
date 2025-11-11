from lstore.table import Table

# class Database():

#     def __init__(self):
#         # self.tables = []
#         self.tables = {}
#         pass

#     # Not required for milestone1
#     def open(self, path):
#         pass

#     def close(self):
#         pass

#     """
#     # Creates a new table
#     :param name: string         #Table name
#     :param num_columns: int     #Number of Columns: all columns are integer
#     :param key: int             #Index of table key in columns
#     """
#     def create_table(self, name, num_columns, key_index):
#         if(name in self.tables):
#             raise ValueError(f"Table {name} is already existed.")
        
#         table = Table(name, num_columns, key_index)
#         # add created table to table dict
#         self.tables[name] = table

#         return table

    
#     """
#     # Deletes the specified table
#     """
#     def drop_table(self, name):
#         if(name not in self.tables):
#             raise ValueError(f"No table named: {name} found!")
        
#         # overwrite del (not determine)
        
#         del self.tables[name]

    
#     """
#     # Returns table with the passed name
#     """
#     def get_table(self, name):
        
#         if(name not in self.tables): 
#             raise ValueError(f"No table named: {name} found!")
        
#         return self.tables[name]


"""
One possible implementation

"""

import os
import json
import pickle
import struct
from lstore.table import Table


class Database():

    def __init__(self):
        self.tables = {}
        self.path = None

    def _meta_path(self):
        return os.path.join(self.path, "db_meta.json")
    
    # Binary meta data
    # def open_v2(self, path):
    #     self.path = path
    #     # check if path aleardy existed
    #     if (not os.path.exists(path)):
    #         os.makedirs(path)

    # def close_v2(self):
    #     for t_name, table in self.tables.items():
    #         table_path = os.path.join(self.path, t_name)
    #         if (not os.path.exists(table_path)):
    #             os.makedirs(table_path)
            
    #         # call close func. for Table class
    #         table.close()

    def open(self, path):
        """
        Open or create a database at `path`.
        Load db metadata and each table file.
        """
        self.path = path
        if (not os.path.exists(path)):
            os.makedirs(self.path, exist_ok=True)

        meta_file = self._meta_path()
        if not os.path.exists(meta_file):
            # first time: nothing to load
            self.tables = {}
            return

    def close(self):
        """
        Save all tables + metadata.
        """
        if not self.path:
            return

        # 1) save each table as a separate file
        for name, table in self.tables.items():
            table_path = os.path.join(self.path, f"{name}")
            if (not os.path.exists(table_path)):
                os.makedirs(table_path)
            table.close()
        
        # 2) save meta data to json
        meta = {"tables": {}}
        for name, table in self.tables.items():
            meta["tables"][name] = {
                "num_columns": table.num_columns,
                "key_index": table.key,
                "num_base_records": table.page_directory.num_base_records,
                "num_tail_records": table.page_directory.num_tail_records
            }

        with open(self._meta_path(), "w") as f:
            json.dump(meta, f, indent=2)


    # def open(self, path):
    #     """
    #     Open or create a database at `path`.
    #     Load db metadata and each table file.
    #     """
    #     self.path = path
    #     os.makedirs(self.path, exist_ok=True)

    #     meta_file = self._meta_path()
    #     if not os.path.exists(meta_file):
    #         # first time: nothing to load
    #         self.tables = {}
    #         return

    #     # load metadata
    #     with open(meta_file, "r") as f:
    #         meta = json.load(f)

    #     self.tables = {}
    #     for table_name, info in meta.get("tables", {}).items():
    #         num_columns = info["num_columns"]
    #         key_index = info["key_index"]

    #         table_file = os.path.join(self.path, f"{table_name}.tbl")
    #         if os.path.exists(table_file):
    #             # let table load its internal pages
    #             table = Table.load(table_file)
    #         else:
    #             # fallback: create empty table with same schema
    #             table = Table(table_name, num_columns, key_index)

    #         self.tables[table_name] = table

    # def close(self):
    #     """
    #     Save all tables + metadata.
    #     """
    #     if not self.path:
    #         return

    #     # 1) save each table as a separate file
    #     for name, table in self.tables.items():
    #         table_file = os.path.join(self.path, f"{name}.tbl")
    #         table.save(table_file)

    #     # 2) write db_meta.json
    #     meta = {"tables": {}}
    #     for name, table in self.tables.items():
    #         meta["tables"][name] = {
    #             "num_columns": table.num_columns,
    #             "key_index": table.key
    #         }

    #     with open(self._meta_path(), "w") as f:
    #         json.dump(meta, f, indent=2)

    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            raise ValueError(f"Table {name} is already existed.")

        # table = Table(name, num_columns, key_index)
        table = Table(name, self.path, num_columns, key_index)
        self.tables[name] = table

        # persist right away
        if self.path:
            self.close()

        return table

    def drop_table(self, name):
        if name not in self.tables:
            raise ValueError(f"No table named: {name} found!")
        del self.tables[name]

        # delete the table file too
        if self.path:
            table_file = os.path.join(self.path, f"{name}.tbl")
            if os.path.exists(table_file):
                os.remove(table_file)
            self.close()

    def get_table(self, name):
        table_path = os.path.join(self.path, name)
        
        if (name not in self.tables) and (not os.path.exists(table_path)):
            raise ValueError(f"No table named: {name} found!")
        else:
            # # binary meta data
            # meta_data_path = os.path.join(table_path, "meta_data")
            # with open(meta_data_path, "rb") as fp:
            #     num_columns = struct.unpack('<i', fp.read(4))[0]
            #     key = struct.unpack('<i', fp.read(4))[0]
            #     num_base_records = struct.unpack('<i', fp.read(4))[0]
            #     num_tail_records = struct.unpack('<i', fp.read(4))[0]
            
            # json meta data
            with open(self._meta_path(), "r") as f:
                meta_data = json.load(f)

            for table_name, info in meta_data.get("tables", {}).items():
                if table_name == name:
                    num_columns = info["num_columns"]
                    key = info["key_index"]
                    num_base_records = info["num_base_records"]
                    num_tail_records = info["num_tail_records"]
            
                    table = Table(name, self.path, num_columns, key, num_base_records, num_tail_records)
                    self.tables[name] = table

                    return table
            
            raise ValueError(f"No table named: {name} found!")

