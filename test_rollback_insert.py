from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.config import Config

def test_rollback_insert():
    print("\n[TEST] Starting Rollback Insert Test...")
    
    db = Database()
    db.open('./ECS165') 
    
    grades_table = db.create_table('Grades', 5, 0)
    query = Query(grades_table)
    
    key = 906659671
    records = [93, 0, 0, 0] 
    
    t = Transaction()
    
    print(f"1. Attempting to insert record with Key: {key}")
    
    query.insert(key, *records, transaction=t)
    
    record_before = query.select(key, 0, [1, 1, 1, 1, 1])
    if record_before and len(record_before) > 0:
        print("   -> Check: Record exists before abort. (Correct)")
    else:
        print("   -> ERROR: Record failed to insert initially.")
        return

    rids = grades_table.index.locate(0, key)
    print(rids)
    assert rids is not None, "Index should contain the key before abort"
    inserted_rid = rids[0][0] # QUESTION: correct?
    print(f"   -> Internal: Inserted RID is {inserted_rid}")

    print("2. Aborting Transaction...")
    t.abort()
    
    # check 1: record should not be found via select
    record_after = query.select(key, 0, [1, 1, 1, 1, 1])
    if not record_after or len(record_after) == 0:
        print("   -> Check: Record NOT found via Select. (Correct)")
    else:
        print(f"   -> FAILURE: Record still found via Select! Data: {record_after[0].columns}")

    # check 2: index should not contain the key
    index_result = grades_table.index.locate(0, key)
    if index_result is None or len(index_result) == 0:
        print("   -> Check: Index does not contain key. (Correct)")
    else:
        print(f"   -> FAILURE: Index still contains key! RIDs: {index_result}")

    # check 3: internal RID status (deep validation)
    # We directly read the Page Directory to see if the RID's location is marked as invalid
    page_idx = inserted_rid // Config.PAGE_CAPACITY
    rec_idx = inserted_rid % Config.PAGE_CAPACITY

    # Directly read the RID column of the Base Page
    try:
        base_record = grades_table.page_directory.read_base_record(page_idx, rec_idx)
        rid_val = base_record['rid']
        if rid_val == -1:
             print(f"   -> Check: Internal RID marked as -1 (Invalid). (Correct)")
        else:
             print(f"   -> FAILURE: Internal RID is still {rid_val} (Expected -1)")
             
        # check indirection reset
        if base_record['indirection'] == 0:
            print(f"   -> Check: Indirection reset to 0. (Correct)")
        else:
            print(f"   -> WARNING: Indirection is {base_record['indirection']}, expected 0.")
            
    except Exception as e:
        print(f"   -> Internal check skipped or failed: {e}")

    print("[TEST] Rollback Insert Test Completed.\n")

if __name__ == "__main__":
    test_rollback_insert()