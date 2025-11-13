import os
import shutil
import tempfile
from datetime import datetime

# Import your code
from lstore.config import Config
from lstore.page import BasePage, TailPage  # just to ensure imports exist
from lstore.index import Index              # ensure Index exists
from lstore.cache_policy import LRUCache
from lstore.db import Database              # if you have it; not used here
from lstore.table import Table              # <-- adjust if your Table is in a different module

def _mk_tmp_dir(prefix="merge_test_"):
    d = tempfile.mkdtemp(prefix=prefix)
    return d

def _cleanup(p):
    if os.path.exists(p):
        shutil.rmtree(p)

def test_merge_updates_and_tsp():
    """
    Scenario:
      - PAGE_CAPACITY = 2 to force multiple pages with few records.
      - Create a table with 3 user columns (C0, C1, C2). Key = column 0.
      - Insert 3 base records -> RIDs 0,1,2 (two base pages created when capacity=2).
      - Append 5 tail updates spread across those base records:
            * Update C0 and C2 for different base_rids with mixed schema masks.
            * Enough updates to push tail into at least two tail pages.
      - Call merge()
      - Verify base pages reflect the LAST update per base_rid & column,
        and base “tsp” (stored in BASE_RID_COLUMN per your code) is the latest tail rid.
    """
    tmp = _mk_tmp_dir()
    original_capacity = Config.PAGE_CAPACITY
    try:
        # Make paging small to trigger multiple pages
        Config.PAGE_CAPACITY = 2

        # Table: 3 columns, primary key at column 0
        name = "Grades"
        t = Table(name=name, dp_path=tmp, num_columns=3, key=0)

        # --- Insert 3 base records (RIDs: 0,1,2) ---
        # Columns are user columns (no metadata here)
        # Records hold values: [key, c1, c2]
        bases = [
            [100, 10, 1000],   # base rid 0
            [101, 11, 1001],   # base rid 1
            [102, 12, 1002],   # base rid 2
        ]

        # Use the Table's page_directory to insert base records (ensures metadata is set)
        for row in bases:
            rid = t.page_directory.num_base_records
            t.page_directory.insert_base_record(
                rid=rid,
                timestamp=int(datetime.now().timestamp()),
                columns=row
            )

        # Helper to append tail and return (tail_rid, page_index, record_index)
        def append_tail(base_rid, col_values_mask_tuple):
            """
            base_rid: the base RID this update belongs to
            col_values_mask_tuple: list of (col_idx, new_value) to write in this tail
            schema_encoding: bitmask (1 means column updated). bit i corresponds to column i.
            """
            # Build schema mask and columns payload
            schema_mask = 0
            cols_payload = [None] * t.num_columns
            for col_idx, new_val in col_values_mask_tuple:
                schema_mask |= (1 << col_idx)
                cols_payload[col_idx] = new_val

            tail_rid = t.page_directory.num_tail_records
            page_idx_before = tail_rid // Config.PAGE_CAPACITY
            # indirection for this tail record (chain): we’ll set to -1 for newest leaf
            indirection = -1
            tsp = int(datetime.now().timestamp())

            # Append tail record
            loc = t.page_directory.append_tail_record(
                rid=tail_rid,
                indirection=indirection,
                timestamp=tsp,
                schema_encoding=schema_mask,
                base_rid=base_rid,
                columns=cols_payload,
            )
            return tail_rid, loc

        # --- Create enough tail updates to span at least two tail pages ---
        # With PAGE_CAPACITY=2, 5 updates -> 3 pages (indexes 0,1,2)
        # Tail updates:
        #   - For base_rid=0: update C0 then later C2
        #   - For base_rid=1: update C2 then later C0
        #   - For base_rid=2: update C0 once
        tails = []
        tails.append(append_tail(base_rid=0, col_values_mask_tuple=[(0, 1000)]) )  # schema: 001
        tails.append(append_tail(base_rid=1, col_values_mask_tuple=[(2, 3001)]) )  # schema: 100
        tails.append(append_tail(base_rid=2, col_values_mask_tuple=[(0, 2002)]) )  # schema: 001
        tails.append(append_tail(base_rid=0, col_values_mask_tuple=[(2, 9000)]) )  # schema: 100 (later update C2 for rid0)
        tails.append(append_tail(base_rid=1, col_values_mask_tuple=[(0, 7001)]) )  # schema: 001 (later update C0 for rid1)

        # Optionally, set base indirection to point to latest tail (not required by your merge())
        # but if you want the base to know latest for other ops:
        # Update base indirection for rid 0 and 1 to their latest tail RID
        latest_tail_for_0 = tails[3][0]  # rid from the 4th append
        latest_tail_for_1 = tails[4][0]  # rid from the 5th append
        for base_rid, latest_tail in [(0, latest_tail_for_0), (1, latest_tail_for_1)]:
            b_page, b_slot = divmod(base_rid, Config.PAGE_CAPACITY)
            t.page_directory.update_base_indirection(b_page, b_slot, latest_tail)

        # Persist to disk before merge (exercise the I/O path)
        t.page_directory.save_to_disk()

        # --- Call merge() (it merges tail pages 0 and 1 per your code) ---
        t.merge()

        # Read back base rows and verify:
        #  - base_rid=0: C0 last set to 1000, C2 last set to 9000
        #  - base_rid=1: C2 last set to 3001, C0 last set to 7001
        #  - base_rid=2: C0 last set to 2002, others unchanged
        def read_base_row(base_rid):
            p, s = divmod(base_rid, Config.PAGE_CAPACITY)
            rec = t.page_directory.read_base_record(p, s)
            return rec  # dict with keys: indirection, rid, timestamp, schema_encoding, base_rid, columns

        r0 = read_base_row(0)
        r1 = read_base_row(1)
        r2 = read_base_row(2)

        # Columns format = [C0, C1, C2]
        assert r0["columns"][0] == 1000, f"Base rid 0 C0 expected 1000, got {r0['columns'][0]}"
        assert r0["columns"][2] == 9000, f"Base rid 0 C2 expected 9000, got {r0['columns'][2]}"

        assert r1["columns"][2] == 3001, f"Base rid 1 C2 expected 3001, got {r1['columns'][2]}"
        assert r1["columns"][0] == 7001, f"Base rid 1 C0 expected 7001, got {r1['columns'][0]}"

        assert r2["columns"][0] == 2002, f"Base rid 2 C0 expected 2002, got {r2['columns'][0]}"
        # unchanged columns
        assert r2["columns"][1] == 12, f"Base rid 2 C1 should remain 12, got {r2['columns'][1]}"
        assert r2["columns"][2] == 1002, f"Base rid 2 C2 should remain 1002, got {r2['columns'][2]}"

        # Verify tsp (stored in BASE_RID_COLUMN in your code) advanced to latest tail RID per base record
        # Read the value your merge writes via update_base_tsp(...)
        tsp0 = r0["base_rid"]
        tsp1 = r1["base_rid"]
        tsp2 = r2["base_rid"]
        assert tsp0 == latest_tail_for_0, f"TSP for base 0 should be latest tail {latest_tail_for_0}, got {tsp0}"
        assert tsp1 == latest_tail_for_1, f"TSP for base 1 should be latest tail {latest_tail_for_1}, got {tsp1}"
        # rid=2 had only one tail append; it should equal that tail rid
        only_tail_for_2 = tails[2][0]
        assert tsp2 == only_tail_for_2, f"TSP for base 2 should be {only_tail_for_2}, got {tsp2}"

        # Final persistence pass (exercise save)
        t.page_directory.save_to_disk()
        t.close()

        print("✅ test_merge_updates_and_tsp passed.")

    finally:
        # restore config and cleanup temp dir
        Config.PAGE_CAPACITY = original_capacity
        _cleanup(tmp)


if __name__ == "__main__":
    test_merge_updates_and_tsp()
    print("All merge tests completed.")
