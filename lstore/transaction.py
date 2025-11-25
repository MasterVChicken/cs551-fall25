from lstore.table import Table, Record
from lstore.index import Index
import threading
import traceback

class Transaction:
    # We follow the designs for Strict Strong 2PL lock
    
    # global transaction id and id_protector to generate unique id in multi-thread env
    transaction_id_counter = 0
    id_lock = threading.Lock()
    
    def __init__(self):
        self.queries = []
        
        with Transaction.id_lock:
            Transaction.transaction_id_counter += 1
            self.transaction_id = Transaction.transaction_id_counter
        
        # Operation log for possible rollback
        # Format: [(table, op_type, rollback_data), ...]
        self.operations_log = []
        
        # Track tables involved
        self.tables = set()
    
    """
    Adds the given query to this transaction.
    Example:
        q = Query(grades_table)
        t = Transaction()
        t.add_query(q.insert, grades_table, *[93, 0, 0, 0, 0])
        t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, table, args))
        self.tables.add(table)
    
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        # Run as Strong Strict 2PL policy
        # Growing Phase: Acquire locks as needed and no release during execution
        # Shrinking Phase: Only release locks after it has ended
        # Return True if commited, False is aborted
        try:
            for query, table, args in self.queries:
                # Query will try to acquire necessary locks and return corresponding results
                result = query(*args, transaction=self)
                
                # NO-WAIT Policy: Abort immediately on lock failure
                if result == False:
                    return self.abort()
            
            # All queries succeeded, commit
            return self.commit()
            
        except Exception as e:
            print(f"Transaction {self.transaction_id} exception: {e}")
            traceback.print_exc()
            return self.abort()
    
    def abort(self):
        #Abort transaction and rollback all changes.
        # Rollback in reverse order (LIFO)
        for table, op_type, rollback_data in reversed(self.operations_log):
            try:
                if op_type == 'insert':
                    rid = rollback_data['rid']
                    table.rollback_insert(rid)
                    
                elif op_type == 'update':
                    rid = rollback_data['rid']
                    old_indirection = rollback_data['old_indirection']
                    table.rollback_update(rid, old_indirection)
                    
                elif op_type == 'delete':
                    rid = rollback_data['rid']
                    table.rollback_delete(rid)
                    
            except Exception as e:
                print(f"Rollback error: {op_type} on rid {rollback_data.get('rid')}: {e}")
        
        # Strong Strict 2PL: Release ALL locks at abort
        for table in self.tables:
            table.lock_manager.release_all_locks(self.transaction_id)
        
        self.operations_log.clear()
        return False
    
    def commit(self):
        # Release all locks once sucess
        for table in self.tables:
            table.lock_manager.release_all_locks(self.transaction_id)
        
        self.operations_log.clear()
        return True
    
    def log_operation(self, table, op_type, rollback_data):
        # Log operation for potential rollback.
        self.operations_log.append((table, op_type, rollback_data))