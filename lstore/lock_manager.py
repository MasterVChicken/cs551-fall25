import threading
from enum import Enum

class LockType(Enum):
    SHARED = 1
    EXCLUSIVE = 2
    
class LockManager:
    def __init__(self):
        self.locks = {}
        # self.lock is a lock for protecting locks dict {}
        self.lock = threading.Lock()
        
    # Try to acquire a lock with NO-WAIT policy
    # return True for successful and False if not
    def acquire_lock(self, lock_id, lock_type, transaction_id):
        with self.lock:
            if lock_id not in self.locks:
                self.locks[lock_id] = {
                    'type': lock_type,
                    'holders': {transaction_id}
                }
                return True
            
            existing = self.locks[lock_id]
            already_holds = transaction_id in existing['holders']
            
            if lock_type == LockType.SHARED:
                if existing['type'] == LockType.SHARED:
                    # Multiple shared locks allowed
                    existing['holders'].add(transaction_id)
                    return True
                elif existing['type'] == LockType.EXCLUSIVE:
                    if already_holds:
                        # Already holds exclusive
                        return True
                    else:
                        # NO-WAIT: Cannot acquire, return False immediately
                        return False
                        
            else:  # Requesting EXCLUSIVE
                if len(existing['holders']) == 1 and already_holds:
                    # Lock upgrade
                    existing['type'] = LockType.EXCLUSIVE
                    return True
                else:
                    # NO-WAIT: Cannot acquire, return False immediately
                    return False
    
    def release_lock(self, lock_id, transaction_id):
        with self.lock:
            if lock_id in self.locks:
                self.locks[lock_id]['holders'].discard(transaction_id)
                if len(self.locks[lock_id]['holders']) == 0:
                    del self.locks[lock_id]
        
    # Release all locks for the transaction
    def release_all_locks(self, transaction_id):
        with self.lock:
            lock_ids_to_remove = []
            for lock_id, lock_info in self.locks.items():
                lock_info['holders'].discard(transaction_id)
                if len(lock_info['holders']) == 0:
                    lock_ids_to_remove.append(lock_id) 
            
            for lock_id in lock_ids_to_remove:
                del self.locks[lock_id]