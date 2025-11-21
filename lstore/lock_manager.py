import threading
from enum import Enum

class LockType(Enum):
    SHARED = 1
    EXCLUSIVE = 2
    
class LockManager:
    # NO-WAIT policy for deadlock prevention
    # Strong Strict 2PL protocol
    def __init__(self):
        self.locks = {}  # {lock_id: {'type': LockType, 'holders': set()}}
        self.lock = threading.Lock()
        
    def acquire_lock(self, lock_id, lock_type, transaction_id):
        # Acquire a lock with NO-WAIT policy
        # return True if lock acquired successfully
        # return False if lock cannot be acquired
        with self.lock:
            # if no existing lock on this sources
            if lock_id not in self.locks:
                self.locks[lock_id] = {
                    'type': lock_type,
                    'holders': {transaction_id}
                }
                return True
            
            existing = self.locks[lock_id]
            already_holds = transaction_id in existing['holders']
            
            # Requesting a SHARED lock
            if lock_type == LockType.SHARED:
                if existing['type'] == LockType.SHARED:
                    existing['holders'].add(transaction_id)
                    return True
                elif existing['type'] == LockType.EXCLUSIVE:
                    if already_holds:
                        # EXCLUSIVE lock has already been assigned to current resource
                        return True
                    else:
                        # EXCLUSIVE lock has already been assigned to other resource
                        return False
            
            # Requesting an EXCLUSIVE lock
            else:
                if already_holds:
                    # EXCLUSIVE lock has already been assigned to current resource
                    if existing['type'] == LockType.EXCLUSIVE:
                        return True
                    else:
                        # TODO: Not sure if we allow lock upgrade in Strong Strict 2PL
                        return False
                else:
                    # EXCLUSIVE lock has already been assigned to other resource
                    return False
    
    def release_lock(self, lock_id, transaction_id):
        # Release a specific lock
        # In Strong Strict 2PL, it will only be called at commit/abot
        with self.lock:
            if lock_id in self.locks:
                self.locks[lock_id]['holders'].discard(transaction_id)
                if len(self.locks[lock_id]['holders']) == 0:
                    del self.locks[lock_id]
        
    def release_all_locks(self, transaction_id):
        # Same as release lock except for the number is all
        with self.lock:
            lock_ids_to_remove = []
            for lock_id, lock_info in self.locks.items():
                lock_info['holders'].discard(transaction_id)
                if len(lock_info['holders']) == 0:
                    lock_ids_to_remove.append(lock_id)
            
            for lock_id in lock_ids_to_remove:
                del self.locks[lock_id]