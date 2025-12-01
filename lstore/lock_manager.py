import threading
from enum import Enum
import unittest

class LockType(Enum):
    SHARED = 1
    EXCLUSIVE = 2
    
class LockManager:
    # NO-WAIT policy for deadlock prevention
    # Strong Strict 2PL protocol
    def __init__(self):
        # {lock_id: {'type': LockType, 'holders': set()}}
        self.locks = {}  
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
                        # Yanliang's Modification here: lock upgrading switch
                        # The following implementation allows lock upgrading
                        if len(existing['holders']) == 1:
                            # Only myself holds the SHARED lock, can upgrade
                            existing['type'] = LockType.EXCLUSIVE
                            return True
                        else:
                            # Other transactions also hold SHARED lock
                            # NO-WAIT policy, cannot wait for them to release
                            # So upgrade fails, may cause current transaction to Abort
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
                
                
class TestLockManager(unittest.TestCase):
    def setUp(self):
        self.lm = LockManager()

    def test_basic_shared_shared(self):
        """test1: multiple shared locks allowed"""
        print("\nRunning: test_basic_shared_shared")
        self.assertTrue(self.lm.acquire_lock('A', LockType.SHARED, 'T1'))
        self.assertTrue(self.lm.acquire_lock('A', LockType.SHARED, 'T2'))
        
        # verify both holders exist
        self.assertEqual(len(self.lm.locks['A']['holders']), 2)
        self.assertEqual(self.lm.locks['A']['type'], LockType.SHARED)

    def test_exclusive_conflict(self):
        """test2: exclusive locks should conflict"""
        print("\nRunning: test_exclusive_conflict")
        # T1 obtains EXCLUSIVE lock
        self.assertTrue(self.lm.acquire_lock('A', LockType.EXCLUSIVE, 'T1'))

        # T2 attempts to obtain EXCLUSIVE lock -> fails
        self.assertFalse(self.lm.acquire_lock('A', LockType.EXCLUSIVE, 'T2'))
        # T2 attempts to obtain SHARED lock -> fails
        self.assertFalse(self.lm.acquire_lock('A', LockType.SHARED, 'T2'))

    def test_lock_upgrade_success(self):
        """test3: lock upgrade should succeed when no other holders"""
        print("\nRunning: test_lock_upgrade_success")

        # T1 obtains SHARED lock
        self.assertTrue(self.lm.acquire_lock('A', LockType.SHARED, 'T1'))

        # T1 attempts to upgrade to EXCLUSIVE lock
        # Expected: succeeds, because T1 is the only holder
        success = self.lm.acquire_lock('A', LockType.EXCLUSIVE, 'T1')
        self.assertTrue(success)

        # Verify lock type has changed to EXCLUSIVE
        self.assertEqual(self.lm.locks['A']['type'], LockType.EXCLUSIVE)

        # Verify T2 cannot obtain lock
        self.assertFalse(self.lm.acquire_lock('A', LockType.SHARED, 'T2'))

    def test_lock_upgrade_fail_due_to_others(self):
        """test4: lock upgrade should fail when other holders exist"""
        print("\nRunning: test_lock_upgrade_fail_due_to_others")

        # 1. T1 obtains SHARED lock
        self.assertTrue(self.lm.acquire_lock('A', LockType.SHARED, 'T1'))
        # 2. T2 also obtains SHARED lock
        self.assertTrue(self.lm.acquire_lock('A', LockType.SHARED, 'T2'))

        # 3. T1 attempts to upgrade to EXCLUSIVE lock
        # Expected: fails (False). Because T2 is still reading, T1 cannot write, and NO-WAIT does not allow waiting
        success = self.lm.acquire_lock('A', LockType.EXCLUSIVE, 'T1')
        self.assertFalse(success)

        # 4. Verify lock type has not changed
        self.assertEqual(self.lm.locks['A']['type'], LockType.SHARED)

    def test_release_allows_upgrade(self):
        """test5: lock upgrade should succeed after other readers release"""
        print("\nRunning: test_release_allows_upgrade")
        
        self.lm.acquire_lock('A', LockType.SHARED, 'T1')
        self.lm.acquire_lock('A', LockType.SHARED, 'T2')

        # T1 attempts to upgrade to EXCLUSIVE lock
        self.assertFalse(self.lm.acquire_lock('A', LockType.EXCLUSIVE, 'T1'))

        # T2 releases lock
        self.lm.release_lock('A', 'T2')

        # T1 attempts to upgrade -> should succeed
        self.assertTrue(self.lm.acquire_lock('A', LockType.EXCLUSIVE, 'T1'))
        self.assertEqual(self.lm.locks['A']['type'], LockType.EXCLUSIVE)

if __name__ == '__main__':
    unittest.main()