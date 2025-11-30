from lstore.table import Table, Record
from lstore.index import Index
import threading

# TransactionWorker representing a worker thread that process multiple transactions
class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        
        self.thread = None

    
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)

        
    """
    Runs all transaction as a thread
    """
    def run(self):
        # here you need to create a thread and call __run
        self.thread = threading.Thread(target=self.__run)
        self.thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        if self.thread:
            self.thread.join()


    def __run(self):
        for transaction in self.transactions:
            commited = False
            while not commited:
                commited = transaction.run()
            self.stats.append(True)
        self.result = len(self.status)

