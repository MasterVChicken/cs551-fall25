from collections import OrderedDict
import threading

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.base_cache = OrderedDict()
        self.tail_cache = OrderedDict()
        self.lock = threading.Lock()
    
    def get(self, key, page_type):
        with self.lock:
            cache = self.base_cache if page_type == "Base" else self.tail_cache
            if key not in cache:
                return None
            # Move key to end (most recently used)
            cache.move_to_end(key)
            # print(cache[key], page_type)
            return cache[key]

    def put(self, key, page, page_type):
        with self.lock:
            # print(page, page_type)
            cache = self.base_cache if page_type == "Base" else self.tail_cache
            if key in cache:
                # Update and move to end
                cache.move_to_end(key)
            cache[key] = page

            # Evict least recently used
            total_num_pages = len(self.base_cache) + len(self.tail_cache)
            if total_num_pages > self.capacity:
                return cache.popitem(last=False)
            return None
    
    def set(self, key, page, column_index, page_type):
        cache = self.base_cache if page_type == "Base" else self.tail_cache
        if key in cache:
            cache.move_to_end(key)
            cache[key].physical_pages[column_index] = page
            return
        return False
