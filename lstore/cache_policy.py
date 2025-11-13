from collections import OrderedDict

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        # self.cache = OrderedDict()
        self.base_cache = OrderedDict()
        self.tail_cache = OrderedDict()
    
    # def get(self, key):
    #     if key not in self.cache:
    #         return None
    #     # Move key to end (most recently used)
    #     self.cache.move_to_end(key)
    #     return self.cache[key]

    # def put(self, key, value):
    #     if key in self.cache:
    #         # Update and move to end
    #         self.cache.move_to_end(key)
    #     self.cache[key] = value

    #     # Evict least recently used
    #     if len(self.cache) > self.capacity:
    #         return self.cache.popitem(last=False)
    #     return None

    def get(self, key, page_type):
        cache = self.base_cache if page_type == "Base" else self.tail_cache
        if key not in cache:
            return None
        # Move key to end (most recently used)
        cache.move_to_end(key)
        return cache[key]

    def put(self, key, value, page_type):
        cache = self.base_cache if page_type == "Base" else self.tail_cache
        if key in cache:
            # Update and move to end
            cache.move_to_end(key)
        cache[key] = value

        # Evict least recently used
        total_num_pages = len(self.base_cache) + len(self.tail_cache)
        if total_num_pages > self.capacity:
            return cache.popitem(last=False)
        return None
