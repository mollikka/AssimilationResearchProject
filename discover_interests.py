from pysocialwatcher import watcherAPI
from time import time, localtime
from lib import PersistentSet

watcher = watcherAPI()
watcher.load_credentials_file('credentials.txt')

autosave_every_N_seconds = 60

class PersistentSet:

    def __init__(self, filename, my_set = None):
        self._filename = filename
        self._my_set = set()
        try:
            with open(filename) as in_file:
                for line in in_file.readlines():
                    self._my_set.add(line.strip())
        except:
            if my_set is not None:
                self._my_set = my_set

    def __iter__(self):
        return self._my_set.__iter__()

    def __next__(self):
        return self._my_set.__next__()

    def add(self, item):
        self._my_set.add(item)

    def remove(self, item):
        self._my_set.remove(item)

    def add_from(self, other_set):
        self._my_set.update(other_set._my_set)

    def remove_from(self, other_set):
        self._my_set -= other_set._my_set

    def save(self):
        with open(self._filename, 'w') as out_file:
            for item in self._my_set:
                if item is not '': out_file.write(item + '\n')

    def clear(self):
        self._my_set.clear()

    def is_empty(self):
        return len(self._my_set) == 0

def explore():

    last_update = time()

    visited_set = PersistentSet('visited.txt')
    running_set = PersistentSet('running.txt',{'Science'})
    next_set = PersistentSet('next.txt')
    while True:
        try:
            for item in running_set:
                if time() > last_update + autosave_every_N_seconds:
                    last_update = time()
                    now = localtime()
                    print('Autosaving at {}.{}.{} {}.{}.{}'.format(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec))
                    visited_set.save()
                    running_set.save()
                    next_set.save()
                print(item)
                try:
                    results = watcher.get_search_targeting_from_query_dataframe(item)
                    visited_set.add(item)
                    for result in results['name']:
                        next_set.add(result)
                except KeyboardInterrupt:
                    raise
                except:
                    pass
            running_set.add_from(next_set)
            running_set.remove_from(visited_set)
            next_set.clear()
            if running_set.is_empty():
                visited_set.save()
                running_set.save()
                next_set.save()
                return

        except KeyboardInterrupt:
            visited_set.save()
            running_set.save()
            next_set.save()
            return

if __name__ == '__main__':
    #import pudb; pu.db
    explore()
