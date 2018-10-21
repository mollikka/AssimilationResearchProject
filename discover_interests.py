from pysocialwatcher import watcherAPI
from time import time, localtime
from lib import PersistentSet

watcher = watcherAPI()
watcher.load_credentials_file('credentials.txt')

autosave_every_N_seconds = 60

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
                        try:
                            next_set.add(unicode(result, 'utf-8'))
                        except TypeError:
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
