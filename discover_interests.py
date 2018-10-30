import os

from pysocialwatcher import watcherAPI
from time import time, localtime
from lib import PersistentSet
from string import ascii_lowercase

COLS = ('id','audience_size','name','path','type')

watcher = watcherAPI()
watcher.load_credentials_file('credentials.txt')

autosave_every_N_seconds = 60

def explore():

    def save():
        next_set.save()
        running_set.save()
        visited_set.save()
        saved_set.save()

    last_update = time()

    working_folder = 'CACHE_interest_discovery'

    visited_set = PersistentSet(working_folder+'/visited.txt')
    saved_set = PersistentSet(working_folder+'/saved.txt')
    running_set = PersistentSet(working_folder+'/running.txt',set(ascii_lowercase))
    next_set = PersistentSet(working_folder+'/next.txt')

    output_dataframe_file = working_folder+'/dataframe.csv'

    if not os.path.isdir(working_folder):
        os.makedirs(working_folder)

    if not os.path.isfile(output_dataframe_file):
        with open(output_dataframe_file,'w') as out_file:
            out_file.write(','.join(COLS)+'\n')

    while True:
        try:
            running_set.remove_from(visited_set)
            for item in running_set:
                if time() > last_update + autosave_every_N_seconds:
                    last_update = time()
                    now = localtime()
                    print('Autosaving at {}.{}.{} {}.{}.{}'.format(now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec))
                    save()
                print(item)
                try:
                    results = watcher.get_search_targeting_from_query_dataframe(item)
                    results = results[results.type == 'interests']
                    results[ ~results['name'].str.encode('utf-8').isin(saved_set)
                           ].to_csv(output_dataframe_file, mode='a', encoding='utf-8', columns = COLS, header=False)
                    for audience,name in zip(results['audience_size'],results['name'].str.encode('utf-8')):
                        print(str(audience) + ' ' + unicode(name,'utf-8'))
                        if audience > 100000:
                            next_set.add(unicode(name, 'utf-8'))
                        saved_set.add(unicode(name, 'utf-8'))
                    visited_set.add(item)
                except KeyboardInterrupt:
                    raise
                except Exception as error:
                    print("ERROR:",error)
                    continue
                print
            running_set.add_from(next_set)
            next_set.clear()
            if running_set.is_empty():
                print('Exploration is finished!')
                save()
                return

        except KeyboardInterrupt:
            print('Saving and exiting')
            save()
            return

if __name__ == '__main__':
    explore()
