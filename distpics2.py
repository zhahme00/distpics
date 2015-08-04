from sys import argv
import multiprocessing as mp
import os
from pathlib import Path
import distpics as dp

total_duplicates = 0
remove_duplicates = True if len(argv) == 2 and argv[1] == '-remove' else False

def parallel_compare_done(duplicates):
    global total_duplicates
    if remove_duplicates:
        # todo: catch exception(s) while deleting
        for d in duplicates:
            print(d)
            os.remove(str(d))
            total_duplicates += 1
    else:
        total_duplicates += len(duplicates)
        for d in duplicates:
            print(d)
    
def parallel_compare(files):
    duplicates = []
    for equals in dp.group_equals(files):
        original = min(equals, key = lambda f: f.stat().st_ctime)
        for dup in filter(lambda f: f != original, equals):
            duplicates.append(dup)
    return duplicates
    
def main():
    folder = Path(os.getcwd())
    with mp.Pool(processes = mp.cpu_count() * 2) as pool:
        results = [pool.apply_async(parallel_compare, (files, ), callback = parallel_compare_done)
                   for files in dp.group_by_size(folder).values() if len(files) > 1]
        # todo: use global signal instead of waiting on multiple async-results
        for r in results:
            r.get()
        pool.close()
        pool.join()
        
    msg = 'deleted' if remove_duplicates else 'found'
    print('%s duplicate(s) %s' % (total_duplicates, msg))
 
if __name__ == '__main__':
    main()
