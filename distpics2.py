import multiprocessing as mp
import os
from pathlib import Path
import distpics as dp

total_duplicates = 0

def parallel_compare_done(duplicates):
    global total_duplicates
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
        for r in results:
            r.get()
        pool.close()
        pool.join()
    print('%s duplicate(s) found' % total_duplicates)
 
if __name__ == '__main__':
    main()
