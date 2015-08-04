from filecmp import cmp
import os
from pathlib import Path
 
def group_by_size(folder: Path):
    sizes = {}
    for file in filter(lambda f: f.is_file(), folder.glob('**/*')):
        s = file.stat().st_size
        if s in sizes:
            sizes[s].append(file)
        else:
            sizes[s] = [file]
    return sizes
 
def group_equals(files):    
    # No side effects
    files = list(files)
    i = 0
    while i < len(files):
        duplicates = [files[i]]
        j = i + 1
        while j < len(files):
            if cmp(str(files[i]), str(files[j]), shallow = False):
                duplicates.append(files.pop(j))
            else:
                j += 1
        # The base file compared against should be 
        # returned too if duplicates were found.
        if len(duplicates) > 1:
            yield duplicates
        i += 1
 
def get_duplicates(folder: Path):
    for files in filter(lambda f: len(f) > 1, group_by_size(folder).values()):
        # only return the duplicate files from a 
        # group of files that have the same size.
        for equals in group_equals(files):
            original = min(equals, key = lambda f: f.stat().st_ctime)
            for dup in filter(lambda f: f != original, equals):
                yield dup
def main():
    for i, f in enumerate(get_duplicates(Path(os.getcwd())), 1):
        print(f)
    print('%s duplicate(s) found' % i)
 
if __name__ == '__main__':
    main()
