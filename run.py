import asyncio
from concurrent.futures import ProcessPoolExecutor
import csv
from itertools import zip_longest
from multiprocessing import Manager
from collections import namedtuple
import re

from data_import import load_books
from scrape import task


async def main(books_data, authorset, lock):
    books = tuple(filter(None.__ne__, books_data))
    authordata_local = []
    for book in books:
        if not book:
            continue
        authordata = await task(book.title, book.author)
        if authordata:
            authordata_local.append(authordata)


    with lock:
        # with open('./output.csv', 'a', newline='') as fd:
        #     writer = csv.writer(fd, delimiter='\t')
        #     for authordata in authordata_local:
        #         pass
        #         # if not book.subjects:
        #         #     book.subjects = "None"
        #         # else:
        #         #     book.subjects = ",".join(book.subjects)
        #         # if not book.author:
        #         #     book.author = "None"
        #         # writer.writerow((
        #         #     book.id, book.title, book.author, book.subjects))

        authorset.extend(authordata_local)


def tasks(books, subject_set, lock):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(books, subject_set, lock))


if __name__ == '__main__':
    def grouper(iterable, n, fillvalue=None):
        args = [iter(iterable)] * n
        return zip_longest(*args, fillvalue=fillvalue)

    books = load_books()
    # with open('./output.csv', 'w') as fd:
    #     fd.truncate(0)

    manager = Manager()
    lock = manager.Lock()
    authorset = manager.list()
    with ProcessPoolExecutor() as executor:
        for chunk in grouper(books, 1):
            executor.submit(tasks, chunk, authorset, lock)

    # sort and combine data
    data = {}
    Author = namedtuple('Author', 'name subjects')
    for item in authorset:
        if item is None or len(item) != 2:
            continue
        name, subjects = item
        name = re.sub(" [\d-]{5,9}", '', name)
        name = name.strip()
        subjects = [s.strip() for s in subjects]
        if data.get(name):
            data[name].update(subjects)
        else:
            data[name] = set(subjects)

    
    with open('./output.csv', 'w', newline='') as fd:
        writer = csv.writer(fd, delimiter='\t')
        data = sorted(data.items(), key=lambda x: x[0])
        for name, subjects in data:
            for subject in subjects:
                writer.writerow((name, subject))