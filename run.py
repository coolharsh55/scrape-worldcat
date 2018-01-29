import asyncio
from concurrent.futures import ProcessPoolExecutor
import csv
from itertools import zip_longest
from multiprocessing import Manager

from data_import import load_books
from scrape import task


async def main(books_data, subject_set, lock):
    books = tuple(filter(None.__ne__, books_data))
    subject_set_local = []
    for book in books:
        if not book:
            continue
        book.subjects = await task(book.title, book.author)
        if book.subjects:
            subject_set_local.extend(book.subjects)


    with lock:
        with open('./output.csv', 'a', newline='') as fd:
            writer = csv.writer(fd, delimiter='\t')
            for book in books:
                if not book.subjects:
                    book.subjects = "None"
                else:
                    book.subjects = ",".join(book.subjects)
                if not book.author:
                    book.author = "None"
                writer.writerow((
                    book.id, book.title, book.author, book.subjects))

        subject_set.extend(subject_set_local)


def tasks(books, subject_set, lock):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(books, subject_set, lock))


if __name__ == '__main__':
    def grouper(iterable, n, fillvalue=None):
        args = [iter(iterable)] * n
        return zip_longest(*args, fillvalue=fillvalue)

    books = load_books()
    with open('./output.csv', 'w') as fd:
        fd.truncate(0)

    manager = Manager()
    lock = manager.Lock()
    subject_set = manager.list()
    with ProcessPoolExecutor() as executor:
        for chunk in grouper(books, 100):
            executor.submit(tasks, chunk, subject_set, lock)
    
    subjects = sorted(set(subject_set))
    with open('./subjects.txt', 'w') as fd:
        for subject in subjects:
            print(subject, file=fd)

