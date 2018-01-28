import asyncio
from concurrent.futures import ProcessPoolExecutor
import csv
from itertools import zip_longest
from multiprocessing import Manager

from data_import import load_books
from scrape import task


async def main(books_data, lock):
    books = tuple(filter(None.__ne__, books_data))
    for book in books:
        if not book:
            continue
        book.subjects = await task(book.title, book.author)

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


def tasks(books, lock):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(books, lock))


if __name__ == '__main__':
    def grouper(iterable, n, fillvalue=None):
        args = [iter(iterable)] * n
        return zip_longest(*args, fillvalue=fillvalue)

    books = load_books(1000)
    with open('./output.csv', 'w') as fd:
        fd.truncate(0)

    manager = Manager()
    lock = manager.Lock()
    with ProcessPoolExecutor() as executor:
        for chunk in grouper(books, 50):
            executor.submit(tasks, chunk, lock)
