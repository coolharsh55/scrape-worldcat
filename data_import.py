import csv

from book import Book


def _load():
    with open('data.csv', 'r') as fd:
        reader = csv.reader(fd, delimiter='\t')
        next(reader)  # skip header
        for index, row in enumerate(reader):
            # row = next(reader)
            book = Book()
            book.id, book.author, book.title = index, row[2], row[3]
            if not book.title:
                continue
            yield book


def load_books(limit=None):
    books = _load()
    if limit and limit > 0:
        return [next(books) for i in range(limit)]
    return list(books)


if __name__ == '__main__':
    books = load_books()
    for b in books:
        print(b)
