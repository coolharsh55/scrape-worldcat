import re


class Book(object):

    title_with_comments = re.compile(r'(.+)\[.+\]')
    author_with_comments = re.compile(r'(.+)\[.+\]')

    def __init__(self):
        self.id = 0
        self.accession = 0
        self.received = None  # datetime
        self._author = None
        self._title = None
        self.publisher = None
        self.publication = None  # datetime
        self.volumes = None
        self.size = None
        self.lending = None
        self.ref = None
        self.lang = None
        self.withdrawn = None  # datetime
        self.subjects = []

    @property
    def author(self):
        return self._author

    @author.setter
    def author(self, author):
        if author is None:
            raise Exception('author cannot be None')
        author = author.strip()
        if not author:
            self._author = None
            return
        if Book.author_with_comments.match(author):
            self._author = Book.author_with_comments\
                            .search(author).groups()[0].strip()
        else:
            self._author = author
        # self._author = re.sub('\s*\(.+\)', '', self._author)
        # self._author = self._author.split(',')[0]
        if self._author == "Anon.":
            self._author = None

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, title):
        if title is None:
            raise Exception('title cannot be None')
        title = title.strip()
        if not title:
            self._title = None
            return
        if Book.title_with_comments.match(title):
            self._title = Book.title_with_comments\
                            .search(title).groups()[0].strip()
        else:
            self._title = title
        self._title = re.sub('\s*[\[\{\(]{1}.+[\]\}\)]?', '', self._title)
        return

    def __str__(self):
        return f'{self.title} -- {self.author}'
