from aiohttp import ClientSession
from bs4 import BeautifulSoup
import logging
import re
import sys

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
        '%(levelname)s:%(filename)s:%(funcName)s:%(lineno)s %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


_BASE_URL = 'https://www.worldcat.org'

_AUTHORS = set()


async def fetch(url, params):
    async with ClientSession() as session:
        async with session.get(url, params=params) as resp:
            # logger.debug(f'Waiting to fetch {url}')
            data = await resp.text()
            # logger.debug(f'Fetched {len(data)}bytes - {url}')
            return data


async def search(title, author=None):
    params = {'q': f'ti:{title}{" au:"+author if author else ""}'}
    logger.debug(f'fetching search page for {title} - {author}')
    page = await fetch(_BASE_URL + '/search', params)
    logger.debug(f'fetched search page for {title} - {author}')
    soup = BeautifulSoup(page, 'html.parser')
    for div in soup.find_all("div", class_="name"):
        text = div.find_next("a").get_text()
        # if text.lower() == title.lower():
        if text.lower() in title.lower() or title.lower() in text.lower():
            for link in div.parent.find_all("a"):
                url = link.get('href', None)
                if not url:
                    continue
                # if 'editions?editions' in url:
                #     logger.debug(f'found editions for {title} - {author}')
                #     return url
                return url
    logger.debug(f'notfound editions for {title} - {author}')


async def get_authors(bookurl):
    page = await fetch(_BASE_URL + bookurl, params=None)
    soup = BeautifulSoup(page, 'html.parser')
    select = soup.find("select", {"id": "authorSearchSelect"})
    options = select.find_all("option")
    authorlinks = [option['value'] for option in options]
    logger.debug(f"found {len(authorlinks)} authors")
    # for author in authorlinks:
    #     logger.debug(f"found author link page {author}")
    return authorlinks


async def extract_author_subjects(authorlink):
    page = await fetch(_BASE_URL + authorlink, params=None)
    if not page:
        return
    soup = BeautifulSoup(page, 'html.parser')
    if not soup:
        return
    name = soup.find("h1")
    if not name:
        return
    name = name.get_text()

    tagcloud = soup.find("div", {"id": "identitiesFASTCloud"})
    if not tagcloud:
        print('no tagcloud')
        return
    tags = tagcloud.find_all("a")
    subjects = []
    for tag in tags:
        text = tag.get_text()
        if not text:
            continue
        subjects.append(text)

    logger.debug(f"author {name} has subjects {subjects}")
    print(name, subjects)
    return name, subjects


async def get_editions(url):
    logger.debug(f'waiting to fetch editions page {url}')
    page = await fetch(_BASE_URL + url, params=None)
    logger.debug(f'fetched editions page {url}')
    soup = BeautifulSoup(page, 'html.parser')
    links = tuple(
        div.find_next("a").get('href', None)
        for div in soup.find_all("div", class_="name"))
    links = filter(lambda x: x is not None, links)
    logger.debug(f'found links editions for {url}')
    return links


async def extract_subjects(url):
    logger.debug(f'waiting to fetch page for book {url}')
    page = await fetch(_BASE_URL + url, params=None)
    logger.debug(f'fetched book page {url}')
    soup = BeautifulSoup(page, 'html.parser')
    subjects = []
    for li in soup.find_all("li", class_="subject-term"):
        subjects.append(li.get_text().strip())
    logger.debug(f'found {len(subjects)} subjects for {url}')
    return subjects


async def task(title, author):
    # this part ensures that we have a link to the book page on worldcat
    logger.debug(f'waiting to fetch search page for {title} - {author}')
    bookurl = await search(title, author)
    logger.debug(f'fetched search page for {title} - {author}')
    if not bookurl:
        if not author:
            logger.warning(f'FAILED - {title} - {author}')
            return []
        author = re.sub('\s*\(.+\)', '', author)
        author = author.split(',')[0]
        logger.debug(f'trying with author last name: {title} - {author}')
        bookurl = await search(title, author)
        if not bookurl:
            logger.warning(f'FAILED with author lastname - {title} - {author}')
            logger.debug(f'trying without author: {title} - {author}')
            bookurl = await search(title, None)
            if not bookurl:
                logger.warning(f'FAILED without author- {title} - {author}')
                return []
            logger.debug(
                f'found search page without author- {title} - {author}')
        else:
            logger.debug(
                f'found search page with author lastname- {title} - {author}')

    # here we will get the link to the authors information from the books page
    logger.debug(f'getting authors for {bookurl}')
    authorlinks = await get_authors(bookurl)

    # get associated subjects from each author page
    for authorlink in authorlinks:
        authordata = await extract_author_subjects(authorlink)

    return authordata
