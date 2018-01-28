from aiohttp import ClientSession
from bs4 import BeautifulSoup
import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.ERROR)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s %(lineno)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


_BASE_URL = 'https://www.worldcat.org'


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
        if text.lower() == title.lower():
            for link in div.parent.find_all("a"):
                url = link.get('href', None)
                if not url:
                    continue
                if 'editions?editions' in url:
                    logger.debug(f'found editions for {title} - {author}')
                    return url
    logger.debug(f'notfound editions for {title} - {author}')


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
    logger.debug(f'waiting to fetch search page for {title} - {author}')
    bookurl = await search(title, author)
    logger.debug(f'fetched search page for {title} - {author}')
    if not bookurl:
        if not author:
            logger.warning(f'FAILED - {title} - {author}')
            return []
        logger.debug(f'trying without author: {title} - {author}')
        bookurl = await search(title, None)
        if not bookurl:
            logger.warning(f'FAILED without author- {title} - {author}')
            return []
        logger.debug(f'found search page without author- {title} - {author}')
    logger.debug(f'getting editions for {title} - {author}')
    editions = await get_editions(bookurl)
    logger.debug(f'got editions for {title} - {author}')
    for edition in editions:
        logger.debug(f'extracting subjects for {title} - {author} - {edition}')
        subjects = await extract_subjects(edition)
        logger.debug(f'got {len(subjects)}subjects for {title} - {edition}')
        if subjects:
            logger.info(f'SUCCESS: {title} - {author}')
            return subjects
