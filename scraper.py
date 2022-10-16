import logging
import sqlite3
from queue import Queue
from threading import Event, Thread
from urllib.error import HTTPError
from urllib.request import urlopen

from bs4 import BeautifulSoup

BASE_URL = 'https://www.microsoft.com/en-us/download/details.aspx?id={}'
RESULTS_DB = 'results.db'
REQUEST_TIMEOUT = 2

THREAD_POOL_SIZE = 20

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter("[%(threadName)s]: %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.removeHandler(logger.handlers)
logger.addHandler(console_handler)


def find_download_header(soup: BeautifulSoup) -> str:
    div = soup.find('div', class_='download-header')
    return div.h2.text.strip()


def scrape(_id: int):
    status = 0
    title = ''
    download_header = ''

    try:
        with urlopen(BASE_URL.format(_id), timeout=REQUEST_TIMEOUT) as response:
            status = response.status
            if response.status == 200:
                soup = BeautifulSoup(response, 'html.parser')
                title = soup.title.text
                download_header = find_download_header(soup)
    except HTTPError as e:
        status = e.code
        logger.debug(f'ID: {_id}, http error: {e.code}')
    except Exception as e:
        logger.error(f'ID: {_id}, error: {e.__class__.__name__}: {e.msg}')
    finally:
        logger.debug(
            f'Result for ID {_id}, {status}, {title}, {download_header}')
        return status, title, download_header


def spider(ids: Queue, results: Queue):
    while True:
        _id = ids.get()
        result = scrape(_id)
        results.put((_id, *result))


def get_last_id(con: sqlite3.Connection):
    ensure_results_table(con)
    cur = con.cursor()
    res = cur.execute('SELECT id FROM results ORDER BY id DESC LIMIT 1')
    max_id = res.fetchone()

    if max_id is None:
        return 0

    return max_id[0]


def get_missing_ids(con: sqlite3.Connection):
    ensure_results_table(con)
    cur = con.cursor()
    missing_ids = []

    last_id = get_last_id(con)
    res = cur.execute('SELECT COUNT(id) FROM results')

    if last_id == res.fetchone()[0]:
        return missing_ids

    for i in range(1, last_id + 1):
        res = cur.execute('SELECT id FROM results WHERE id = ?', (i,))
        if res.fetchone() is None:
            missing_ids.append(i)

    return missing_ids


def ensure_results_table(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS results (
            id INT PRIMARY KEY,
            status INT,
            title TEXT,
            download_header TEXT
        );
    ''')


def archiver(results: Queue, stop: Event):
    con = sqlite3.connect(RESULTS_DB)
    ensure_results_table(con)
    cur = con.cursor()
    count = 0

    try:
        while not stop.is_set():
            result = results.get()
            count += 1
            if count % 50 == 0:
                logger.info(f'Archived 50 URLs, last id: {result[0]}')
            
            logger.debug(f'Inserting result with id {result[0]}')
            cur.execute('INSERT INTO results VALUES(?, ?, ?, ?)', result)
            con.commit()
    finally:
        con.close()


def main():
    con = sqlite3.connect(RESULTS_DB)
    current_id = get_last_id(con) + 1
    missing_ids = get_missing_ids(con)

    id_queue = Queue()
    for _id in missing_ids:
        id_queue.put(_id)

    result_queue = Queue()
    archiver_stop = Event()
    archiver_thread = Thread(target=archiver, args=(
        result_queue, archiver_stop), daemon=True)
    archiver_thread.start()

    threads = [Thread(target=spider, args=(id_queue, result_queue), daemon=True)
               for _ in range(THREAD_POOL_SIZE)]

    logger.info(f'Initializing {THREAD_POOL_SIZE} spiders')
    for t in threads:
        t.start()

    logger.info(f'Starting scraping from id {current_id}')
    try:
        while True:
            # Populate id_queue
            if id_queue.qsize() < 5000:
                for _id in range(current_id, current_id+10000):
                    id_queue.put(_id)
                current_id = current_id + 10000
    finally:
        logger.info('Shutting down')
        archiver_stop.set()
        archiver_thread.join()
        con.close()


if __name__ == '__main__':
    main()
