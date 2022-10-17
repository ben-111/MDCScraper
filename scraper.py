import argparse
import logging
import sqlite3
import ssl
from queue import Queue
from threading import Event, Thread
from time import sleep
from urllib.error import HTTPError
from urllib.request import urlopen

from bs4 import BeautifulSoup

BASE_URL = 'https://www.microsoft.com/en-us/download/details.aspx?id={}'

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

logger = logging.getLogger(__name__)
log_formatter = logging.Formatter("[%(threadName)s]: %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.removeHandler(logger.handlers)
logger.addHandler(console_handler)


def find_download_header(soup: BeautifulSoup) -> str:
    div = soup.find('div', class_='download-header')
    return div.h2.text.strip()


def scrape(_id: int, timeout: int):
    status = 0
    title = ''
    download_header = ''

    try:
        with urlopen(BASE_URL.format(_id), timeout=timeout, context=ctx) as response:
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


def spider(options: argparse.Namespace, ids: Queue, results: Queue):
    while True:
        _id = ids.get()
        result = scrape(_id, options.timeout)
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
    logger.debug('Finding missing IDs from the DB')
    ensure_results_table(con)
    cur = con.cursor()
    missing_ids = []

    last_id = get_last_id(con)
    res = cur.execute('SELECT COUNT(id) FROM results')
    rows = res.fetchone()[0]
    missing_ids_count = last_id - rows

    if missing_ids_count == 0:
        return missing_ids

    for i in range(1, last_id + 1):
        res = cur.execute('SELECT id FROM results WHERE id = ?', (i,))
        if res.fetchone() is None:
            missing_ids.append(i)
            missing_ids_count -= 1
            if missing_ids_count == 0:
                break

    logger.info(
        f'Found {len(missing_ids)} missing IDs, starting with {min(missing_ids)}')
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


def archiver(options: argparse.Namespace, results: Queue, stop: Event):
    con = sqlite3.connect(options.output)
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


def main(options: argparse.Namespace):
    logger.info(f'Starting scraper with arguments: {options}')
    con = sqlite3.connect(options.output)
    current_id = get_last_id(con) + 1
    missing_ids = get_missing_ids(con)

    id_queue = Queue()
    for _id in missing_ids:
        id_queue.put(_id)

    result_queue = Queue()
    archiver_stop = Event()
    archiver_thread = Thread(target=archiver, args=(
        options, result_queue, archiver_stop), daemon=True)
    archiver_thread.start()

    threads = [Thread(target=spider, args=(options, id_queue, result_queue), daemon=True)
               for _ in range(options.workers)]

    logger.info(f'Initializing {options.workers} spiders')
    for t in threads:
        t.start()

    logger.info(f'Starting scraping from id {current_id}')
    try:
        while True:
            # Populate id_queue
            for _id in range(current_id, current_id + options.rate_limit):
                id_queue.put(_id)
            current_id = current_id + options.rate_limit
            sleep(60)
    except KeyboardInterrupt:
        pass
    finally:
        logger.info('Shutting down')
        archiver_stop.set()
        archiver_thread.join()
        con.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Microsoft Download Center Archiver',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-v', '--verbose',
                        help='Print more logs', action='store_true')
    parser.add_argument(
        '-o', '--output', help='Path of output SQLite3 DB', default='results.db')
    parser.add_argument('-t', '--timeout', type=int,
                        help='Timeout in seconds for each request', default=2)
    parser.add_argument('-w', '--workers', type=int,
                        help='Number of workers', default=5)
    parser.add_argument('--rate-limit', type=int,
                        help='Requests sent in one minute', default=500)
    options = parser.parse_args()

    if options.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    main(options)
