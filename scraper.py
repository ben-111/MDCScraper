import json
import logging
import os
from queue import Queue
from threading import Thread
from urllib.error import HTTPError
from urllib.request import urlopen

from bs4 import BeautifulSoup

BASE_URL = 'https://www.microsoft.com/en-us/download/details.aspx?id={}'
RESULTS_FILE = 'results.json'

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
    result = {
        'status': 0,
        'title': '',
        'download_header': ''
    }
    try:
        with urlopen(BASE_URL.format(_id)) as response:
            result['status'] = response.status
            if response.status == 200:
                soup = BeautifulSoup(response, 'html.parser')
                result['title'] = soup.title.text
                result['download_header'] = find_download_header(soup)
    except HTTPError as e:
        result['status'] = e.code
        logger.debug(f'ID: {_id}, http error: {e.code}')
    except Exception as e:
        logger.debug(f'ID: {_id}, error: {e.__class__.__name__}: {e.msg}')
    finally:
        logger.debug(f'Result for ID {_id}, {result}')
        return result


def get_results():
    results = {}
    if not os.path.exists(RESULTS_FILE):
        save_results(results)
        return results

    with open(RESULTS_FILE, 'r') as rf:
        results = json.load(rf)

    return results


def save_results(results):
    with open(RESULTS_FILE, 'w') as rf:
        json.dump(results, rf)


def spider(ids: Queue, results: Queue):
    while True:
        _id = ids.get()
        result = scrape(_id)
        results.put((_id, result))


def main():
    results = get_results()
    current_id = 1
    if len(results.keys()) > 0:
        current_id = max(int(i) for i in results.keys()) + 1
    
    id_queue = Queue()
    result_queue = Queue()
    threads = [Thread(target=spider, args=(id_queue, result_queue), daemon=True)
               for _ in range(THREAD_POOL_SIZE)]
    
    logger.info(f'Starting scraping from id {current_id}')
    for t in threads:
        t.start()
    
    try:
        while True:
            # Populate id_queue
            if id_queue.qsize() < 5000:
                for _id in range(current_id, current_id+10000):
                    id_queue.put(_id)
                current_id = current_id + 10000

            # Add to results
            if result_queue.qsize() > 50:
                for _ in range(50):
                    _id, result = result_queue.get()
                    results[_id] = result
                logger.info(f'Scraped up to ID: {_id}')
                save_results(results)
    finally:
        save_results(results)


if __name__ == '__main__':
    main()
