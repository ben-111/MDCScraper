import json
import os
from bs4 import BeautifulSoup
from urllib.error import HTTPError
from urllib.request import urlopen
import logging

BASE_URL = 'https://www.microsoft.com/en-us/download/details.aspx?id={}'
RESULTS_FILE = 'results.json'

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
        logger.error(f'ID: {_id}, http error: {e.code}')
    except Exception as e:
        logger.error(f'ID: {_id}, error: {e.__class__.__name__}: {e.msg}')
    finally:
        logger.info(f'Result for ID {_id}, {result}')
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


def main():
    results = get_results()
    current_id = max(int(i) for i in results.keys()) if len(results.keys()) > 0 else 0
    try:
        while True:
            current_id += 1
            results[current_id] = scrape(current_id)
            if current_id % 50 == 0:
                save_results(results)
    finally:
        save_results(results)


if __name__ == '__main__':
    main()
