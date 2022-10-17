# MDCScraper - The Microsoft Download Center Scraper
This tool will archive the Microsoft Download Center website by titles.

## Usage
You can simply run the scraper as `python ./scraper.py`, or use one of the 
switches to alter its behaviour. Note that the rate limit argument exists because
microsoft will temporarily block you if you issue too many requests in one time frame, 
so use caution with that one.

The tool outputs an SQLite3 database that contains the status for each download ID, and
if successful, the product name and title of the web page.
