from glob import glob
from hashlib import md5
from fake_useragent import UserAgent as ua
from splinter import Browser
from splinter.exceptions import *
from selenium.common.exceptions import *
from random import randint
from time import sleep
import requests
import time
import os
import logging

user_agent = ua()
CACHE_FLDR = 'cache'
DATA_FLDR = 'data'
GEOCODE_URL = ''.join(['https://maps.googleapis.com/maps/api/geocode/json',
                       '?address={:s}&key={:s}'])
logging.basicConfig(format='%(asctime)s %(message)s', 
                    datefmt='%d/%m/%Y %I:%M:%S %p',
                    level=logging.INFO)


def enable_detailed_logging():
    # These two lines enable debugging at httplib level
    # (requests->urllib3->http.client)
    # You will see the REQUEST, including HEADERS and DATA,
    # and RESPONSE with HEADERS but without DATA.
    # The only thing missing will be the response.body which is not logged.
    # source - https://stackoverflow.com/a/16630836/170005
    import logging
    try:
        import http.client as http_client
    except ImportError:
        # Python 2
        import httplib as http_client
    http_client.HTTPConnection.debuglevel = 1

    # You must initialize logging, otherwise you'll not see debug output.
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


# def get_new_proxy():
#     resp = req.get('https://www.sslproxies.org/',
#     headers={'User-Agent': user_agent.random})
#     proxies_table = soup.find(id='proxylisttable')
#     proxies = []
#     for row in proxies_table.tbody.find_all('tr'):
#         proxies.append({
#           'ip':   row.find_all('td')[0].string,
#           'port': row.find_all('td')[1].string
#         })
#     proxies = pd.DataFrame(proxies)


def q(URL, reviews=False):
    '''
        request URL; cache and return response
    '''
    fname = md5(URL.encode('utf-8')).hexdigest() + '.html'
    cache_file_path = os.sep.join([CACHE_FLDR, fname])

    if cache_file_path not in glob(os.sep.join([CACHE_FLDR, '*html'])):
        browser = Browser('chrome', incognito=True,
                          user_agent=user_agent.random,
                          )
        logging.info("NOT CACHED: {:s}".format(URL))
        try:
            browser.visit(URL)
            while browser.find_by_css('div.load-more') and reviews:
                load_more = browser.find_by_css('div.load-more')
                try:
                    load_more.click()
                except Exception:
                    for tag in ['i.close',
                                'div.photoviewer_dimmer i.close_viewer']:
                        if browser.find_by_css(tag):
                            browser.find_by_css(tag).click()
                time.sleep(1)
                logging.info('sleeping on: {:s}'.format(URL))

        except (DriverNotFoundError, ElementDoesNotExist,
                ElementNotVisibleException) as error:
            logging.error('{:s} : {:s}'.format(URL, str(error)))
            time.sleep(1)
            q(URL)

        response = browser.html
        title = browser.title
        # logging.info('{:s} in {:s}'.format(browser.title, URL))
        browser.quit()

        if 'access denied' not in title.lower():
            # write cache file if did not exist previously
            with open(cache_file_path, 'w') as outfile:
                outfile.write(response)
            return False, ''
        return False, response
    else:
        # print("CACHED: ", URL)
        return True, open(cache_file_path, 'r')


def geocode(address, delay=5):
    '''
        returns google place tags and lat-lngs
        for valid addresses.
    '''
    url = GEOCODE_URL.format(address)
    try:
        jsonData = requests.get(GEOCODE_URL.format(address)).json()['results']
        if jsonData:
            lat = jsonData[0]['geometry']['location']['lat']
            lng = jsonData[0]['geometry']['location']['lng']
            google_place_tags = jsonData[0]['types']
            return {'lat': lat, 'lng': lng,
                    'google_place_tags': '-'.join(google_place_tags)}
        else:
            return [None, None, None]
    except Exception:
        logging.info('sleeping: {:s}'.format(address))
        time.sleep(15)
        geocode(address)
