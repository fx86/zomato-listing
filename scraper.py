from boilerplate import *
from bs4 import BeautifulSoup as bs
import pandas as pd
from multiprocessing import Pool, Manager, Process

city = 'ncr'
LISTINGS_PAGE = 'https://www.zomato.com/{:s}/restaurants?page={:d}'
metadata, all_reviews = [], []


def scrape_restros(cached, response):
    '''
        scrape DOM and return a dataframe
    '''
    if response:
        soup = bs(response, 'html.parser')
        results = soup.findAll('article', {'class': 'search-result'})
        page_results = []
        for result in results:
            datum = {}
            datum['restaurant-name'] = result.find(
                'a', {'class': 'result-title'}).text.strip()
            if result.find('div',
                           {'class': 'res-snippet-small-establishment'}):
                datum['zomato-genre'] = result.find(
                    'div', {'class': 'res-snippet-small-establishment'}).text
            else:
                datum['zomato-genre'] = ''
            datum['direct-url'] = result.find(
                'a', {'class': 'result-title'}).attrs['href']
            datum['subzone'] = result.find(
                'a', {'class': 'search_result_subzone'}).text.strip()
            datum['address'] = result.find(
                'div', {'class': 'search-result-address'}).text.strip()
            datum['cuisines'] = result.find(
                'span', {'class': 'col-s-11 col-m-12 nowrap pl0'.split(' ')
                         }).text

            cost = result.select('div.res-cost > span:nth-of-type(2)')
            datum['meal-for-two'] = cost[0].text if cost else ''

            timing = result.find('div', {'class': 'res-timing'})
            datum['timing'] = timing.attrs['title'] if timing else ''

            rating = result.find('div', {'class': 'res-rating-nf'})
            datum['restaurant-rating'] = rating.text.strip() if rating else ''

            votes = result.select('span[class*="rating-votes-div"]')
            datum['restaurant-votes'] = votes[0].text if votes else ''
            page_results.append(datum)

        return pd.DataFrame(page_results)
    else:
        return False


def has_more_pages(response):
    '''
        checks if page has a pagination element
    '''
    soup = bs(response, 'html.parser')
    if not soup.find('i', {'class': ['disabled', 'item', 'next']}):
        return True
    else:
        print('no more pages')
        return False
    return


def get_all_restaurants(url):
    '''
        scrapes pages of restaurant listings
        and returns a dataframe of all listings.
    '''
    page_num = 1
    data = []

    more_pages = True
    while more_pages:
        page = url.format(city, page_num)
        cached, response = q(page)

        # return dataframe after scraping a page
        datum = scrape_restros(cached, response)
        if not datum.empty:
            data.append(datum)
        page_num += 1
        more_pages = has_more_pages(response)
        if page_num % 5 == 0:
            pd.concat(data).to_csv('temp.csv', index=False, encoding='utf-8')
            if not cached:
                sleep(randint(5, 30))
    # concat all dataframes to make a final dataframe
    return pd.concat(data)


def scrape_restaurant_details(url):
    status, response = q(url, reviews=True)
    if response:
        soup = bs(response, 'html.parser')

        result = {}

        phone = soup.find('span', {'class': 'res-tel'})
        result['phone-numbers'] = phone.text.strip() if phone else ''

        photo_count = soup.find('a', {'class': 'photosTab'})
        result['photo-count'] = photo_count.attrs['data-count'] if photo_count else ''

        known_for = soup.find('div', {'class': 'res-info-known-for-text'})
        result['known-for'] = known_for.text.strip() if known_for else ''

        what_people_love = soup.select('div.rv_highlights__section')
        what_people_love += [''] * (3 - len(what_people_love))
        food, service, ambience = what_people_love
        if food:
            result['best-dishes'] = ''.join([v.text.strip()
                                             for v in food.select('div.fontsize13 span')])
            result['food-rating'] = len(food.select('div[class*="level-"]'))
        if service:
            result['service'] = ''.join(
                [v.text.strip() for v in service.select('div.fontsize13 span')])
            result['service-rating'] = len(
                service.select('div[class*="level-"]'))
        if ambience:
            result['look-and-feel'] = ''.join([v.text.strip()
                                               for v in ambience.select('div.fontsize13 span')])
            result['ambience-rating'] = len(
                ambience.select('div[class*="level-"]'))

        review_data = []
        reviews = soup.select('div.res-review')
        for ind, r in enumerate(reviews):
            each_review = {}
            each_review['url'] = url
            reviewer = r.select('div.header.nowrap.ui.left a')
            if reviewer:
                reviewer = reviewer[0]
                each_review['reviewer-name'] = reviewer.text.strip()
                each_review['reviewer-profile'] = reviewer.attrs['href']

            ts = r.select('div.fs12px.pbot0.clearfix a time')
            each_review['timestamp'] = ts[0].attrs['datetime'] if ts else ''

            r_rating = r.select('div[class*="rev-text"] div.ttupper')
            each_review['rating'] = r_rating[0].attrs['aria-label'] if r_rating else ''

            each_review['content'] = r.select(
                'div[class*="rev-text"]')[0].text.strip()
            each_review['direct-url'] = url
            review_data.append(each_review)
        result['popular_review_count'] = len(review_data)
        result['direct-url'] = url
        return (result, review_data)


def append_data(datum):
    metadata.append(datum[0])
    all_reviews.append(datum[1])

if __name__ == '__main__':
    restaurants = get_all_restaurants(LISTINGS_PAGE)
    restaurants.to_csv(os.sep.join(
        [DATA_FLDR, city + '.csv']), index=False)
    df = pd.read_csv(os.sep.join([DATA_FLDR, city + '.csv']))

    pool=Pool()
    for url in df['direct-url'].unique().tolist():
        r = pool.apply_async(scrape_restaurant_details, 
                      (url,),
                      callback=append_data).get()

    meta1 = pd.read_csv("data/ncr.csv")
    meta2 = pd.concat([pd.DataFrame(d, index=[0]) for d in metadata]
              )
    metadata = meta1.merge(meta2, right_on='direct-url', left_on='direct-url')
    metadata = metadata.drop('Unnamed: 0', 1)
    metadata.to_csv(os.sep.join(
                                [DATA_FLDR, city + '_metadata.csv']),
        index=False, encoding='utf-8')
    pd.concat(pd.concat(pd.DataFrame(d, index=[0]) for d in review)
     for review in all_reviews if review
     ).to_csv(os.sep.join([DATA_FLDR, city + '_reviews.csv']))
