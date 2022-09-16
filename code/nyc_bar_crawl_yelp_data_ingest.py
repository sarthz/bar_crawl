#!/usr/bin/env python
# coding: utf-8

# In[23]:


from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import urllib


# In[24]:


try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode


# In[1]:


import pandas as pd
import time


# In[2]:


df = pd.read_csv('./../data/nyc_bar_crawl_sample.csv')


# In[26]:


df.head()


# In[31]:


df['bar_id'][2]


# In[11]:


# df[df.borough=='Manhattan'].head(25)
# df[df['borough']=='Manhattan'].head(25)


# In[17]:


# Yelp Fusion API

API_KEY= '4nRS21VgE79PQdVbjIz8wYPndzvh94hFffLad8WesROrkUBAqMftFyEUrcHlD1jLLBb6aggJD6NAEZcFOLjTn2hi3DGTM-jpiSisM8Aq4REo92Jwk6znbI-qKvAhY3Yx' 

# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.


# Defaults for our simple example.
DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'San Francisco, CA'
SEARCH_LIMIT = 3


# In[18]:



def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.
    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.
    Returns:
        dict: The JSON response from the request.
    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search(api_key, term, location):
    """Query the Search API by a search term and location.
    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.
    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'limit': SEARCH_LIMIT
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def get_business(api_key, business_id):
    """Query the Business API by a business ID.
    Args:
        business_id (str): The ID of the business to query.
    Returns:
        dict: The JSON response from the request.
    """
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)


def query_api(term, location):
    """Queries the API by the input values from the user.
    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    response = search(API_KEY, term, location)

    businesses = response.get('businesses')

    if not businesses:
        print(u'No businesses for {0} in {1} found.'.format(term, location))
        return

    business_id = businesses[0]['id']

#     print(u'{0} businesses found, querying business info ' \
#         'for the top result "{1}" ...'.format(
#             len(businesses), business_id))
    response = get_business(API_KEY, business_id)

    print(u'Result for business "{0}" found:'.format(business_id))
#     print(response)
#     pprint.pprint(response, indent=2)
    
    print(type(response))
    
    return response



# In[25]:


def main():
    parser = argparse.ArgumentParser()

#     parser.add_argument('-q', '--term', dest='term', default=DEFAULT_TERM,
#                         type=str, help='Search term (default: %(default)s)')
#     parser.add_argument('-l', '--location', dest='location',
#                         default=DEFAULT_LOCATION, type=str,
#                         help='Search location (default: %(default)s)')
#     parser.add_argument('-lo', '--longitude', dest='longitude',
#                         type=str, help='Search longitude')
#     parser.add_argument('-la', '--latitude', dest='latitude',
#                         type=str, help='Search latitude')

#     input_values = parser.parse_args()

    location = 'New York'

    for i in range(len(df)):
        print(df['bar_pub_name'][i])
        term = df['bar_pub_name'][i]
    
        try:
            response = query_api(term, location)
    #         query_api(input_values.term, input_values.location)
        except HTTPError as error:
            sys.exit(
                'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                    error.code,
                    error.url,
                    error.read(),
                )
            )
            
        for res in response:
#             pprint.pprint(response)
            if res == 'hours':
#                     print(response['hours'])
                    
                    
                    hours = response['hours']
                    print("hours", hours)
                    for h in hours:
                        i = 0
                        for io in h:
                            if io == 'is_open_now':
                                print(h[0])
                            i = i+1
                                
        
#         bdf = pd.json_normalize(response.json()['name'])
#         print(bdf.head())

        
#         parsed = json.loads(response.text)
 
#         hours = parsed["hours"]
    
#         print(hours)

#         for review in reviews:
#             print("User:", review["user"]["name"], "Rating:", review["rating"], "Review:", review["text"], "\n")

        
#     print(df.head(1))
    


# In[21]:


if __name__ == '__main__':
    main()


# In[ ]:




