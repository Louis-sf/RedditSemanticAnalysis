import json
from datetime import datetime

import requests


def get_pushshift_data(after, before, sub):
    #Build URL
    url = 'https://api.pushshift.io/reddit/search/submission/?sort = desc'+'&limit=1000&after='+str(after)+'&before='+str(before)+'&subreddit='+str(sub)
    #Print URL to show user
    #print(url)
    #Request URL
    r = requests.get(url)
    #Load JSON data from webpage into data variable
    data = json.loads(r.text)
    #return the data element which contains all the submissions data
    return data['data']

# count = 0
# subm = get_pushshift_data("1498931701", "", "gaming")
# for record in subm:
#     print(record['selftext'], datetime.fromtimestamp(record['created_utc']), "\n")
    #print(record.keys())
    # count+=1
    # print(count)
