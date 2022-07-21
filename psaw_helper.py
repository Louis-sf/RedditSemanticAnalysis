import json
from datetime import datetime

import requests


def get_pushshift_data(after, before, sub):
    result = []
    start = after
    while True:
        #Build URL
        url = 'https://api.pushshift.io/reddit/search/submission/?sort = desc'+'&limit=1000&after='+str(start)+'&before='+str(before)+'&subreddit='+str(sub)
        r = requests.get(url)
        #Load JSON data from webpage into data variable
        initial_data = json.loads(r.text)
        # if initial_data.length['data'] == 0:
        #     break
        init_len = len(result)
        result += initial_data['data']
        updated_len = len(result)
        if updated_len == init_len:
            break
        start = (result[len(result) - 1])['created_utc']
    return result


print(len(get_pushshift_data(1626825952,1626912352,"nba")))
