import json
import requests
import requests.auth
import ccloud_lib
from confluent_kafka import Producer

client_key = "34p9yVEI4vWN7YgOLv_phA"
client_secret = "MmVidcg43Lab7ckKZK6kvt8IGe_7Dw"
auth = requests.auth.HTTPBasicAuth(client_key,client_secret)
post_data = {"grant_type": "password",
             "username": "reddit_user3699",
             "password": "Fe@5$yfcMGqRxP5"}

headers = {"User-Agent": "MyAPI/0.1 by reddit_user3699"}
res = requests.post("https://www.reddit.com/api/v1/access_token",
                         auth=auth, data=post_data, headers=headers)
print(res.json())
token = res.json()['access_token']
headers = {**headers, **{'Authorization': f'bearer {token}'}}

# info = requests.get("https://oauth.reddit.com/api/v1/me", headers = headers)
# print(info.json())
config_file = "python.config"
topic = "reddit1"
conf = ccloud_lib.read_ccloud_config(config_file)
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
redditproducer = Producer(producer_conf)
py_subreddit_info = requests.get("https://oauth.reddit.com/r/python/new", headers = headers)
#print(py_subreddit_info.json()['data']['children'][0]['data'].keys())
for post in py_subreddit_info.json()['data']['children']:
    subreddit = post['data']['subreddit']
    author = post['data']['author_fullname']
    title = post['data']['title']
    time_created = post['data']['created_utc']
    print(post)
    print(post['data'].keys())
    break
    #redditproducer.produce(topic, value=post)
    print("record appended")
# print(py_subreddit_info.json()['data'])



