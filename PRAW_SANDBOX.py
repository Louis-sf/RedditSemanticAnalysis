import requests
import praw
import json

from confluent_kafka import Producer

import ccloud_lib
client_key = "34p9yVEI4vWN7YgOLv_phA"
client_secret = "MmVidcg43Lab7ckKZK6kvt8IGe_7Dw"
auth = requests.auth.HTTPBasicAuth(client_key,client_secret)
user_agent = "MyAPI/0.1 by reddit_user3699"
user_name = "reddit_user3699"
password = "Fe@5$yfcMGqRxP5"

reddit = praw.Reddit(
    client_id=client_key,
    client_secret=client_secret,
    user_agent=user_agent,
    password=password,
    username=user_name
)
gaming_subreddit = reddit.subreddit("gaming")

#config for producer
config_file = "python.config"
topic = "reddit1"
conf = ccloud_lib.read_ccloud_config(config_file)
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
redditproducer = Producer(producer_conf)

#producing loop
for submission in reddit.subreddit("FORMULA1").top(limit=10000):
    title = submission.title
    author = submission.author
    url_link = submission.url
    sub_id = submission.id
    Schema = {
        'title':str(title),
        'author':str(author),
        'url':str(url_link)
    }
    record = json.dumps(Schema)
    redditproducer.produce(topic = topic, key= sub_id, value = record)
    print("record appended for:",submission.title)


