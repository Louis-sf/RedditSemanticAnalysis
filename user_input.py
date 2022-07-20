import json
#import reddit_api_poller
from confluent_kafka import Producer
import ccloud_lib
from datetime import datetime
import requests
import praw
from prawcore import NotFound


################PRAW CONFIG########################
client_key = "34p9yVEI4vWN7YgOLv_phA"
client_secret = "MmVidcg43Lab7ckKZK6kvt8IGe_7Dw"
auth = requests.auth.HTTPBasicAuth(client_key, client_secret)
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
################Kafka Producer Config#################
topic = "user_input"
config_file = "python.config"
conf = ccloud_lib.read_ccloud_config(config_file)
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
userProducer = Producer(producer_conf)


def sub_invalid(sub):
    invalid = False
    try:
        reddit.subreddits.search_by_name(sub, exact=True)
    except NotFound:
        invalid = True
    return invalid

try:
    while True:
        try:
            sub = input("Please choose a subreddit you would like to analyze, don't include /r in the beginning\n")
            if sub_invalid(sub):
                print("this subreddit does not exist, please enter again")
                continue
            my_string = str(input('Please enter the start date: \nEnter date(yyyy-mm-dd): '))
            start_date = datetime.strptime(my_string, "%Y-%m-%d").timestamp()
            my_string = str(input('Please enter the end date: \nEnter date(yyyy-mm-dd): '))
            end_date = datetime.strptime(my_string, "%Y-%m-%d").timestamp()
            Schema = {
                "sub_reddit": sub,
                "start_date": str(int(start_date)),
                "end_date": str(int(end_date))
            }
            userProducer.produce(topic, value=json.dumps(Schema))
            #reddit_api_poller.getredditrawthread()
            #print("tuple <" + sub, ", ", start_date, ", ", end_date, "> appended to user_input topic")
        except ValueError:
            print("Please make sure the input format is correct")
            continue
except KeyboardInterrupt:
    print("Program ended")