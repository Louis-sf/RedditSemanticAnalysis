import json
from confluent_kafka import Producer
from confluent_kafka import Consumer
import ccloud_lib
from datetime import datetime
import requests
import praw
from psaw import PushshiftAPI
import psaw_helper



################PSAW CONFIG########################
client_key = "34p9yVEI4vWN7YgOLv_phA"
client_secret = "MmVidcg43Lab7ckKZK6kvt8IGe_7Dw"
auth = requests.auth.HTTPBasicAuth(client_key, client_secret)
user_agent = "MyAPI/0.1 by reddit_user3699"
user_name = "reddit_user3699"
password = "Fe@5$yfcMGqRxP5"

praw_i = praw.Reddit(
    client_id=client_key,
    client_secret=client_secret,
    user_agent=user_agent,
    password=password,
    username=user_name
)
psaw_i = PushshiftAPI(praw_i)


################ Consumer Config #####################
config_file = "python.config"
consumer_topic = "user_input"
conf = ccloud_lib.read_ccloud_config(config_file)

consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
consumer_conf['group.id'] = 'indefiniteconsumer'
consumer_conf['auto.offset.reset'] = 'latest'
consumer_conf['enable.auto.commit'] = 'false'

api_Consumer = Consumer(conf)
api_Consumer.subscribe([consumer_topic])

############## Producer Config #####################
producer_topic = "reddit_raw_data"
config_file = "python.config"
conf = ccloud_lib.read_ccloud_config(config_file)
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
raw_producer = Producer(producer_conf)

####################def############
raw_producer.flush()
#def getredditrawthread():
try:
    while True:
        try:
            count = 0
            msg = api_Consumer.poll(1.0)
            if msg is None:
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
                break
            else:
                # Check for Kafka message
                record_value = msg.value()
                data = json.loads(record_value)
                subreddit = data['sub_reddit']
                start_epoch = int(data['start_date'])
                end_epoch = int(data['end_date'])
                request_id = data['request_id']
                for record in psaw_helper.get_pushshift_data(start_epoch, end_epoch, subreddit):
                    count += 1
                    text = record['title'] + "**&*" + record['selftext']
                    sub = record['subreddit']
                    url = record['url']
                    id =record['id']
                    Schema = {
                        'id': id,
                        'request_id': request_id,
                        'title_text': text,
                        'sub_reddit': sub,
                        'url': url
                    }
                    to_be_recorded = json.dumps(Schema)
                    raw_producer.produce(topic = producer_topic, key = id, value = to_be_recorded)
                    print("record", record['title'], datetime.fromtimestamp(record['created_utc']), "appended", "\n")
                print(count, "records were appended")
                raw_producer.flush()
        except KeyError:
            raw_producer.flush()
            continue
except KeyboardInterrupt:
    raw_producer.flush()

#getredditrawthread()
