import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import datetime
import time
import sys
import os
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import functools

print = functools.partial(print, flush=True)

consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
access_key = os.environ['ACCESS_KEY']
access_secret = os.environ['ACCESS_SECRET']
connection_string = os.environ['EVENTHUB_ENDPOINT']
event_hub_name = os.environ['EVENTHUB_NAME']
runtime = int(os.environ['RUNTIME_MINS'])*60
HASHTAGS = os.environ['HASHTAGS']

# Convert hashtags env variable to list and add # prefix
HASHTAGS = HASHTAGS.replace(" ", "")
HASHTAGS = HASHTAGS.split(",")
prefix_str = '#'
HASHTAGS = [prefix_str + i for i in HASHTAGS]

async def run(text):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string, eventhub_name=event_hub_name)
    async with producer:
        event_data_batch = await producer.create_batch()
        event_data_batch.add(EventData(text))
        await producer.send_batch(event_data_batch)


class StreamListener(tweepy.StreamListener):

    def __init__(self, time_limit):
        self.start_time = time.time()
        self.limit = time_limit
        api = tweepy.API(
            wait_on_rate_limit=True,
            wait_on_rate_limit_notify=True,
            compression=True)
        super(StreamListener, self).__init__()

    def on_connect(self):
        print("Established connection to the Twitter streaming API.")

    def on_error(self, status_code):
        print('An Error has occured: ' + repr(status_code),
              file=sys.stderr)
        return False

    def on_limit(self, status):
        print("API rate limit exceeded, sleep for 15 minutes")
        time.sleep(15 * 60)
        return True

    def get_text_cleaned(text,entity_dict): 
        # Removes any mentions, links, hashtags, symbols from the tweet's text
        slices = []
        for entityname, content in entity_dict.items():
            for key in content:
                slices += [{'start': key['indices'][0], 'stop': key['indices'][1]}]
        slices = sorted(slices, key=lambda x: -x['start'])

        for s in slices:
            text = text[:s['start']] + text[s['stop']:]
        
        # Finally, strip white spaces and new lines
        text = " ".join(text.split())
        # Return cleaned text to be stored in the tweetcontainer
        return text

    # Return true if tweet is extended_tweet
    def check_extended_tweet(status):
        if hasattr(status, "extended_tweet"):
            return True
        return False
    
    def check_extended_entities(status):
        if hasattr(status,"extended_entities"):
            return True
        return False

    # Return True if tweet is a reply to another tweet.
    def check_reply(status):
        if hasattr(status, "in_reply_to_status_id"):
            return True
        return False

    # Return True if tweet contains place information
    def check_place(status):
        if hasattr(status, "place.id"):
            return True
        return False

    # Return true if tweet in stream contains a retweeted tweet
    def check_retweeted(status):
        if hasattr(status, "retweeted_status"):
            retweet = status.retweeted_status
            if hasattr(retweet, 'user'):
                if retweet.user is not None:
                    if hasattr(retweet.user, "screen_name"):
                        if retweet.user.screen_name is not None:
                            return True
        return False

    # Return true if tweet in stream contains a quoted tweet
    def check_quoted_tweet(status):
        if hasattr(status, "quoted_status"):
            quote_tweet = status.quoted_status
            if hasattr(status.quoted_status, 'user'):
                if status.quoted_status.user is not None:
                    user = status.quoted_status.user
                    if hasattr(user, 'screen_name'):
                        if user.screen_name is not None:
                            return True
        return False

    def on_status(self, status):
        if (time.time() - self.start_time) < self.limit:
            print('processing incoming id:',status.id_str)
            loop = asyncio.get_event_loop()
            tweetcontainer = []
            tweet_details = {}

            # Capture tweeting user's profile attributes
            tweet_details['user_id_str'] = status.user.id_str
            tweet_details['user_name'] = status.user.name
            tweet_details['user_screenname'] = status.user.screen_name
            tweet_details['user_location'] = status.user.location
            tweet_details['coordinates'] = status.coordinates
            tweet_details['tweet_lang'] = status.lang
            tweet_details['user_created'] = status.user.created_at.strftime("%d-%b-%Y")
            tweet_details['created'] = status.created_at.strftime("%d-%b-%Y")
            tweet_details['followers_count'] = status.user.followers_count
            tweet_details['friends'] = status.user.friends_count
            tweet_details['user_listed_count'] = status.user.listed_count
            tweet_details['user_verified'] = status.user.verified
            tweet_details['statuses_count'] = status.user.statuses_count
            tweet_details['favourites_count'] = status.user.favourites_count            

            # Capture tweet's attributes
            tweet_details['tweet_id'] = status.id_str
            tweet_details['created'] = status.created_at.strftime("%d-%b-%Y")
            tweet_details['truncated'] = status.truncated

            # Capture tweet attributes. If the tweet being streamed is a quote, we only capture the top-level tweet's attributes.
            attributes = ['hashtags','user_mentions','urls','symbols','media']
            entity_dict = {}
            for attr in attributes:
                    #entity = None remove after test passes
                    entity = 'unprocessed'
                    entity_count = attr+"_count"
                    #while entity is None: remove after test passes
                    while entity is 'unprocessed':
                        try:
                            entity = status.extended_tweet['extended_entities'][attr]
                            break
                        except (AttributeError, KeyError) as e:
                            pass
                        try:
                            entity = status.extended_tweet['entities'][attr]
                            break
                        except (AttributeError, KeyError) as e:
                            pass
                        try:
                            entity = status.extended_entities[attr]
                            break
                        except (AttributeError, KeyError) as e:
                            pass
                        try:
                            entity = status.entities[attr]
                            break
                        except (AttributeError, KeyError) as e:
                            break

                    #if entity: # remove after test passes
                    if entity != 'unprocessed':
                        if len(entity) == 0:
                            tweet_details[attr] = None
                        else:
                            tweet_details[attr] = entity                    
                        tweet_details[entity_count] = len(entity)
                        entity_dict[attr] = entity
                    else:
                        tweet_details[attr] = None
                        tweet_details[entity_count] = 0
                        entity = None

                    if attr == 'media' and entity != 'null' and entity is not None:
                            media_container = []
                            for index, med in enumerate(entity):
                                media_details = {}
                                media_details['id_Str'] = entity[index]['id_str']
                                media_details['type'] = entity[index]['type']
                                media_details['media_url'] = entity[index]['media_url_https']
                                media_container.append(media_details)
                            tweet_details['media_info'] = media_container

            if hasattr(status, "extended_tweet"):
                try:
                    tweet_details['full_text'] = status.extended_tweet['full_text']
                except:
                    tweet_details['full_text'] = status.text         
            else:
                tweet_details['full_text'] = status.text
            
            tweet_details['cleantext'] = StreamListener.get_text_cleaned(tweet_details['full_text'], entity_dict)

            # Check if tweet is a reply to another tweet and if yes capture tweet's id, user's id, user's screen name.
            if StreamListener.check_reply(status):
                tweet_details['is_reply'] = True
                tweet_details['tweet_reply_id_str'] = status.in_reply_to_status_id
                tweet_details['tweet_reply_user_id_str'] = status.in_reply_to_user_id_str
                tweet_details['tweet_reply_user_screen_name'] = status.in_reply_to_screen_name
            else:
                tweet_details['is_reply'] = False

            # Check if tweet contains a retweet and get the retweeted tweet's id, user id and user screen name. Get also full text.
            if StreamListener.check_retweeted(status):
                tweet_details['has_retweet'] = True
                tweet_details['retweeted_tweet_id'] = status.retweeted_status.id_str
                tweet_details['retweeted_tweet_user_id'] = status.retweeted_status.user.id_str
                tweet_details['retweeted_tweet_user_screen_name'] = status.retweeted_status.user.screen_name
            else:
                tweet_details['has_retweet'] = False

            # Check if tweet contains a quote and get the quoted tweet's id, user id and user screen name
            if StreamListener.check_quoted_tweet(status):
                tweet_details['has_quote'] = True
                tweet_details['quoted_tweet_id'] =  status.quoted_status.id_str
                tweet_details['quoted_tweet_user_id'] =  status.quoted_status.user.id_str
                tweet_details['quoted_tweet_user_screen_name'] =  status.quoted_status.user.screen_name
            else:
                tweet_details['has_quote'] = False

            # Check if tweet contains a place and get the place attributes
            if StreamListener.check_place(status):
                tweet_details['has_place'] = True
                tweet_details['place_id'] = status.place.id
                tweet_details['place_url'] = status.place.url
                tweet_details['place_type'] = status.place.place_type
                tweet_details['place_name'] = status.place.name
                tweet_details['place_full_name'] = status.place.full_name
                tweet_details['place_country_code'] = status.place.country_code
                tweet_details['place_country'] = status.place.country
                tweet_details['place_bounding_box.type'] = status.place.bounding_box.type
                tweet_details['place_bounding_box.coords'] = status.place.bounding_box.coordinates
            else:
                tweet_details['has_place'] = False    
            
            tweetcontainer.append(tweet_details)
            if 'media' in tweet_details: del tweet_details['media'] 
            output = json.dumps(tweetcontainer)
            loop.run_until_complete(run(output))
            print('Successfully processed',tweet_details['tweet_id'])
        else:
            t = time.localtime()
            current_time = time.strftime("%H:%M:%S", t)
            print('Runtime limit of', self.limit, ' seconds reached, stopping connection at UTC time.',current_time)
            sys.exit()
            return False


def start_stream():
    while True:
        try:
            auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_key, access_secret)
            streamer = tweepy.Stream(
                auth=auth, listener=StreamListener(time_limit=runtime),tweet_mode='extended')
            t = time.localtime()
            current_time = time.strftime("%H:%M:%S", t)
            print("Started tracking process for", runtime, "seconds at UTC time",current_time)
            print("Tracking hashtags:", HASHTAGS)
            streamer.filter(track=HASHTAGS)
        except Exception as e:
            print('Error in main():')
            print(e.__doc__)


if __name__ == "__main__":
    start_stream()
