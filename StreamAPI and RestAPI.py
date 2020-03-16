from tweepy import OAuthHandler
import tweepy
import time
import mongoClient as mongo
import json
import numpy as np
#counters for total tweets, redundant and retweets/quote
tweetCounter = np.zeros((6,))
retweetCounter = np.zeros((6,))
redundantCounter = np.zeros((6,))
#time array to seperate the time intervals

timeArray = [10]
i=np.zeros((1)).astype(int)

#authentication and connection with tweeter api
consumer_key=""
consumer_secret=""
access_token=""
access_token_secret=""
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api=tweepy.API(auth)


class MyStreamListener(tweepy.StreamListener):
#time limit    
    def __init__(self, time_limit):
        self.start_time = time.time()
        self.limit = time_limit
        super(MyStreamListener, self).__init__()
#collect the json file and store in MongoDB
# counting related metadata from the json file such as retweets and quotes
#also counting the total number of tweets gathered
    def on_data(self, data): 
        all_data = json.loads(data)
        print(all_data)
        timestamp = all_data["timestamp_ms"]
        if (time.time() - self.start_time) < self.limit:
            db1.db_collection.insert_one(all_data)
            if ((float(timestamp)/1000 -self.start_time) < timeArray[0]):
                tweetCounter[i] +=1
            else :
                timeArray[0] += 10
                i[0] +=1
            if (db1.db_collection.find({data:"text"})):
                redundantCounter [i] +=1
            if ("retweeted_status" in all_data.keys() or all_data["is_quote_status"]==True):
                retweetCounter [i] +=1 
            
        else:
            print ("SEARCH ENDED")
            return False  
        print(float(timestamp)/1000 -self.start_time)
	
 

#connect to mongo db and create collection
db1=mongo.mongoConnect("Stream - Rest")

#parameteres for timelimit and specific geolocation
myStream = tweepy.Stream(auth=api.auth, listener=MyStreamListener(time_limit=10))
myStream.filter(track=['football'], languages=['en'])

start_time = time.time()
limit = 10
#tweet limit perpage
limitTweets = 50
#below the parameters for the search are set inside the cursor
tweets = tweepy.Cursor(api.search, q="football", result_type="recent",include_entities=True,lang="en").items(limitTweets)
for tweet in tweets:
    try:
        tweet = tweet._json
        if hasattr(tweet, 'created_at'):        
            print('Tweet.created_at: ' + str(tweet['created_at']))
            timestamp = tweet.created_at

        print('Tweet.text : ' + str(tweet['text']))
        text = str(tweet['text'])   
        db1.db_collection.insert_one(tweet) 
        t_end = time.time() +60
        #searching for retweets and quotes
        if  (hasattr(tweet, 'retweeted_status') or tweet["is_quote_status"]):
            retweetCounter [i] +=1 
            print (text)
        print('-'*20)
    except tweepy.TweepError as e:  
        print(e.reason)
        raise
    except StopIteration: #stop iteration when last tweet is reached
        break
