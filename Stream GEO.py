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
# counting related metadata from the json file 
# searching for specific words to find retweets,quotes and total number of tweets   
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
db1=mongo.mongoConnect("StreamAPI-GEO")
#parameteres for timelimit and specific geolocation
myStream = tweepy.Stream(auth=api.auth, listener=MyStreamListener(time_limit=10))
myStream.filter(languages=['en'], locations=[-4.299907,55.835971,-4.192241,55.877593])