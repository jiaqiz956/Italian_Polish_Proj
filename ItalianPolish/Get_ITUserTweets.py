#could be used in specical , limited users
import tweepy
from tweepy import Cursor
import unicodecsv
from unidecode import unidecode
global linelist
import csv
import time

# Authentication and connection to Twitter API.
consumer_key = " "
consumer_secret = " "
access_key = " "
access_secret = " "

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

# Userid/names whose tweets we want to gather.
linelist = [line.rstrip('\n') for line in open('/Users/jiaqizheng/Desktop/covid19/data/it_user_id.txt')] #user_ids from Italy, concluding 3 langs
users = linelist




   
with open('it_usertweets.csv', 'wb') as file:
	writer = csv.writer(file, delimiter = ',', quotechar = '"')
# Write header row.
	writer.writerow(['user_id','user_screen_name', 'user_followers_count'
    ,'user_listed_count','user_friends_count','user_favourites_count','user_default_profile'
    ,'user_location','user_statuses_count','user_description'
    ,'user_geo_enabled','user_created_at'
    ,'id', 'created_at','text', 'lang', 'retweeted','retweet_count','favorite_count'
    ,'lat','long','tweet_in_reply_to_screen_name','tweet_direct_reply'
    ,'tweet_hashtags','tweet_hashtags_count','tweet_mentioned_user_count'])
    

	
	for user in users:
		user_obj = api.get_user(user)
		
			# Gather info specific to the current user.
		user_info = [user_obj.id,
						 user_obj.screen_name,
						 user_obj.followers_count,
						 user_obj.listed_count,
						 user_obj.friends_count,
						 user_obj.favourites_count,
						 user_obj.default_profile,
						 user_obj.location,
						 user_obj.statuses_count,
						 user_obj.description,
						 user_obj.geo_enabled,
						 user_obj.created_at]

			     
        # Get 1000 most recent tweets for the current user.
		for tweet in Cursor(api.user_timeline, id = user).items(3200):
			# Latitude and longitude stored as array of floats within a dictionary.
				lat = tweet.coordinates['coordinates'][1] if tweet.coordinates != None else None
				long = tweet.coordinates['coordinates'][0] if tweet.coordinates != None else None
			# If tweet is not in reply to a screen name, it is not a direct reply.
				direct_reply = True if tweet.in_reply_to_screen_name != "" else False
			# Retweets start with "RT ..."
				retweet_status = True if tweet.text[0:3] == "RT " else False

			# Get info specific to the current tweet of the current user.
				tweet_info = [tweet.id,
						  tweet.created_at,
						  unidecode(tweet.text),
						  tweet.lang,
						  retweet_status,
						  tweet.retweet_count,
						  tweet.favorite_count,
						  lat,
						  long,
						  tweet.in_reply_to_screen_name,
						  direct_reply]            

			# Below entities are stored as variable-length dictionaries, if present.
				hashtags = []
				hashtags_data = tweet.entities.get('hashtags', None)
				if(hashtags_data != None):
					for i in range(len(hashtags_data)):
						hashtags.append(unidecode(hashtags_data[i]['text']))

				user_mentions = []
				user_mentions_data = tweet.entities.get('user_mentions', None)
				if(user_mentions_data != None):
					for i in range(len(user_mentions_data)):
						user_mentions.append(unidecode(user_mentions_data[i]['screen_name']))


				more_tweet_info = [', '.join(hashtags),
							   len(hashtags),
							   len(user_mentions)]

			# Write data to CSV.
				writer.writerow(user_info + tweet_info + more_tweet_info)
					# Show progress.
			print("Wrote tweets by %s to CSV." % user)

        


