#Could Work
#Streaming API


from TwitterAPI import TwitterAPI
import sys
import tweepy
import time
import datetime
from dateutil.tz import tzlocal
from random import randint
import pdb
import os
import psycopg2
import copy
import string

#pp = pprint.PrettyPrinter(depth=6)

consumer_key = ''
consumer_secret = ''
access_token_key = ''
access_token_secret = ''

def save_tweet(mark, connection, data_dict):
	# print("Saving the tweet here...")
	tweet_id_str = str(data_dict['id'])
	if tweet_id_str == 'None':
		print("No id for this tweet; skipping")
		return 2
	author_id_str = str(data_dict['user_id'])
	if author_id_str == 'None':
		print("No author id for this tweet; skipping")
		return 2
	# pdb.set_trace()
	# first check we didn't already add this tweet to the database
	statement = "SELECT count(*) FROM streaming_tweets WHERE id = " + tweet_id_str
	mark.execute(statement)
	results = mark.fetchall()
	if results[0][0] != 0:
		# tweet already in database -- as tweets are sent from API from newest to oldest, we should already have this user's older tweets
		# so return False to interrupt collection for this user
		# NB if we have had some error that interrupted collection, if we return True here then all this user's tweets will be (re)collected
		return 1
	sql_column_section = "INSERT INTO streaming_tweets ("
	sql_value_section = ") VALUES (" 
	for data_item in data_dict.keys():
		sql_column_section += data_item + ", "
		if data_dict[data_item] is None:
			sql_value_section += 'Null, '
		else:
			# if data_item == 'coordinates':
				# print("Coordinates!")
				# pdb.set_trace()
			if isinstance(data_dict[data_item],str):
				sql_value_section += '$QQ$' + data_dict[data_item] + '$QQ$, '
			elif isinstance(data_dict[data_item],datetime.datetime):
				sql_value_section += '$QQ$' + str(data_dict[data_item]) + '$QQ$, '
			elif isinstance(data_dict[data_item],dict):
				#probably a Point type
				if data_dict[data_item]['type'] == 'Point':
					sql_value_section += "'(" + str(data_dict[data_item]['coordinates'][0]) + "," + str(data_dict[data_item]['coordinates'][1]) +")', "
				else:
					print("Don't know how to add this to SQL INSERT statement:")
					print(data_dict[data_item])
					pdb.set_trace()
			else:
				sql_value_section += str(data_dict[data_item]) + ', '
	#delete the trailing commas on both strings
	statement = sql_column_section[:-2] + sql_value_section[:-2] + ")"
	# print(statement)
	# pdb.set_trace()
	mark.execute(statement)
	connection.commit()
	return 0

#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):
	def on_status(self, status):
		print(status)
	def on_error(self, status_code):
		if status_code == 420:
			print("error found")
			# returning false disconnects the stream
			return False 
			
# connect to the tweet database	
db_name = 'Covid19'
connection_string = "dbname='" + db_name + "' user='postgres' host='localhost' password=''"
try:
    connection = psycopg2.connect(connection_string);
    print("Connected to database ", db_name)
except:
    print("Unable to connect to database")
mark = connection.cursor()

auth1 = tweepy.auth.OAuthHandler(consumer_key,consumer_secret)
auth1.set_access_token(access_token_key,access_token_secret)
api = tweepy.API(auth1)

locations = [7.05809, 36.71703, 18.37819, 46.99623] #IT
#[14.24712, 49.29899, 23.89251, 23.89251] #PL
#[124.15717, 24.34478, 145.575, 45.40944] #JP
keywords = ['corona', '#coronavirus', '#COVID19Italia',  '#Koronawirus', '#COVID19Pandemic', '新型コロナウィルス', 'コロナ','新型肺炎',
	'covid', 'covid19', 'sarscov2', '#corona virus', '#Coronavirus',
	 'SARS-CoV-2','covid-19', 'corona virus', '#2019nCoV', '#codvid_19', '#codvid19', 
	 '#conronaviruspandemic', '#coronaflu', '#coronaoutbreak', '#coronapandemic', '#Coronapanik', '#coronavid19']
languages = ['it', 'pl']

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
myStream.filter(locations = locations, track = keywords, languages = languages, is_async=True)


