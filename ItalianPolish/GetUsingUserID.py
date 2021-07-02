#collect by user_id and lang = 'it', 'pl', 'ja'


import sys
import tweepy
import time
import datetime
from dateutil.tz import tzlocal
from random import randint
import pdb
import os
import psycopg2
import string

def fetch_settings(settings_file):
	if os.path.exists(settings_file):
		f = open(settings_file,'r')
		temp_text = f.read()
		f.close()
		settings_dict = {}
		temp_lines = temp_text.split('\n')
		for temp_line in temp_lines:
			temp_line_items = temp_line.split('\t')
			if len(temp_line_items) > 1:
				settings_dict[temp_line_items[0].strip()] = temp_line_items[1].strip()
		return settings_dict
	else:
		print("Sorry, can't locate the settings file:")
		print(settings_file)
		sys.exit()

# make a function to store info for a tweet.
# when we get a tweet, store that tweet's info (id, text, created_at, user_id etc)
# if it's a RT, store the RT info as a separate tweet data
# same if it's a reply to another tweet
# also store user info in the accounts table.
# need to deal with "array" data e.g. URLs

# make a function to store info for a tweet.
def save_tweet(mark, connection, data_dict, keyword):
	# print("Saving the tweet here...")
	# pdb.set_trace()
	tweet_id_str = str(data_dict['id'])
	if tweet_id_str == 'None':
		print("No id for this tweet; skipping")
		return False
	author_id_str = str(data_dict['user_id'])
	if author_id_str == 'None':
		print("No author id for this tweet; skipping")
		return False		
	# pdb.set_trace()
	# first check we didn't already add this tweet to the database
	sql = "SELECT id, keywords FROM tweets WHERE id = " + tweet_id_str
	mark.execute(sql)
	results = mark.fetchall()
	sql = ""
	if results != []:
		# print ("ADD CODE TO ADD KEYWORD TO KEYWORDS FIELD (AFTER CHECKING IT ISN'T IN IT ALREADY)")
		existing_keyword_count = None
		if results[0][1] is None:
			existing_keyword_count = 0
		else:
			if not keyword in results[0][1]:
				existing_keyword_count = len(results[0][1])
		if existing_keyword_count is not None:
			sql = "UPDATE tweets SET keywords [" + str(existing_keyword_count + 1) + "] = '" + keyword + "' WHERE user_id = " + str(results[0][0])
			# return False
	else:
		sql_column_section = "INSERT INTO tweets (keywords, "
		sql_value_section = ") VALUES (ARRAY ['" + keyword + "'], " 
		for data_item in data_dict.keys():
			sql_column_section += data_item + ", "
			if data_dict[data_item] is None:
				sql_value_section += 'Null, '
			else:
				if isinstance(data_dict[data_item],str):
					sql_value_section += "$QQ$" + data_dict[data_item] + "$QQ$, "
				elif isinstance(data_dict[data_item],datetime.datetime):
					sql_value_section += "$QQ$" + str(data_dict[data_item]) + "$QQ$, "
				#add Point datatype if we want to save coordinates data(datatime, Point...)
				elif isinstance(data_dict[data_item],dict):  
					if data_dict[data_item]['type'] == 'Point':
						sql_value_section += "'(" + str(data_dict[data_item]['coordinates'][0]) + "," + str(data_dict[data_item]['coordinates'][1]) +")', "
				else:
					sql_value_section += str(data_dict[data_item]) + ', '
	#delete the trailing commas on both strings
		sql = sql_column_section[:-2] + sql_value_section[:-2] + ")"  
	# pdb.set_trace()
	if sql != "":
#		print(sql)
		mark.execute(sql)
		connection.commit()
	return True
#statement = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public'AND table_name   = 'tweets'"
	
def construct_tweet_data_dict():
#    attributes = ['id', 'created_at', 'text', 'coordinates', 'place', 'lang', 'retweeted', 'retweet_count', 'favorite_count', 'possibly_sensitive', 'source', 'hashtags']
	attributes = ['id', 'created_at', 'text', 'coordinates', 'place_country_code', 'lang', 'retweeted', 'retweet_count', 'favorite_count'
	, 'possibly_sensitive', 'source', 'hashtags'
	,'user_id', 'user_name', 'user_screen_name', 'user_location'
	,'user_description', 'user_geo_enabled', 'user_followers_count'
	, 'user_friends_count', 'user_listed_count', 'user_statuses_count'
	, 'user_created_at']
	data_dict = {}
	for attribute in attributes:
		data_dict[attribute] = None
	return data_dict
		
def find_all(a_str, sub):
	start = 0
	while True:
		start = a_str.find(sub, start)
		if start == -1: return
		yield start
		start += len(sub)
	
def get_existing_id(mode, mark, keyword):
	if mode == 'newest':
		sort_direction = 'DESC'
	elif mode == 'oldest':
		sort_direction = 'ASC'
	# sql = "SELECT id FROM tweets WHERE lower(text) like '%" + keyword.lower() + "%' ORDER BY id DESC LIMIT 1"
	# finding a string as an item of an array:
	# select * from tweets where 'Japan' = ANY(keywords)
	sql = "SELECT user_id FROM tweets WHERE '" + keyword.lower() + "' = ANY(keywords) ORDER BY id " + sort_direction + " LIMIT 1"
	# print(sql)
	# pdb.set_trace()
	mark.execute(sql)
	results = mark.fetchall()
	if results == []:
		return(False)
	if results[0][0] != []:
		return(results[0][0])
	else:
		return(False)
	
def recordError(keyword, since_id, last_tweet_saved_id, error_report_dir):
	# write an error report containing the last id we collected, and the id we were collecting until. That way we fill the gap by collecting just between those tweets.
	error_output = "keyword\tsince_id\tlast_collected(max_id)\n"
	error_output += keyword + "\t" + str(since_id) + "\t" + str(last_tweet_saved_id) + "\n"
	timestamp_now = datetime.datetime.now()
	str_time_now = str(timestamp_now.year) + "-" + str(timestamp_now.month) + "-" + str(timestamp_now.day) + "-" + str(timestamp_now.hour) + "-" + str(timestamp_now.minute)
	f = open(error_report_dir + keyword + str_time_now + ".txt",'w')
	f.write(error_output)
	f.close()
	print(error_output)
	
def get_tweets_containing_keyword(keyword, lang, max_id, since_id, auth, mark, connection):
	last_tweet_saved_id = ''
	try:
		# pdb.set_trace()
		api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)
		if (max_id == False) and (since_id != False):
			c = tweepy.Cursor(api.search,q=keyword,since_id=since_id)
		elif (max_id != False) and (since_id == False):
			c = tweepy.Cursor(api.search,q=keyword,max_id=max_id)
		elif (max_id != False) and (since_id != False):
			c = tweepy.Cursor(api.search,q=keyword,max_id=max_id,since_id=since_id)
		else:
			c = tweepy.Cursor(api.search,q=keyword)
		# for j, page in enumerate(c.pages()):
			# for tweet in page:
				# print(tweet.text)
		interrupt_collection = False
		tweet_counter = 0
		# we get a TweepError: Not authorized if the user has protected their tweets
		# skip to next user in that case...
		# and set the error flag column in the accounts table to true
		# so that we don't try accessing this user's followers again and again
		# pdb.set_trace()
		for j, page in enumerate(c.pages()):
			#for debugging
			# if j > 1:
				# break
			# print('page ' + str(j))
			if interrupt_collection:
				break
			for tweet in page:
				# get the tweet's available attributes
				tweet_data_dict = construct_tweet_data_dict()
				available_attributes = dir(tweet)
				for attribute in tweet_data_dict.keys():
					if attribute in available_attributes:
						tweet_data_dict[attribute] = getattr(tweet,attribute)
				# assemble the data for this tweet and save
				# don't collect tweets or retweets sent before our cut off date
				# if tweet_data_dict['created_at'] < cut_off_date:
					# interrupt_collection = True
					# break
				# in many cases the tweet's text does not contain the keyword; no point in saving those
				# if keyword.lower() not in tweet.text.lower():
				#	continue
				
				tweet_data_dict['user_id'] = tweet.author.id
				tweet_data_dict['user_name'] = tweet.author.name
				tweet_data_dict['user_screen_name'] = tweet.author.screen_name
				tweet_data_dict['user_location'] = tweet.author.location
				tweet_data_dict['user_description'] = tweet.author.description
				tweet_data_dict['user_geo_enabled'] = tweet.author.geo_enabled
				tweet_data_dict['user_followers_count'] = tweet.author.followers_count
				tweet_data_dict['user_friends_count'] = tweet.author.friends_count
				tweet_data_dict['user_listed_count'] = tweet.author.listed_count
				tweet_data_dict['user_statuses_count'] = tweet.author.statuses_count
				tweet_data_dict['user_created_at'] = tweet.author.created_at
                #get coordinates data of lon and lat
				tweet_data_dict['coordinates'] = tweet.coordinates
				tweet_counter += 1
				if tweet_counter % 1000 == 0:
					print(keyword + ": collected " + str(tweet_counter) + "; " + str(tweet.created_at))
				# pdb.set_trace()
				save_tweet(mark, connection, tweet_data_dict, keyword)
				last_tweet_saved_id = tweet_data_dict['user_id']						
	except AttributeError as e:
		print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
	except KeyError as e:
		print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
	except psycopg2.ProgrammingError as e:
		print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
	except TypeError as e:
		print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
	except NameError as e:
		print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
		# pdb.set_trace()
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)
		recordError(keyword, since_id, last_tweet_saved_id, error_report_dir)
	if tweet_counter > 0:
		print("Collected " + str(tweet_counter) + "; last tweet " + str(tweet.created_at))
	else:
		print("Collected no tweets this time.")
	return True

if len(sys.argv) == 1:
	print("please provide the path to one or more settings.txt files containing the database name and Twitter API keys.")
	sys.exit()
# pdb.set_trace()
settings_file_list = sys.argv[1:]
error_report_dir = '/Users/jiaqizheng/OneDrive/TwitterResearch/CollectionErrors/'
for settings_file in settings_file_list:
	settings_dict = fetch_settings(settings_file)
	# connect to the tweet database
	db_name = 'Covid19'
	connection_string = "dbname='" + db_name + "' user='" + settings_dict["db_username"] + "' host='" + settings_dict["host"] + "' port='5432' password='" + settings_dict["db_password"] + "'"
	try:
		connection = psycopg2.connect(connection_string);
	except:
		print("I am unable to connect to database " + db_name)
	mark = connection.cursor()
	# get keywords from the keywords table
	sql = "SELECT keyword FROM keywords WHERE collect = TRUE"
	mark.execute(sql)
	results = mark.fetchall()
	keywords = ['corona', '#coronavirus', '#COVID19Italia',  '#Koronawirus', '#COVID19Pandemic', '新型コロナウィルス', 'コロナ','新型肺炎',
	'covid', 'covid19', 'sarscov2', '#corona virus', '#Coronavirus',
	 'SARS-CoV-2','covid-19', 'corona virus', '#2019nCoV', '#codvid_19', '#codvid19', 
	 '#conronaviruspandemic', '#coronaflu', '#coronaoutbreak', '#coronapandemic', '#Coronapanik', '#coronavid19']
	lang = ['it', 'pl', 'ja']
	for result in results:
		keywords.append(result[0])
	# set up connection to Twitter API	
	auth = tweepy.AppAuthHandler(settings_dict["consumer_key"], settings_dict["consumer_secret"])

	# START DEBUGGING
	#tweet_data_dict = {'coordinates_coordinates': None, 'created_at': datetime.datetime(2018, 9, 14, 23, 40, 43), 'favorited': False, 'favorite_count': 0, 'id': 1040747200379142144, 'in_reply_to_status_id': None, 'lang': 'en', 'media': None, 'place_bounding_box_coordinates': None, 'possibly_sensitive': None, 'quoted_status_id': None, 'retweeted': False, 'retweet_count': 357, 'retweeted_status_id': None, 'source': 'Twitter for iPhone', 'symbols': None, 'text': 'RT @ajplus: Countries rejected Japan’s proposal to allow "sustainable" whale hunting, which would have reversed a 30-year ban. The Internat…', 'user_id': 813484250783576064, 'user_name': 'Mareena Vivas', 'user_screen_name': 'mareena_vivas'}
	#keyword = "whaling"
	# save_tweet(mark, connection, tweet_data_dict, keyword)
	# print("STOP HERE")
	# pdb.set_trace()
	# END DEBUGGING


	for keyword in keywords:
		#TEMPORARY CODE
		# if keyword != '慰安婦':
			# continue
		oldest_id = get_existing_id("oldest",mark,keyword)
		newest_id = get_existing_id("newest",mark,keyword)
		"""
		# ADD KEYWORD AND IDS TO THE FOLLOWING CODE IF WE WANT TO FILL GAPS
		# 
		if keyword == 'KEYWORD_WITH_COLLECTION_GAPS': # replace 'KEYWORD_WITH_COLLECTION_GAPS' with the keyword
			newest_id = 1079543406701867009
			oldest_id = 1079497904904007680
			print(keyword + ": setting oldest_id and newest_id to fill a gap")
			get_tweets_containing_keyword(keyword, newest_id, oldest_id, auth, mark, connection)
		else:
		#  next get newly sent Tweets
		"""
		oldest_id = False
		newest_id = False
		print(keyword + ": getting newly sent Tweets")
		get_tweets_containing_keyword(keyword, False, newest_id, lang, auth, mark, connection)
