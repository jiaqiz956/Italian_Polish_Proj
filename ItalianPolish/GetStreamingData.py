from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import datetime
import csv

consumer_key = ''
consumer_secret = ''
access_token_key = ''
access_token_secret = ''

class StdOutListener(StreamListener):
	def on_connect(self):
		print("You are connected to the Twitter API")
		
	def on_status(self, status):
  #  if (status.lang == "en") & (status.user.followers_count >= 500):
	#	if status.lang == ['it', 'pl', 'ja']:
		if (status.lang == "it") or (status.lang == "pl") or (status.lang == "ja"): 
		#& (status.retweet_count >= 10):
	# Altering tweet text so that it keeps to one line
			text_for_output = "'" + status.text.replace('\n', ' ') +"'"
			csvw.writerow([status.id,
						  status.retweeted,
						  status.created_at.strftime('%m/%d/%y'),
						  status.favorite_count,
						  status.retweet_count,
						  status.lang,
						  status.coordinates,
						  status.user.id,
	# Using datetime to parse it to just get date
						  status.user.followers_count,
						  status.user.location,                     
						  text_for_output])
			return True
        
	def on_error(self, status_code):
		if status_code == 420:
		# Returning False in on_error disconnects the stream
			return False
			
if __name__ == '__main__':
   
	l = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token_key, access_token_secret)
	stream = Stream(auth, l)
locations = [7.05809, 36.71703, 18.37819, 46.99623] #IT
#[14.24712, 49.29899, 23.89251, 23.89251] #PL
#[124.15717, 24.34478, 145.575, 45.40944] #JP
keyword = ['corona', '#coronavirus', '#COVID19Italia',  '#Koronawirus', '#COVID19Pandemic', '新型コロナウィルス', 'コロナ','新型肺炎',
'covid', 'covid19', 'sarscov2', '#corona virus', '#Coronavirus',
 'SARS-CoV-2','covid-19', 'corona virus', '#2019nCoV', '#codvid_19', '#codvid19', 
 '#conronaviruspandemic', '#coronaflu', '#coronaoutbreak', '#coronapandemic', '#Coronapanik', '#coronavid19']
languages = ['it', 'pl', 'ja']

# with open('streaming_test4.csv', 'w') as f:
f = open('it_0530_02.csv', 'w')
fieldnames=['id', 'retweeted', 'created_at','favorite_count', 'retweet_count','lang', 'coordinates', 'user_id',
'user_followers_count', 'user_location', 'text']
csvw = csv.writer(f, lineterminator='\n')
# csvw = csv.writer(open('streaming_test4.csv', 'w'))
csvw.writerow(fieldnames)
stream.filter(locations = locations, track = keyword, languages = languages, is_async=True)    
               
"""                   
with open('streaming_test4.csv', 'w', newline='') as f:
	fieldnames=['id', 'retweeted', 'created_at','favorite_count', 'retweet_count','lang', 'coordinates', 'user_id', 'user_screen_name',
    'user_followers_count', 'user_location', 'text']
   # cscwriter = csv.writer(f, fieldnames, delimiter=';')
	#cscwriter = csv.writer(f, fieldnames=fieldnames)
	csvwriter = csv.DictWriter(f, headers=fieldnames.keys())
	csvwriter.writerow(fieldnames)
#csvwriter = csv.writer("streaming_test4.csv", "a")
	#csvwriter.writerow(fieldnames)
"""
#stream.filter(track=['star wars'])
