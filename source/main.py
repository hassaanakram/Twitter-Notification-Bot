# Imports
from os import name
import utils
from pandas import read_csv
import threading
from globals import keywords_queue, tweets_queue, time_queue
from MyStreamListener import MyStreamListener
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


# Get auth credentials for twitter and gdrive
credentials_file = read_csv('../resources/credentialsfile.csv')
consumer_key = (credentials_file['consumer key'])[0]
consumer_secret = (credentials_file['consumer secret'])[0]
access_token = (credentials_file['access token'])[0]
token_secret = (credentials_file['token secret'])[0]
dir_id = (credentials_file['dir id'])[0]

# Authenticate with gdrive
try:
	gauth = GoogleAuth()
	drive = GoogleDrive(gauth)
except Exception as e:
	print(e)

	# Setup stream
stream = MyStreamListener(consumer_key, consumer_secret, access_token, token_secret)

# File path of keywords file
file_path = '../resources/keywordsfile.csv'

filecheck_thread = threading.Thread(target=utils.get_keywords,
									args=(file_path, keywords_queue, time_queue, stream))
tweetcheck_thread = threading.Thread(target=utils.stream_tweets, 
									args=(keywords_queue, stream))
telegram_thread = threading.Thread(target=utils.send_message,
								args=(tweets_queue,))

sentiment_thread = threading.Thread(target=utils.get_sentiment_analysis,
									args=(time_queue, dir_id, drive))

filecheck_thread.setDaemon(True)
tweetcheck_thread.setDaemon(True)
telegram_thread.setDaemon(True)
sentiment_thread.setDaemon(True)

filecheck_thread.start()
tweetcheck_thread.start()
sentiment_thread.start()
telegram_thread.start()
while True:
	pass