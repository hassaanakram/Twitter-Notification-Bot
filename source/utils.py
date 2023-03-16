import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor
import datetime
import numpy as np
from httplib2 import Http
from json import dumps


def get_keywords(file_path, keywords_queue, time_queue, stream):
	'''get_keywords runs on a thread to fetch the keywords to be searched.
	The said excel file is checked every 10 minutes for changes. 
	args:
	file_path: full filepath for the excel spreadsheet
	'''
	old_keywords = []
	old_keyword_time = {}
	while True:
		# Read excel file
		try:
			keywords_file = pd.read_csv(file_path)
		except PermissionError:
			print("Permission error occured. Please close the keywords file")
			continue
		# Get keywords
		try:
			keywords = keywords_file["keywords"]
			keywords = keywords.dropna()
			if keywords.empty:
				print("No keywords found")
				continue
			keywords = keywords.tolist()
		except:
			print("\nUnhandled exception occured while reading keywords. Re-starting thread.\n")
			continue
	
		if old_keywords != keywords:
			# Implies a change
			keywords_queue.put(keywords)
			stream.disconnect()
			old_keywords = keywords
		else:
			# Do nothing
			pass
		
		try:
			print("reading timeframe")
			timeframe = keywords_file[["start", "end"]]
			timeframe = timeframe.dropna()
			start_time = timeframe["start"].tolist()
			end_time = timeframe["end"].tolist()

			keyword_time = {k:[s_t, e_t] for k,s_t,e_t in zip(keywords, start_time, end_time)}

			print(f'Start time: {start_time}\nEnd time: {end_time}\n')
		except:
			print("Unhandled exception occured while reading Timeframe. Restarting thread.")
			continue
		
		changed_time = []
		print(f'Old times: {list(old_keyword_time.values())}\n')

		# Looping (sigh) over the old and new keywords to see for changes
		old_kwords = list(old_keyword_time.keys())
		new_kwords = list(keyword_time.keys())
		for kword in new_kwords:
			if kword in old_kwords:
				# Keyword is present. check if time changed
				times = keyword_time[kword]
				if times == old_keyword_time[kword]:
					# Time didn't change either
					continue
				else:
					# The keyword didn't change but the time did
					changed_time.append(new_kwords.index(kword))
					old_keyword_time[kword] = keyword_time[kword]
			else:
				# New keyword
				changed_time.append(new_kwords.index(kword))
				old_keyword_time[kword] = keyword_time[kword]		

		print(f'Changed time: {changed_time}\n')

		if changed_time:
			req_timeframe = timeframe.iloc[changed_time, :]
			req_keywords = [keywords[i] for i in changed_time]
			keywords_time = (req_keywords, req_timeframe)
			time_queue.put(keywords_time)
		
		# Wait for 15 seconds
		time.sleep(15)


def stream_tweets(keywords_queue, stream):	
	while True:
		try:
			search_keywords = keywords_queue.get()
			if search_keywords:
				stream.filter(track=search_keywords)
		except:
			print("\nUnhandled exception occured in streaming thread. Restarting.\n")
			continue


def send_message(tweets_queue):
	while True:
		try:
			urls = []

			while len(urls) != 10:
				text = tweets_queue.get()
				if text:
					urls.append(text)
				
			# Now start multiple requests
			with ThreadPoolExecutor(max_workers=10) as pool:
				pool.map(send_request_tweets, urls)
		except:
			print("\nUnhandled exception occured while sending messages. Restarting thread.\n")
			continue


def send_request_tweets(req):

	url = 'https://chat.googleapis.com/v1/spaces/AAAApvSQFWE/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=gLFNv5v0UK0yXFc9Z99A4Cu2rdqQIONnbMitRChQBck%3D'
	message = {'text': req}
	message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
	http_object = Http()

	http_object.request(uri=url,
						method='POST',
						headers=message_headers,
						body=dumps(message))


def send_request_sentiment(req):
	url = 'https://chat.googleapis.com/v1/spaces/AAAAJ_MTnIQ/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=h8lv02UmGyMYWV4IkV4siuw6o5r5Kh8Jyiu7z91KdvE%3D'

	message = {'text': req}
	message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
	http_object = Http()

	http_object.request(uri=url,
						method='POST',
						headers=message_headers,
						body=dumps(message))


def get_tweet_data(tweets_file):
	timestamps = tweets_file["timestamp"].dropna().to_numpy()
	timestamps_dt = list(map(lambda x: datetime.datetime.strptime(x, " %d-%m-%Y %H:%M"), timestamps))
	latest_time_dt = timestamps_dt[-1]

	return latest_time_dt, timestamps_dt


def get_sentiment_analysis(time_queue,
						   dir_id,
						   drive
						   ):
	delay_timeframe = pd.DataFrame(columns=["start", "end"])
	delay_keywords = []
	delay_flag = False
	while(True):
		try:
			if delay_flag:
				time_queue.put((delay_keywords, delay_timeframe))
				delay_flag = False
				delay_keywords = []
				delay_timeframe = pd.DataFrame(columns=["start", "end"])
			keywords, timeframe = time_queue.get()
			
			start_times = timeframe["start"].dropna().to_numpy()
			end_times = timeframe["end"].dropna().to_numpy()
			print(f'\nGot times: {start_times} ---- {end_times}\n')
			
			tweets_file = pd.read_csv('../sentiment_analysis_files/tweets.csv')
			if tweets_file.empty:
				print('\nNo tweets in DB\n')
				continue

			for i in range(len(keywords)):
				start_time = start_times[i]
				end_time = end_times[i]
				keyword = keywords[i]

				if start_time == '-' or end_time == '-':
					continue
				if end_time == 'CONTINUE':
					continue
				elif end_time == 'STOP':
					end_time_dt, _ = get_tweet_data(tweets_file)
					end_time = end_time_dt.strftime("%d-%m-%Y %H:%M")
				else:
					end_time_dt = datetime.datetime.strptime(end_time,
								" %d-%m-%Y %H:%M")
				
				start_time_dt = datetime.datetime.strptime(start_time,
								" %d-%m-%Y %H:%M")
				latest_time_dt, timestamps_dt = get_tweet_data(tweets_file)

				if (end_time_dt > latest_time_dt or start_time_dt > latest_time_dt):
					delay_keywords.append(keyword)
					delay_times = [[start_time, end_time]]
					delay_timeframe = delay_timeframe.append(
									pd.DataFrame(delay_times, columns=["start", "end"])) 
					delay_flag = True
					continue

				sentiments = tweets_file["sentiment"].dropna().to_numpy()
				text = tweets_file["text"].dropna()

				start_idx = np.where(np.array(timestamps_dt)>=start_time_dt)
				end_idx = np.where(np.array(timestamps_dt)<=end_time_dt)
				start_idx = start_idx[0]
				end_idx = end_idx[0]
				start_idx = start_idx[0]
				end_idx = end_idx[-1]
				idx = list(range(start_idx, end_idx+1))

				text = text[idx]
				indices = text.str.contains(keyword, regex=False, case=False)

				required_sentiments = sentiments[idx]
				required_sentiments = required_sentiments[indices]
				required_text = text[indices].to_numpy()
				# Creating csv file to be uploaded to gdrive
				file_df = pd.DataFrame({'sentiment':required_sentiments, 'tweet':required_text})
				file_name = f'../sentiment_analysis_files/{keyword}_{start_time}_to_{end_time}.csv'              
				file_name = file_name.replace(":", " ")
				file_df.to_csv(file_name, mode='w')

				# Uploading to gdrive
				gfile = drive.CreateFile({'parents':
				[{'id':dir_id}]})
				gfile.SetContentFile(file_name)
				gfile.Upload()

				# Get file id to create link
				
				positive = np.sum(required_sentiments=="Positive")
				negative = np.sum(required_sentiments=="Negative")
				positive = (positive/required_sentiments.shape[0])*100
				negative = (negative/required_sentiments.shape[0])*100

				# Relay the stats
				stats = f'From {start_time} To {end_time}\nKeyword: {keyword}\nTweets: {required_sentiments.shape[0]}\nPositives: {positive:.1f}%\nNegatives: {negative:.1f}%'

				send_request_sentiment(stats)
		except Exception as e:
			print(e)
			continue


			


