import tweepy
from globals import tweets_queue
import pytz
from vaderSentiment import vaderSentiment
from googletrans import Translator
import pandas as pd
from filter_model import load_model, get_prediction


class MyStreamListener(tweepy.Stream):
	def __init__(self, consumer_key, consumer_secret, access_token, token_secret,
				):
		self.translator = Translator()
		self.model = load_model()
		self.sentiment = vaderSentiment.SentimentIntensityAnalyzer()
		super().__init__(consumer_key, consumer_secret, access_token, token_secret)


	def disconnect(self):
		if self.session:
			self.session.close()
			self.running = False
			#self.on_disconnect()


	def on_status(self, tweet):
		# Check for retweeted status
		if hasattr(tweet, "retweeted_status"):
			if hasattr(tweet.retweeted_status, "extended_tweet"):
				text = tweet.retweeted_status.extended_tweet['full_text']
			else:
				text = tweet.retweeted_status.text
		else:
			if hasattr(tweet, "extended_tweet"):
				text = tweet.extended_tweet['full_text']
			else:
				text = tweet.text

		# Append timestamp in ESD
		timestamp = tweet.created_at
	
		est =  timestamp.astimezone(pytz.timezone('EST5EDT'))

		link = 'https://twitter.com/twitter/statuses/' + tweet.id_str
		# Translation
		if text:
			try:
				text = self.translator.translate(text).text
			except:
				return
		# Check if the tweet is talking about football
		prediction = get_prediction(self.model, text)
		if not prediction:
			return
		# Sentiment
		
		sentiment_score = self.sentiment.polarity_scores(text)
		sentiment_score = sentiment_score['compound']

		# Associate sentiment wrt sentiment_score
		if sentiment_score > 0.05:
			sentiment = 'Positive'
		elif sentiment_score <= 0.05 and sentiment_score >= -0.05:
			sentiment = 'Neutral'
		elif sentiment_score <= -0.05:
			sentiment = 'Negative'
		
		timestamp = est.strftime(" %d-%m-%Y %H:%M")
		
		# Storing tweet to file
		tweet_df = pd.DataFrame([[timestamp, text, sentiment]])
		try:
			tweet_df.to_csv("../sentiment_analysis_files/tweets.csv", index=False, mode='a', header=False)
		except PermissionError:
			print("Permission error in tweets file. Please close it")
			return
		# Put to the queue
		text = f'Link: {link}\nUser: {tweet.user.name}\nTimestamp: {timestamp}\nSentiment: {sentiment}\nTweet: {text}'
		#print(f'{text}')
		tweets_queue.put(text)


	def on_error(self,status):
		print("Error detected")
