# CHALLENGE EUROPEI ITALIA 

# vince chi scarica il maggior numero di tweets relativi al percorso dell’Italia agli Europei in corso, 
# proponendo l’analisi più interessante e distintiva

# Python
# Twitter API
# GetOldTweets3
# Scraping, Selenium, BeautifulSoup4
# AWS, Kinesis, Kafka

import GetOldTweets3 as Got # A Python 3 library and a corresponding command line utility for accessing old tweets.
import tweepy
from tweepy import OAuthHandler
import csv
import pandas as pd
import json
import re
import pprint
import io
import pymongo as pym

#The variable api is now the starting point for all our operations with Twitter.
#The wait_on_rate_limit parameter enables management of the client-side rate_limit. In this way we avoid being blocked for too much traffic.
#SCREEN NAME nome user :  (max 200 tweet in one request)
#For each status (== tweet) we have the complete Json through the ._json method.

# CHIAVI API

consumer_key = 't00h37NvLAinaCxoT4V0ebsOm'
consumer_secret = '8cJ2bM1YPeRFFvLE6xc5JJnJG6wTFL8soLaAZImHXGDMRkrpmo'
access_token = '3630281659-nUzX3LDtF9pA9OES6unm1AG4555LtW13LV7URja'
access_secret = 'KCuC9h4JINnxMpAv56ASWR0ki0BwTWSJ7tGkgtKgUyxyB'
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)


# RICERCA PER HASHTAG 

#count:The number of tweets to return per page, up to a maximum of 100. 
#until: Returns tweets created before the given date. Date should be formatted as YYYY-MM-DD. Keep in mind that the search index has a 7-day limit. In other words, no tweets will be found for a date older than one week
#q: A UTF-8, URL-encoded search query of 500 characters maximum, including operators. Queries may additionally be limited by complexity.
#geocode : ??

query = "#euro2021 OR #euro2020 OR #europei2021 OR #europei2020 OR #uefa2020 OR #uefa2021"
max_tweets = 1000000 # << diminuire per fare prove 
path_file= r'C:\Users\sophi\OneDrive\Desktop\MASTER\3. Modulo Big Data and Analytics (Pelucchi-Vaccarino)\1. Big Data\Codice\challenge_europei'
file_export_tweet = 'Euro_challenge.csv'

tweet_by_query = [] 
#newline='',

with open(f'{path_file}\{file_export_tweet}', 'w', encoding='utf-8') as csv_file:

    for ind, tweet in enumerate(tweepy.Cursor(api.search, q=query, count=100, lang="it", since = "2021-06-27", until = "2021-06-28", tweet_mode="extended" ).items() ):
        if ind <= max_tweets:
            print (f'INFO >> TWEET N : {ind} {tweet.created_at} {tweet.full_text}')
            tweet_by_query.append(tweet)
            writer = csv.writer(csv_file, delimiter=';',lineterminator='\n' , quoting = csv.QUOTE_ALL)
            if 'retweeted_status' in tweet._json:
                writer.writerow([tweet.created_at, tweet.full_text.strip().replace('\n',''), tweet._json['retweeted_status']['full_text'].strip().\
                                replace('\n','')]) 
            else:
                writer.writerow([tweet.created_at, tweet.full_text.strip().replace('\n',''),tweet.full_text.strip().replace('\n','')])

            if not tweet:
                break
        else:
            break

len(tweet_by_query)

print(type(tweet_by_query[0]))

 # RICERCA PER MENTIONS ?
 # RICERCA PER USER?
 # RICERCA PER CONTENUTO POST ?


# prendo i tweet più vecchi TODO
#tweetCriteria_ = Got.manager.TweetCriteria().setQuerySearch(query)\
#                                           .setSince("2021-06-10")\
#                                           .setUntil("2021-06-12")\
#                                           .setMaxTweets(10)
#tweets_older = Got.manager.TweetManager.getTweets(tweetCriteria_)[0]



