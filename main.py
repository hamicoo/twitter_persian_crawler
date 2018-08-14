from __future__ import unicode_literals
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import psycopg2.extras
import datetime
import time
#from mongo import mongo_hashtag_writer
#from redis_ import redis_hashtag_writer
from hazm import *
import os
os.environ["HTTPS_PROXY"] = "http://ir402013:136820@us.mybestport.com:443"


stemmer = Stemmer()
normalaizer=Normalizer()

conn = psycopg2.connect("host='192.168.64.131' dbname='postgres' user='postgres' password='123'")
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
# connect to our cluster
from elasticsearch import Elasticsearch

#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
# import os
# os.environ["HTTPS_PROXY"] = "http://ir402013:136820@us.mybestport.com:443"


consumer_key = "-"
consumer_secret = "-"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token = "-"
access_token_secret = "-"


# cur.execute("""select count(*) from twitter """)
# count = cur.fetchall()
# print(count)





cur.execute("""set timezone ='iran'""")
cur.execute("""select hash_tag from twitter a where date(a.datetime)=current_date """)
first_list = cur.fetchall()
imported_list=[]
for i in first_list:
	imported_list.append(str(i).replace('[', '').replace(']', '').replace("'", ''))
print(imported_list)








# for i in imported_list_:
#     j = [i.rstrip().lstrip()]
#     imported_list.append(j)
# print(imported_list)
# print(len(imported_list))
print('------------Miner Start-----------------')



# The pil way (if you don't have matplotlib)
# image = wordcloud.to_image()
# image.show()







class StdOutListener(StreamListener):

	def on_data(self, data):
		try:
			tweet_data = json.loads(data)
			print('1 >>>>>>>>>>>')
			try:
				if 'retweeted_status' in tweet_data:
					print('%%'+str(len(tweet_data["retweeted_status"]["extended_tweet"]["full_text"])))
					tweet_text = tweet_data["retweeted_status"]["extended_tweet"]["full_text"]
				elif 'extended_tweet' in tweet_data:
					print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
					tweet_text = tweet_data["extended_tweet"]["full_text"]
				else:
					tweet_text=(tweet_data['text'])
			except:
				tweet_text = (tweet_data['text'])






			print(tweet_text)
			tweet_text_=tweet_text
			name=tweet_data['user']['name']
			screen_name=tweet_data['user']['screen_name']
			follower_cnt=tweet_data['user']['followers_count']
			friends_count=tweet_data['user']['friends_count']
			status_count=tweet_data['user']['statuses_count']
			favourites_count=tweet_data['user']['favourites_count']
			location = tweet_data['user']['location']

			cur.execute("""insert into public.twitter_detailed(twitt_content,source,follower_cnt,friends_count,user_favorite_count,screen_name,status_count,location)values (%s,%s,%s,%s,%s,%s,%s,%s)""", (tweet_text_,name,follower_cnt,friends_count,favourites_count,screen_name,status_count,location))
			mongo_hashtag_writer(tweet_data)

			conn.commit()
		except BaseException as e:
			print('System Error' + str(e))


		hash_tag_cnt = len(tweet_data["entities"]["hashtags"])
		if hash_tag_cnt > 0:
			for tags in tweet_data["entities"]["hashtags"]:
				local_tag=cleaner(tags['text'])
				if local_tag in imported_list:
					print(local_tag + " -- توی لیست بود--------------")
					cur.execute("""update public.twitter set cnt=cnt+1,datetime=%s where id in (select id from twitter where hash_tag=%s order by cnt desc limit 1)""",
					            (datetime.datetime.now(), local_tag))
				else:
					print(local_tag + "-- اضافه کردیم------" )
					cur.execute("""insert into public.twitter(hash_tag,cnt) values (%s,%s)""", (local_tag, 1))

					#redis_hashtag_writer(local_tag)
					conn.commit()
					imported_list.append(local_tag)
					print('------ تعداد ------' + str(len(imported_list)))
			print(imported_list)


# r = requests.get('http://localhost:9200')
# es.index(index='hash_tag', doc_type='hashtag', id=len(hashtags), body=json.dumps(hashtags))


def twitter_engine(last_tag):
	last_tag = cleaner('روحانی')
	data=stream.filter(languages=["fa"], track=last_tag)
	return True


def cleaner(single_tag):
	single_tag = single_tag.replace("_", " ").replace("_"," ")
	single_tag=normalaizer.normalize(single_tag)

	print(single_tag)
	return single_tag


l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l, tweet_mode='extended')

while True:
	try:
		while twitter_engine(imported_list):
			twitter_engine(imported_list)
	except BaseException as e:
		print(" Ops Bugs Found ! " + str(e) )
		time.sleep(5)
		print(imported_list)
		continue

