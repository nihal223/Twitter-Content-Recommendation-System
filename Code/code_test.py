from newspaper import Article, fulltext
import newspaper
import random
import pprint
import tweepy
from py2neo import authenticate,Graph
from queue import *
from threading import Thread

consumer_key = 'kj9cUM86BaS3FzFRoaLvkeG9h'
consumer_secret = 'kAnjywflxcAkyOAZC5XJp0TVSZzSw4MD4wmD3hKWQbXKVU7Yqv'
access_token = '3080462928-8QIM1ppPfGWyTVd46AsWRRdxZxpjadqzACB1AM9'
access_token_secret = 'KCcYqOxh6jni909Vv7rMXfVFVfI3xuOiRidUvCbofQX2Q'

# set up authentication parameters
authenticate("localhost:7474", "neo4j", "nihal223")

# connect to authenticated graph database
graphdb = Graph('http://localhost:7474/db/data')
#print(type(graphdb))

INSERT_USER_URL_QUERY = '''
    MERGE (user:User {username: {username}})
    MERGE (url:URL {url: {url}})
    CREATE UNIQUE (user)-[:SHARED]->(url)
    FOREACH (kw in {keywords} | MERGE (k:Keyword {text: kw}) CREATE UNIQUE (k)<-[:IS_ABOUT]-(url))
'''

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit = True, wait_on_rate_limit_notify = True)

ids = api.friends_ids()
#urls = [('http://www.foxnews.com/us/2016/11/25/black-friday-kicks-off-with-fatal-shooting-in-new-jersey-mall-parking-lot.html', 'MartinGarrix')]
urls = []

for friend in ids:
    statuses = api.user_timeline(id=friend, count=200)
    for status in statuses:
        if status.entities and status.entities['urls']:
            for url in status.entities['urls']:
                urls.append((url['expanded_url'], status.author.screen_name))
                #print(urls)

with open('urls.csv', 'w') as f:
    for url in urls:
        f.write(url[0] + ',' + url[1] + '\n')
    f.close()

def parseURL(url):
    a = Article(url)
    #print(url)
    try:
        a.download()
        a.parse()
        a.nlp()
        keywords = a.keywords
        del(a)
        return (keywords)
    except:
        return (None)  

def insertUserURL(url, user):
    keywords = parseURL(url)
    print(keywords)
    if keywords:
    	params = {}
    	params['username'] = user
    	params['url'] = url
    	params['keywords'] = keywords
    	graphdb.run(INSERT_USER_URL_QUERY, params)
    	#print('-------------------')

for url, user in urls:
    insertUserURL(url, user)

'''def doWork():
    while True:
    	urlTuple = q.get()
    	insertUserURL(urlTuple[0], urlTuple[1])
    	q.task_done()  

# number of threads / maximum concurrent requests
concurrent = 200

# init the work queue
q = Queue(concurrent * 2)

for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()
try:
    with open('urls.csv', 'r') as f:
        for line in f:
            l = line.split(',')
            url = l[0]
            # trim the newline
            user = l[1].replace('\n', '')
            q.put((user, url))
    q.join()
except:
    pass'''