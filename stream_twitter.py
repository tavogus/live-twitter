# set chdir to current dir
import os
import sys
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sqlite3
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from unidecode import unidecode
import time
from threading import Lock, Timer
import pandas as pd
from textblob import TextBlob

sys.path.insert(0, os.path.realpath(os.path.dirname(__file__)))
os.chdir(os.path.realpath(os.path.dirname(__file__)))

analyzer = SentimentIntensityAnalyzer()

ckey="IMJh4kjQLGDzUaT9t1v0RXm5Y"
csecret="cjt9d684CpvElXof1BxUMgSakNnFBVLDweQTSpGZolzzrnU8JE"
atoken="968521944944529408-oI5NcJVaZellwrsPjhsQkQPDeAZJzKf"
asecret="hc7bTI65fG97smD3ZEB6iCjLrBzHBxn2Sp6TIaX8fZSJZ"

conn = sqlite3.connect('twitter.db', isolation_level=None, check_same_thread=False)
c = conn.cursor()

def create_table():
    try:
        # http://www.sqlite.org/pragma.html#pragma_journal_mode
        c.execute("PRAGMA journal_mode=wal")
        c.execute("PRAGMA wal_checkpoint=TRUNCATE")
        c.execute("CREATE TABLE IF NOT EXISTS sentiment(id INTEGER PRIMARY KEY AUTOINCREMENT, unix INTEGER, tweet TEXT, sentiment REAL)")
        c.execute("CREATE INDEX id_unix ON sentiment (id DESC, unix DESC)")
        # https://sqlite.org/fts5.html - 4.4.2
        c.execute("CREATE VIRTUAL TABLE sentiment_fts USING fts5(tweet, content=sentiment, content_rowid=id, prefix=1, prefix=2, prefix=3)")
        c.execute("""
            CREATE TRIGGER sentiment_insert AFTER INSERT ON sentiment BEGIN
                INSERT INTO sentiment_fts(rowid, tweet) VALUES (new.id, new.tweet);
            END
        """)
        
    except Exception as e:
        print(str(e))
create_table()

lock = Lock()

class listener(StreamListener):

    data = []
    lock = None

    def __init__(self, lock):

        self.lock = lock
        self.save_in_database()
        super().__init__()

    def save_in_database(self):
        Timer(1, self.save_in_database).start()

        with self.lock:
            if len(self.data):
                c.execute('BEGIN TRANSACTION')
                try:
                    c.executemany("INSERT INTO sentiment (unix, tweet, sentiment) VALUES (?, ?, ?)", self.data)
                except:
                    pass
                c.execute('COMMIT')

                self.data = []

    def on_data(self, data):
        try:
            data = json.loads(data)
            
            if 'truncated' not in data:
                return True
            if data['truncated']:
                tweet = unidecode(data['extended_tweet']['full_text'])
            else:
                tweet = unidecode(data['text'])
            time_ms = data['timestamp_ms']
            vs = analyzer.polarity_scores(tweet)
            sentiment = vs['compound']
            
            with self.lock:
                self.data.append((time_ms, tweet, sentiment))

        except KeyError as e:
            print(str(e))
        return True

    def on_error(self, status):
        print(status)

while True:
    try:
        auth = OAuthHandler(ckey, csecret)
        auth.set_access_token(atoken, asecret)
        twitterStream = Stream(auth, listener(lock))
        twitterStream.filter(track=["a","e","i","o","u"])
    except Exception as e:
        print(str(e))
        time.sleep(5)
