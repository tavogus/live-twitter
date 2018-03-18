from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sqlite3
from textblob import TextBlob
from unidecode import unidecode
import time

#consumer key, consumer secret, access token, access secret.
ckey="IMJh4kjQLGDzUaT9t1v0RXm5Y"
csecret="cjt9d684CpvElXof1BxUMgSakNnFBVLDweQTSpGZolzzrnU8JE"
atoken="968521944944529408-oI5NcJVaZellwrsPjhsQkQPDeAZJzKf"
asecret="hc7bTI65fG97smD3ZEB6iCjLrBzHBxn2Sp6TIaX8fZSJZ"

conn = sqlite3.connect('twitter.db')
c = conn.cursor()

def create_table():
    try:
        c.execute("CREATE TABLE IF NOT EXISTS sentiment(unix REAL, tweet TEXT, sentiment REAL)")
        c.execute("CREATE INDEX fast_unix ON sentiment(unix)")
        c.execute("CREATE INDEX fast_tweet ON sentiment(tweet)")
        c.execute("CREATE INDEX fast_sentiment ON sentiment(sentiment)")
        conn.commit()
    except Exception as e:
        print(str(e))
        
create_table()

class listener(StreamListener):

    def on_data(self, data):
        try:
            data = json.loads(data)
            tweet = unidecode(data['text'])
            time_ms = data['timestamp_ms']

            analysis = TextBlob(tweet)
            sentiment = analysis.sentiment.polarity

            print(time_ms, tweet, sentiment)
            c.execute("INSERT INTO sentiment (unix, tweet, sentiment) VALUES (?, ?, ?)",(time_ms, tweet, sentiment))
            conn.commit()

        except KeyError as e:
            print(str(e))
        return(True)

    def on_error(self, status):
        if status == 420:
            #returning False in on_data disconnects the stream
            return False

while True:

    try:
        auth = OAuthHandler(ckey, csecret)
        auth.set_access_token(atoken, asecret)
        twitterStream = Stream(auth, listener())
        twitterStream.filter(track=["a","e","i","o","u"])
    except Exception as e:
        print(str(e))
        time.sleep(5)