# -*- coding: utf-8 -*-

#==============================================================================
# A simple twitter client capable of persisting data to a DB
# Usage: python client.py --terms=datascience --ignore=spammer1,spammer2
# Writing to database can be disabled by using --persist=no
# To persist to a database a valid DNS entry is needed.
# Also, create a proper Tweets table. Check 'insert' method in DbConnector.
# =============================================================================
#
# A JSON formatted config file is mandatory
#
#  Example:
#
#  {
#   "config":{
#     "services": {
#       "db": {
#         "active"           : false,
#         "connectionstring" : "DSN=YOUR_DSN_NAME;Uid=YOUR_USER;Pwd=YOUR_PWD"
#       },
#       "twitter": {
#         "consumerKey"    : "TWITTER_CK",
#         "consumerSecret" : "TWITTER_CS",
#         "accessToken"    : "TWITTER_AT",
#         "accessSecret"   : "TWITTER_AS"
#       }
#     },
#     "ignore": {
#       spammers: [
#           "spammer1",
#           "spammer2"
#         ],
#       terms: [
#           "term1",
#           "term2"
#       ]
#     },
#     "filter": [
#          "python",
#          "datascience",
#          "machinelearning"
#     ]
#   }
# }

from __future__ import unicode_literals
from __future__ import print_function
import pypyodbc
import sys
import getopt
import re
import codecs
from colorama import *
from datetime import datetime
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import json
_persist   = True

CONSUMER_KEY     = u''
CONSUMER_SECRET  = u''
ACCESS_TOKEN     = u''
ACCESS_SECRET    = u''
DATABASE_DSN     = u''

class TweetEntry(object):
    """ Represents a tweet message """
    username = None
    tweet = None
    created = None
    hashtags = []
    def __init__(self, user, message, hash_tags=[]):
        self.username = user
        self.tweet    = message
        self.created  = datetime.now()
        self.hashtags = list(hash_tags)
    def to_string(self):
        """ Returns the tweet as a well-formatted string """
        return u'{0} | {1}'.format(self.username.decode('utf-8'),
                                   self.tweet.decode('utf-8')).replace('\n', ' ').replace('\r', '')

class DbConnector(object):
    """ Helper class for managing DB access """
    cursor = None
    def __init__(self, dsn_name):
        self.connect(dsn_name)

    def connect(self, dsn_name):
        self.conn = pypyodbc.connect(dsn_name)
        self.cursor = self.conn.cursor()
        print(u'Database connection established')

    def insert(self, TweetEntry):
        self.cursor.execute('''INSERT INTO [dbo].[Tweets]
                                           ([ScreenName]
                                           ,[Message]
                                           ,[CreatedAt]
                                           ,[Hashtags])
                                     VALUES
                                           (?
                                           ,?
                                           ,?
                                           ,?)''',(TweetEntry.username,
                                                   TweetEntry.tweet,
                                                   TweetEntry.created,
                                                   ", ".join(TweetEntry.hashtags)))
        self.cursor.commit()

class Listener(StreamListener):
    """Twitter listener implementation"""
    db = None
    hashtags = []
    ignore = []
    def __init__(self, hash_tags, ignore_users, ignore_terms):
        self.hashtags     = list(hash_tags)
        self.ignore       = list(ignore_users)
        self.ignore_terms = list(ignore_terms)
        if _persist:
            self.connect_db()
            print(u'Initialized Twitter Listener with DB')
        else:
            print(u'Initialized Twitter Listener without DB')
        print(u'======================= TWEETS =======================')

    def on_data(self, data):
        """ Must be implemented so the TwitterStream instance cann call it """
        parsed   = json.loads(data)
        username = parsed["user"]["screen_name"]
        tweet    = parsed["text"]
        db_entry = TweetEntry(username, tweet, self.hashtags)
        if self.user_ok(username) and self.tweet_ok(tweet):
            if _persist:
                self.db.insert(db_entry)
            line = unicode(username.ljust(20) + ' | ' + tweet.rjust(20))
            self.print_colorized(line)
        return True

    def user_ok(self, username):
        return username not in self.ignore

    def tweet_ok(self, tweet):
        return not any(term in tweet for term in self.ignore_terms)

    def on_error(self, status):
        print(status)

    def print_colorized(self, line):
        """ Colorize console output """
        for term in self.hashtags:
            line = re.sub(ur'(' + re.escape(term) + ur')', Style.BRIGHT + Fore.RED +
                                        Back.BLACK + ur'\1' + Fore.RESET + Back.RESET,
                                        line.lower(), re.UNICODE)
        print(line.encode('utf-8','replace'))

    def connect_db(self):
        """ Connect with DB by using DSN info """
        self.db = DbConnector(DATABASE_DSN)

def activate_twitter(hash_tags = [], ignore_users = [], ignore_terms = []):
    """ Connect to Twitter API """
    auth                        = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    listener                    = Listener(hash_tags, ignore_users, ignore_terms)
    twitter_stream              = Stream(auth, listener)
    twitter_stream.filter(track =hash_tags)

def usage():
     print(u'Usage: twitter_client.py --config=[JSON_formatted_config]')

def start_client(_json):
    hash_tags = None
    # load ignored users
    ignore       = [_user.lower().decode('utf-8') for _user in _json['config']['ignore']['spammers']]
    ignore_terms = [_term.lower().decode('utf-8') for _term in _json['config']['ignore']['terms']]
    if(len(ignore) > 0):
        print(u'Ignoring users: {0}'.format(ignore))
    if(len(ignore_terms)):
        print(u'Ignoring terms: {0}'.format(ignore_terms))
    # configure twitter api access
    access          = _json['config']['services']['twitter']
    CONSUMER_KEY    = access['consumerKey'].decode('utf-8')
    CONSUMER_SECRET = access['consumerSecret'].decode('utf-8')
    ACCESS_TOKEN    = access['accessToken'].decode('utf-8')
    ACCESS_SECRET   = access['accessSecret'].decode('utf-8')
    # configure persistence
    database = _json['config']['services']['db']
    if(database['active'] == True):
        _persist     = True
        DATABASE_DSN = database['dsn']
    else:
        _persist = False
        DATABASE_DSN = None
    # configure filtering of messages
    hash_tags = _json['config']['filter']
    print(u'Using filter: {0}'.format(hash_tags))
    hash_tags = [tag.lower().decode('utf-8') for tag in hash_tags]
    activate_twitter(hash_tags, ignore, ignore_terms)

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hc:d", ["help", "config="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h","--help"):
            usage()
            sys.exit()
        elif opt == '-d':
            global _debug
            _debug = True
        elif opt in ("-c","--config"):
            print(u'Loading config from file {0}'.format(arg))
            with codecs.open(arg,'r',encoding='utf-8') as config_file:
                _json = json.load(config_file)
                start_client(_json)

if __name__ == "__main__":
    main(sys.argv[1:])