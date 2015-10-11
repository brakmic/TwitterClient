# -*- coding: utf-8 -*-

#==============================================================================
# A simple twitter client capable of persisting data to a DB
# Usage: python client.py --config=config.json
# To persist to a database a valid DNS entry is needed.
# Also, create a proper Tweets table. Check 'insert' method in DbConnector.
# =============================================================================

from __future__ import unicode_literals
from __future__ import print_function
import pypyodbc
import sys
import getopt
import re
import codecs
from random import randint
from colorama import *
from datetime import datetime
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import json
_persist   = True

#CONSUMER_KEY    = u'bkFopGVOWnWgbbFV0sfpfTZCC'
#CONSUMER_SECRET = u'RUxJQTL9z4zBDsjw89BZrI13Nx2SPPEBJEuyASp1H93AdgZGNo'
#ACCESS_TOKEN    = u'21581491-gkbEXQN3ilmpJ0TTS336WbAAoNwWJaxY9WWJfQ4tG'
#ACCESS_SECRET   = u'lLGJHQ1Pp69WROBwN6DSew8bjw4xppqiTqAQKCcAYJin4'

CONSUMER_KEY     = u''
CONSUMER_SECRET  = u''
ACCESS_TOKEN     = u''
ACCESS_SECRET    = u''
DATABASE_DSN     = u''

DEFAULT_FORE = Fore.YELLOW
DEFAULT_BACK = Back.BLACK

colormap = {
    0: Fore.RED,
    1: Fore.GREEN,
    2: Fore.YELLOW,
    3: Fore.MAGENTA
}

class TweetEntry(object):
    """ Represents a tweet message """
    username = None
    tweet    = None
    created  = None
    hashtags = []
    def __init__(self, user, message, hash_tags=[]):
        self.username = user
        self.tweet    = message
        self.created  = datetime.now()
        self.hashtags = list(hash_tags)
    def to_string(self):
        """ Returns the tweet as a well-formatted string """
        return u'{0} | {1}'.format(self.username.decode('utf-8','replace'),
                                   self.tweet.decode('utf-8','replace').replace('\n', ' ').replace('\r', ''))

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
    db                 = None
    hashtags           = []
    colorized_hashtags = {}
    ignored_users      = []
    accepted_langs     = []
    ignored_terms      = []
    def __init__(self, hash_tags, ignore_users, ignore_terms, accept_langs):
        self.hashtags       = list(hash_tags)
        self.ignored_users  = list(ignore_users)
        self.ignored_terms  = list(ignore_terms)
        self.accepted_langs = list(accept_langs)
        self.assign_hashtag_colors()
        if _persist:
            self.connect_db()
            print(u'Initialized Twitter Listener with DB')
        else:
            print(u'Initialized Twitter Listener without DB')
        print(u'======================= TWEETS =======================')

    def on_data(self, data):
        """ Must be implemented so the TwitterStream instance cann call it """
        parsed   = json.loads(data,encoding='utf-8')
        if not 'user' in parsed:
            return True
        username = parsed["user"]["screen_name"].decode('utf-8','replace')
        tweet    = parsed["text"]
        lang     = parsed['lang']
        db_entry = TweetEntry(username, tweet, self.hashtags)
        if self.is_acceptable(username, tweet, lang):
            if _persist:
                self.db.insert(db_entry)
            line = username.ljust(20) + u' | ' + tweet.rjust(20)
            self.print_colorized(line)
        return True

    def is_acceptable(self, username, tweet, lang):
        return (self.lang_ok(lang) and self.user_ok(username) and self.tweet_ok(tweet))

    def user_ok(self, username):
        return username not in self.ignored_users

    def tweet_ok(self, tweet):
        return not any(term in tweet for term in self.ignored_terms)

    def lang_ok(self, lang):
        return lang in self.accepted_langs

    def assign_hashtag_colors(self):
        max = len(colormap)
        count = 0
        for tag in self.hashtags:
            if(count == max):
                count -= 1
            self.colorized_hashtags[tag] = colormap[count]
            count += 1

    def on_error(self, status):
        print(status)

    def print_colorized(self, line):
        """ Colorize console output """
        for term in self.hashtags:
            line = re.sub(ur'(' + re.escape(term) + ur')', Style.BRIGHT + self.colorized_hashtags[term] +
                                        Style.DIM + DEFAULT_BACK + ur'\1' + Fore.RESET + Back.RESET + Style.RESET_ALL,
                                        line.lower(), re.UNICODE)
        print(line.encode('utf-8','replace'))

    def connect_db(self):
        """ Connect with DB by using DSN info """
        self.db = DbConnector(DATABASE_DSN)

def activate_twitter(hash_tags = [], ignore_users = [], ignore_terms = [], accept_langs = []):
    """ Connect to Twitter API """
    auth                        = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    listener                    = Listener(hash_tags, ignore_users, ignore_terms, accept_langs)
    twitter_stream              = Stream(auth, listener)
    twitter_stream.filter(track =hash_tags)

def usage():
     print(u'Usage: twitter_client.py --config=[JSON_formatted_config]')

def start_client(_json):
    hash_tags = None
    # load ignored users
    ignore_users = [_user for _user in _json['config']['ignore']['users']]
    ignore_terms = [_term for _term in _json['config']['ignore']['terms']]
    # accept only messages written in following languages
    accept_langs = [_lang for _lang in _json['config']['accept']['languages']]
    if(len(ignore_users) > 0):
        print(u'Ignoring users: {0}'.format(ignore_users))
    if(len(ignore_terms)):
        print(u'Ignoring terms: {0}'.format(ignore_terms))
    if(len(accept_langs) > 0):
        print(u'Accepting only languages: {0}'.format(accept_langs))
    # configure twitter api access
    access          = _json['config']['services']['twitter']
    CONSUMER_KEY    = access['consumerKey']
    CONSUMER_SECRET = access['consumerSecret']
    ACCESS_TOKEN    = access['accessToken']
    ACCESS_SECRET   = access['accessSecret']
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
    hash_tags = [tag.lower() for tag in hash_tags]
    activate_twitter(hash_tags, ignore_users, ignore_terms, accept_langs)

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
            with codecs.open(arg,'r', encoding='utf-8') as config_file:
                _json = json.load(config_file)
                start_client(_json)

if __name__ == "__main__":
    main(sys.argv[1:])