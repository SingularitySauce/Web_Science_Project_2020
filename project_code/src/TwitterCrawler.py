from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from pymongo import MongoClient
import json
import operator
from src import credentials
from tweepy import TweepError
from http.client import IncompleteRead as http_incompleteRead
from urllib3.exceptions import IncompleteRead as urllib3_incompleteRead
from urllib3.exceptions import ProtocolError
import pandas as pd
from copy import deepcopy
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer


class TwitterClient():
    def __init__(self, twitter_user=None):
        self.authenticator = TwitterAuthenticator().authenticate_app()
        self.twitter_client = API(self.authenticator)

        self.twitter_user = twitter_user

    def get_user_tweets_and_insert(self, num_tweets, collection):
        try:
            most_recent_tweets = self.twitter_client.user_timeline(id=self.twitter_user, count=num_tweets)
            for tweet in most_recent_tweets:
                collection.insert_one(tweet._json)
        except TweepError as e:
            pass


class TwitterAuthenticator():
    def authenticate_app(self):
        auth = OAuthHandler(credentials.CONSUMER_KEY, credentials.CONSUMER_KEY_SECRET)
        auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
        return auth


class TwitterStreamer():
    def __init__(self, limit=10000):
        self.authenticator = TwitterAuthenticator()
        self.listener = StdOutListener(limit)

    def stream_tweets_topic(self, hashtag_list, limit):
        self.listener = StdOutListener(limit)
        auth = self.authenticator.authenticate_app()
        stream = Stream(auth, self.listener)
        print("Now adding tweets from most popular hashtags")
        stream.filter(track=hashtag_list, languages=['en'])

    def stream_tweets_sample(self):
        auth = self.authenticator.authenticate_app()
        stream = Stream(auth, self.listener)

        while self.listener.count != 0:
            try:
                stream.sample(languages=['en'], stall_warnings=True)
            except (ProtocolError, AttributeError):
                continue

    def store_sample(self):
        # Create a sample of data - 2000 out of 20,000 tweets
        tweets = collection.find({}).limit(2000)

        temp_tweets = []

        for tweet in tweets:
            tweet.pop('_id')
            temp_tweets.append(tweet)

        # Save sample data to a file
        with open("sample.json", 'w') as f:
            f.write(json.dumps(temp_tweets))


        print("Stored sample of 2000 tweets as .json")

    def store_as_json(self):
        tweets = collection.find({})

        temp_tweets = []

        for tweet in tweets:
            tweet.pop('_id')
            temp_tweets.append(tweet)

        # Save sample data to a file
        with open("tweets.json", 'w') as f:
            f.write(json.dumps(temp_tweets))

        print("Stored tweets as .json")

    def find_powerusers_and_topics(self, data, number_users, number_hashtags):
        users = {}
        hashtags = {}

        for tweet in data:

            # Evaluate user data
            username = tweet['user']['screen_name']
            if username in users.keys():
                users[username] += 1
            else:
                users[username] = 1

            # Evaluate hashtags
            tweet_hashtags = tweet["entities"]["hashtags"]
            if len(tweet_hashtags) != 0:
                for hashtag in tweet_hashtags:
                    value = hashtag['text']
                    if value in hashtags.keys():
                        hashtags[value] += 1
                    else:
                        hashtags[value] = 1

        sorted_hashtags = sorted(hashtags.items(), key=operator.itemgetter(1), reverse=True)[:number_hashtags]
        sorted_users = sorted(users.items(), key=operator.itemgetter(1), reverse=True)[:number_users]

        return sorted_users, sorted_hashtags


class StdOutListener(StreamListener):
    def __init__(self, limit):
        self.count = 1
        self.limit = limit

    def on_data(self, data):
        try:
            t = json.loads(data)
            collection.insert_one(t)
            self.count += 1
            print(self.count)
            if self.count < self.limit:
                return True
            else:
                self.count = 0
                self.limit = 100000
                return False
        except BaseException as e:
            print("Error on data: %s" % str(e))
            return True
        except http_incompleteRead as e:
            print("http.client Incomplete Read error: %s" % str(e))
            return True
        except urllib3_incompleteRead as e:
            print("urllib3 Incomplete Read error: %s" % str(e))
            return True

    def on_error(self, status_code):
        if status_code == 420:
            print(status_code)
            return False
        else:
            print(status_code)
            pass


def data_collection(number_of_sample_tweets, number_of_power_users, tweets_per_user, number_of_hashtags,
                    hashtag_related_tweets):
    # Drop the collection and starts it up again fresh with every run
    db.drop_collection("tweets")
    db.create_collection("tweets")

    # Stream 1% of sample tweets until there's number_of_sample_tweets tweets
    streamer = TwitterStreamer(number_of_sample_tweets)
    streamer.stream_tweets_sample()

    # Fetch all tweets
    tweets = collection.find({})

    # Find a desired number of power users and popular hashtags to follow
    power_users, hashtags = streamer.find_powerusers_and_topics(tweets, number_users=number_of_power_users,
                                                                number_hashtags=number_of_hashtags)



    # Enrich the data by fetching tweets by power users and tweets relating to the identified topics
    for user in power_users:
        username = user[0]
        client = TwitterClient(twitter_user=username)
        client.get_user_tweets_and_insert(num_tweets=tweets_per_user, collection=collection)
        print("Added %s tweets" % username)

    hashtag_list = []
    for hashtag in hashtags:
        hashtag_list.append(hashtag[0])

    print("Adding tweets from most popular hashtags")
    streamer.stream_tweets_topic(hashtag_list=hashtag_list, limit=hashtag_related_tweets)
    print("Completed data collection")

    #Store data as json
    streamer.store_as_json()

    # Store sample data as .json
    streamer.store_sample()


def user_clustering(tweets, number_of_clusters):

    # Prepare container for the extracted text
    tweets_text = {}

    # Record duplicated encountered
    duplicates = 0

    # Pre-process tweets by removing duplicates and storing only ids and text
    for tweet in tweets:
        if 'text' in tweet.keys() and 'user' in tweet.keys():
            if tweet['id'] not in tweets_text.keys():
                text = tweet["text"]
                tweets_text[tweet["id"]] = text

            else:
                duplicates += 1

    print("Removed %d duplicates" % duplicates)

    # Load into data frame for ease of use
    tweets_frame = pd.DataFrame.from_dict(data=tweets_text, orient="index", columns=['text'])

    data = tweets_frame['text']

    # Transform the text into vectors of tf_idf form
    tf_idf_vectorizor = TfidfVectorizer(stop_words='english', max_features=2000)
    tf_idf = tf_idf_vectorizor.fit_transform(data)

    print("Beginning clustering data")
    # Initialize clustering model and cluster data
    model = KMeans(n_clusters=number_of_clusters, max_iter=100)
    model.fit(tf_idf)

    # Augument the data frame with cluster labels
    tweets_frame['cluster'] = model.labels_

    print("Finished clustering data")
    return tweets_frame


def find_matching_tweet(tweets, id):
    for tweet in tweets:
        if 'id' in tweet.keys():
            if tweet['id'] == id:
                return tweet


def analyze_clusters(dataFrame, number_of_clusters, tweets):
    streamer = TwitterStreamer()
    cluster_ids = {}

    for cluster in range(number_of_clusters):
        cluster_vals = dataFrame[dataFrame['cluster'] == cluster]
        cluster_ids[cluster] = cluster_vals.index.tolist()

    tweets_per_cluster = {}

    for cluster in cluster_ids.keys():
        cluster_tweets = []
        ids = cluster_ids[cluster]
        tweets_deepcopy = deepcopy(tweets)
        for tweet_id in ids:
            found_tweet = find_matching_tweet(tweets_deepcopy, tweet_id)
            cluster_tweets.append(found_tweet)

        tweets_per_cluster[cluster] = cluster_tweets
        print("Found tweets for cluster %s" % cluster)

    for cluster in tweets_per_cluster.keys():
        tweets_to_analyze = tweets_per_cluster[cluster]
        size = len(tweets_to_analyze)

        users, hashtags = streamer.find_powerusers_and_topics(tweets_to_analyze, 1, 1)
        print("Cluster %d has %d tweets with  " % (cluster, size))
        print("Power user: ", users, "Hashtags: ", hashtags)

    return tweets_per_cluster


def find_mentions_network(tweets):
    mentions_network = {}

    for tweet in tweets:
        if 'user' in tweet.keys() and 'retweeted_status' not in tweet.keys() and tweet['is_quote_status'] == False:
            username = tweet['user']['screen_name']
            quotes = tweet['entities']['user_mentions']
            if len(quotes) != 0:
                if username not in mentions_network.keys():
                    for mention in quotes:
                        mentions_network[username] = {mention['screen_name']: 1}
                else:
                    for mention in quotes:
                        mentioned_user = mention['screen_name']
                        if mentioned_user in mentions_network[username].keys():
                            mentions_network[username][mentioned_user] += 1
                        else:
                            mentions_network[username][mentioned_user] = 1

    return mentions_network


def find_retweet_network(tweets):
    retweet_network = {}

    for tweet in tweets:
        if 'user' in tweet.keys() and 'retweeted_status' in tweet.keys():
            username = tweet['user']['screen_name']
            retweeted_user = tweet['retweeted_status']['user']['screen_name']
            if username not in retweet_network.keys():
                retweet_network[username] = {retweeted_user: 1}
            else:
                retweeted_users = retweet_network[username]
                if retweeted_user in retweeted_users.keys():
                    retweet_network[username][retweeted_user] += 1
                else:
                    retweet_network[username][retweeted_user] = 1

    return retweet_network


def find_quote_network(tweets):
    quote_network = {}

    # count = 0

    for tweet in tweets:
        if 'user' in tweet.keys() and 'quoted_status' in tweet.keys():
            # count += 1
            username = tweet['user']['screen_name']
            quoted_user = tweet['quoted_status']['user']['screen_name']
            if username not in quote_network.keys():
                quote_network[username] = {quoted_user: 1}
            else:
                quoted_users = quote_network[username]
                if quoted_user in quoted_users.keys():
                    quote_network[username][quoted_user] += 1
                else:
                    quote_network[username][quoted_user] = 1

    # print(count, len(quote_network.keys()))
    return quote_network


def find_hashtag_network(tweets):
    hashtags = {}

    for tweet in tweets:
        if 'entities' in tweet.keys():
            tweet_hashtags = tweet['entities']['hashtags']
            if len(tweet_hashtags) != 0:
                for hashtag in tweet_hashtags:
                    text = hashtag['text']
                    if text not in hashtags.keys():
                        other_hashtags = []
                        for hashtag_other in tweet_hashtags:
                            text_other = hashtag_other['text']
                            if text != text_other:
                                other_hashtags.append(text_other)
                        hashtags[text] = other_hashtags
                    else:
                        other_hashtags = []
                        for hashtag_other in tweet_hashtags:
                            text_other = hashtag_other['text']
                            if text != text_other:
                                other_hashtags.append(text_other)
                            for other_hashtag in other_hashtags:
                                if other_hashtag not in hashtags[text]:
                                    hashtags[text].append(text_other)
    return hashtags


def find_ties_and_triads(mentions_network, retweet_network, quote_network):

    ties = []
    triads = []

    networks = [mentions_network, retweet_network, quote_network]

    for network in networks:
        users = network.keys()
        for user in users:
            connected_users = network[user]
            for connected_user in connected_users:
                tie = (user, connected_user)
                if tie not in ties:
                    ties.append((user, connected_user))

    for tie in ties:
        first = tie[0]
        second = tie[1]
        for tie_search in ties:
            potential_second = tie_search[0]
            potential_third = tie_search[1]
            if second == potential_second:
                triad = (first, second, potential_third)
                if triad not in triads:
                    triads.append(triad)

    return ties, triads


if __name__ == "__main__":

    cluster = MongoClient("mongodb+srv://user:1234@cluster0-qe3mx.mongodb.net/test?retryWrites=true&w=majority")
    db = cluster["tweets"]

    # Drop the collection and starts it up again fresh with every run
    collection = db["tweets"]

    #Collect the desired amount of data !!!!!UNCOMMENT TO RUN DATA COLLECTION OTHERWISE CODE WILL START FROM PART 2 ONWARDS!!!!
    #data_collection(number_of_sample_tweets=20000, number_of_power_users=50, tweets_per_user=20, number_of_hashtags=50,hashtag_related_tweets=2000)


    #Load data from the .json file
    tweets = []

    #To work with sample data, change 'tweets.json' to 'sample.json'
    for line in open('tweets.json', 'r'):
        tweets.append(json.loads(line))
    tweets = tweets[0]


    #Pick a desired number of clusters
    number_of_clusters = 10

    # Transform data and cluster based on the text
    clustered_tweets = user_clustering(tweets, number_of_clusters)

    # Return analysis of clusters
    tweets_by_cluster = analyze_clusters(clustered_tweets, number_of_clusters, tweets)


    #Find networks for general data and for all clusters
    print("Mentions network for general data")
    mentions_network_general_data = find_mentions_network(tweets)
    print(mentions_network_general_data)
    print("Mentions network size: %d" % len(mentions_network_general_data.keys()))

    print("Retweet network for general data")
    retweet_network_general_data = find_retweet_network(tweets)
    print(retweet_network_general_data)
    print("Retweet network size: %d" % len(retweet_network_general_data.keys()))

    print("Quote network for general data")
    quote_network_general_data = find_quote_network(tweets)
    print(quote_network_general_data)
    print("Quote network size: %d" % len(quote_network_general_data.keys()))

    print("Hashtag network for general data")
    hashtags_general_data = find_hashtag_network(tweets)
    print(hashtags_general_data)
    print("Found %d hashtags" % len(hashtags_general_data.keys()))


    cluster_networks = {}

    for cluster in tweets_by_cluster.keys():
        tweets_for_cluster = tweets_by_cluster[cluster]

        print("Mentions network for cluster %d" % cluster)
        mentions_cluster_network = find_mentions_network(tweets_for_cluster)
        print(mentions_cluster_network)
        print("Mentions network size: %d" % len(mentions_cluster_network.keys()))

        print("Retweet network for cluster %d" % cluster)
        retweet_cluster_network = find_retweet_network(tweets_for_cluster)
        print(retweet_cluster_network)
        print("Retweet network size: %d" % len(retweet_cluster_network.keys()))

        print("Quote network for cluster %d" % cluster)
        quote_cluster_network = find_quote_network(tweets_for_cluster)
        print(quote_cluster_network)
        print("Quote network size: %d" % len(quote_cluster_network.keys()))

        print("Hashtag network for cluster %d" % cluster)
        hashtags_for_cluster = find_hashtag_network(tweets_for_cluster)
        print(hashtags_for_cluster)
        print("Found %d hashtags" % len(hashtags_for_cluster.keys()))

        cluster_networks[cluster] = [mentions_cluster_network, retweet_cluster_network, quote_cluster_network]


    #Find ties and triads within general data and clusters

    ties, triads = find_ties_and_triads(mentions_network_general_data, retweet_network_general_data, quote_network_general_data)

    print("Within general data there are %d ties and %d triads" % (len(ties), len(triads)))


    for cluster in cluster_networks.keys():

        mentions = cluster_networks[cluster][0]
        retweets = cluster_networks[cluster][1]
        quotes = cluster_networks[cluster][2]

        ties, triads = find_ties_and_triads(mentions, retweets, quotes)

        print("Within cluster %d data there are %d ties and %d triads" % (cluster, len(ties), len(triads)))


