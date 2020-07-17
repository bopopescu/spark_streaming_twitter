import json
from builtins import print

import requests
import requests_oauthlib

from read_config import ReadConfig


class TwitterApp:
    """
    Class to process the twitter data.
    """

    def __init__(self):
        """
        Constructor to initialise credentials.
        """
        self.config = ReadConfig()
        self.access_token = self.config.get_access_token()
        self.access_secret = self.config.get_access_secret()
        self.consumer_key = self.config.get_consumer_key()
        self.consumer_secret = self.config.get_consumer_secret()

    def get_auth(self):
        """
        Method to generate the auth request with credentials.
        :return: Object that stores the values for auth.
        """
        return requests_oauthlib.OAuth1(self.consumer_key, self.consumer_secret, self.access_token, self.access_secret)

    def get_tweets(self):
        """
        Method that calls twitter api url.
        :return: Response for a stream of tweets.
        """
        # url for twitter
        url = 'https://stream.twitter.com/1.1/statuses/filter.json'
        # setting query params for language,location and filtering to track '#'
        query_data = [('language', 'en'), ('locations', '-130,-20,100,50'), ('track', '#')]
        # setting the url with list comprehension
        query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
        # sending the request to get the stream object with required url , auth
        response = requests.get(query_url, auth=self.get_auth(), stream=True)
        print(query_url, response)
        return response

    def send_tweets_to_spark(self, twitter_resp, tcp_connection):
        """
        Method to take response from twitter and extract the tweets from the whole tweets json object.
        :param twitter_resp: Response from twitter containing tweets json object.
        :param tcp_connection: Connection to send data to the spark streaming instance.
        """
        # extracting each tweet in single lines from the response
        for line in twitter_resp.iter_lines():
            try:
                # fetching each line in json format
                all_tweets = json.loads(line)
                print(all_tweets)
                d = {'hashtags': all_tweets['entities']['hashtags'], 'id': all_tweets['id_str'],
                     'created_at': all_tweets['created_at'], 'place': all_tweets['place']['name']}

                tcp_connection.send(bytes((json.dumps(d) + "\n"), 'utf-8'))
            except Exception as exception:
                print('Error: %s' % exception)
