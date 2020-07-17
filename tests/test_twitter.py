import unittest
from unittest import mock

from twitter import TwitterApp


class Config:
    def __init__(self):
        pass

    def get_access_token(self):
        return "token"

    def get_access_secret(self):
        return "access_secret"

    def get_consumer_key(self):
        return "key"

    def get_consumer_secret(self):
        return "consumer_secret"

    def send(self, text):
        return None

    def iter_lines(self):
        return "a"


class MockData:
    @staticmethod
    def get_config_obj():
        return Config()


class MyTestCase(unittest.TestCase):

    @mock.patch('twitter.ReadConfig')
    @mock.patch('twitter.requests_oauthlib')
    def test_get_auth(self, mock_request_auth, mock_config):
        mock_config.return_value = MockData.get_config_obj()
        mock_request_auth.OAuth1.return_value = "auth"

        t = TwitterApp()
        actual_result = t.get_auth()

        self.assertEqual(actual_result, "auth")

    @mock.patch('twitter.ReadConfig')
    @mock.patch('twitter.print')
    @mock.patch('twitter.requests')
    def test_get_tweets(self, mock_request, mock_print, mock_config):
        mock_request.get.return_value = "response"
        mock_config.return_value = MockData.get_config_obj()
        mock_print.return_value = True

        t = TwitterApp()
        actual_result = t.get_tweets()

        self.assertEqual(actual_result, "response")
        self.assertTrue(mock_print.called)

    @mock.patch('twitter.ReadConfig')
    @mock.patch('twitter.print')
    @mock.patch('twitter.json')
    def test_send_tweets_to_spark_exception(self, mock_json, mock_print, mock_config):
        mock_json.loads.return_value = {'entities': {'hashtags': 'text'}, 'id_str': "id_string",
                                        'created_at': "time", 'place': {'name': 'place_name'}}
        mock_print.return_value = True
        mock_config.return_value = MockData.get_config_obj()

        t = TwitterApp()
        t.send_tweets_to_spark(MockData.get_config_obj(), MockData.get_config_obj())

        # self.assertTrue(mock_print.called)
        # self.assertTrue(mock_json.called)
        self.assertRaises(Exception)

    @mock.patch('twitter.ReadConfig')
    @mock.patch('twitter.print')
    @mock.patch('twitter.json')
    def test_send_tweets_to_spark(self, mock_json, mock_print, mock_config):
        mock_json.loads.return_value = {'entities': {'hashtags': 'text'}, 'id_str': "id_string",
                                        'created_at': "time", 'place': {'name': 'place_name'}}
        mock_print.return_value = True
        mock_config.return_value = MockData.get_config_obj()
        mock_json.dumps.return_value = "string"

        t = TwitterApp()
        t.send_tweets_to_spark(MockData.get_config_obj(), MockData.get_config_obj())

        self.assertTrue(mock_print.called)
        self.assertTrue(mock_json.loads.called)
        self.assertTrue(mock_json.dumps.called)


if __name__ == '__main__':
    unittest.main()
