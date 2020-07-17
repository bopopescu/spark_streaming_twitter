import unittest
from unittest import mock

from processor import Processor


class Twitter:
    def __init__(self):
        pass

    def get_tweets(self):
        return "tweets"

    def send_tweets_to_spark(self, resp, conn):
        return None

    def bind(self, obj):
        return None

    def listen(self, num):
        return None

    def accept(self):
        return "conn", "addr"


class MockData:
    @staticmethod
    def get_twitter_object():
        return Twitter()


class MyTestCase(unittest.TestCase):

    @mock.patch('processor.print')
    @mock.patch('processor.socket')
    @mock.patch('processor.TwitterApp')
    def test_processor(self, mock_twitter, mock_socket, mock_print):
        mock_twitter.return_value = MockData.get_twitter_object()
        mock_socket.socket.return_value = MockData.get_twitter_object()
        mock_print.return_value = True

        p = Processor()
        p.processor()

        self.assertTrue(mock_print.called)
        self.assertTrue(mock_twitter.called)
        self.assertTrue(mock_socket.socket.called)


if __name__ == '__main__':
    unittest.main()
