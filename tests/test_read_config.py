import unittest
from unittest import mock

from read_config import ReadConfig


class MyTestCase(unittest.TestCase):

    @mock.patch('read_config.yaml')
    @mock.patch('read_config.open')
    def test_get_access_token(self, mock_open, mock_yaml):
        mock_open.return_value.__enter__.return_value = True
        mock_yaml.safe_load.return_value = {'development': {'access_token': 'my_access_token'}}

        r = ReadConfig()
        actual_result = r.get_access_token()

        self.assertEqual(actual_result, 'my_access_token')

    @mock.patch('read_config.yaml')
    @mock.patch('read_config.open')
    def test_get_access_secret(self, mock_open, mock_yaml):
        mock_open.return_value.__enter__.return_value = True
        mock_yaml.safe_load.return_value = {'development': {'access_secret': 'my_access_secret'}}

        r = ReadConfig()
        actual_result = r.get_access_secret()

        self.assertEqual(actual_result, 'my_access_secret')

    @mock.patch('read_config.yaml')
    @mock.patch('read_config.open')
    def test_get_consumer_secret(self, mock_open, mock_yaml):
        mock_open.return_value.__enter__.return_value = True
        mock_yaml.safe_load.return_value = {'development': {'consumer_secret': 'my_consumer_secret'}}

        r = ReadConfig()
        actual_result = r.get_consumer_secret()

        self.assertEqual(actual_result, 'my_consumer_secret')

    @mock.patch('read_config.yaml')
    @mock.patch('read_config.open')
    def test_get_consumer_key(self, mock_open, mock_yaml):
        mock_open.return_value.__enter__.return_value = True
        mock_yaml.safe_load.return_value = {'development': {'consumer_key': 'my_consumer_key'}}

        r = ReadConfig()
        actual_result = r.get_consumer_key()

        self.assertEqual(actual_result, 'my_consumer_key')


if __name__ == '__main__':
    unittest.main()
