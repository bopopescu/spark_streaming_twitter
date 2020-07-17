import unittest
from datetime import time
from unittest import mock

import spark_streaming


class MySqlActions:
    def __init__(self):
        pass

    def connect(self, host, user, database, autocommit, allow_local_infile):
        return Connection()


class Connection:
    def __init__(self):
        pass

    def cursor(self):
        return Cursor()

    def commit(self):
        return None


class Cursor:
    def __init__(self):
        pass

    def executemany(self, query, list_rdd):
        return None


class Rdd:
    def __init__(self):
        pass;

    def map(self, obj):
        return list_rdd()


class list_rdd:
    def __init__(self):
        pass

    def collect(self):
        return [{'hashtags': [{'text': 'text'}], 'id': "id_string",
                 'created_at': "time", 'place': {'name': 'place_name'}}]


class MyTestCase(unittest.TestCase):

    @mock.patch('spark_streaming.m')
    def test_insert_into_tweets(self, mock_mysql):
        mock_mysql.connect.return_value = Connection()

        spark_streaming.insert_into_tweets([1, 2, 3])

        self.assertTrue(mock_mysql.connect.called)

    def test_is_proper_string(self):
        actual_result = spark_streaming.is_proper_string("divya")
        self.assertEqual(actual_result, True)

    def test_is_proper_string_exception(self):
        actual_result = spark_streaming.is_proper_string("\x93\xdf\xdf\x23\xdf")
        self.assertEqual(actual_result, False)
        self.assertRaises(UnicodeDecodeError)

    @mock.patch('spark_streaming.is_proper_string')
    @mock.patch('spark_streaming.insert_into_tweets')
    @mock.patch('spark_streaming.json')
    def test_get_tweets_from_dictionary(self, json_mock, mock_insert, mock_proper_string):
        json_mock.loads.return_value = True
        mock_insert.return_value = None
        mock_proper_string.return_value = True

        spark_streaming.get_tweets_from_dictionary(time, Rdd())

        self.assertTrue(mock_insert.called)
        self.assertFalse(json_mock.loads.called)


if __name__ == '__main__':
    unittest.main()
