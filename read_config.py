from builtins import open

import yaml


class ReadConfig:
    """
    Class to read the config.yaml file data.
    """

    def __init__(self):
        """
        Constructor to open the config.yaml in read mode and loading the data in class variable
        """
        with open('config.yaml', 'r') as f:
            self.doc = yaml.safe_load(f)

    def get_access_token(self):
        """
        Method to fetch the access token from config.yaml
        :return: String, value of access token stored in config.yaml
        """
        return self.doc['development']['access_token']

    def get_access_secret(self):
        """
        Method to fetch the access secret from config.yaml
        :return: String, value of access secret stored in config.yaml
        """
        return self.doc['development']['access_secret']

    def get_consumer_key(self):
        """
        Method to fetch the customer key from config.yaml
        :return: String, value of customer key stored in config.yaml
        """
        return self.doc['development']['consumer_key']

    def get_consumer_secret(self):
        """
        Method to fetch the customer secret from config.yaml
        :return: String, value of customer secret stored in config.yaml
        """
        return self.doc['development']['consumer_secret']
