import socket
from builtins import print

from twitter import TwitterApp


class Processor:
    """
    Class to create the socket for listening from twitter and send the data to spark
    """

    TCP_IP = "localhost"
    TCP_PORT = 3000

    def __init__(self):
        """
        Constructor to create object of twitter app and calling the processor method.
        """
        self.twitter_app = TwitterApp()
        self.processor()

    def processor(self):
        """
        Method to make the app host socket connections that spark will connect with.
        """
        # declaring the connection object
        conn = None
        # initialising socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # setting socket to run on given ip and port
        s.bind((self.TCP_IP, self.TCP_PORT))
        # starting the socket
        s.listen(1)
        print("waiting for tcp connection...")
        # fetching conn and addr from socket using object destructuring
        conn, addr = s.accept()
        print("connected....Getting tweets.")
        # collecting response from twitter in twitter json object
        resp = self.twitter_app.get_tweets()
        # send the json object to spark
        self.twitter_app.send_tweets_to_spark(resp, conn)


if __name__ == '__main__':
    Processor()
