"""
Usage:

black Source/automation/test_kafka.py
pip3 install kafka-python
python3 Source/automation/test_kafka.py

"""

from threading import Thread
from time import sleep, time
import kafka
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

LOG_EVERY = 10000
BOOTSTRAP_SERVERS = ["lab-dwh-kafka-broker-illab1-001.illab1.cynet:6667"]


class ElapsedTime:
    def __init__(self):
        self.start = time()

    def __call__(self):
        return time() - self.start

def round_float(value, decimals=0):
    mul = 10**decimals
    return int(mul*value)/mul

class ThreadConsumer:
    def __init__(self, topics):
        self.connected = False
        self.msg_count = 0
        self.topics = topics

    def connect(self):
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id="test_avi",
            )
        except NoBrokersAvailable:
            print(f"Failed to connect to {BOOTSTRAP_SERVERS}")
            return False

        print(f"Connecting {self.consumer}")
        # See https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
        bootstrap_connected = self.consumer.bootstrap_connected()
        if not bootstrap_connected:
            return False

        return True

    def run(self):
        while True:
            if self.connect():
                break
            sleep(1)

        topics = self.consumer.topics()
        print(f"Connected: I see {len(topics)} topics: {list(topics)[:5]}")
        self.connected = True
        et = ElapsedTime()
        for message in self.consumer:
            self.msg_count += 1
            if not self.msg_count % LOG_EVERY:
                print(
                    f"I have received {self.msg_count} messages, rate {round_float(self.msg_count/et())} msg/s"
                )


class ThreadProducer:
    def __init__(self, topic):
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
        )
        self.topic = topic
        self.msg_count = 0

    def on_send_error(self, err):
        print(f"Failed to send {err}")

    def run(self):
        et = ElapsedTime()
        while True:
            msg = f"test {self.msg_count}".encode("utf-8")
            self.producer.send(self.topic, msg).add_errback(self.on_send_error)
            self.msg_count += 1
            if not self.msg_count % LOG_EVERY:
                self.producer.flush()
                print(f"I have sent {self.msg_count} messages, rate {round_float(self.msg_count/et())} msg/s")

            if consumer.msg_count < self.msg_count - LOG_EVERY:
                et_wait = ElapsedTime()
                while consumer.msg_count < self.msg_count:
                    sleep(0.1)
                print(
                    f"Waited {round_float(et_wait(), 2)}s for cosnumer to catch up"
                )


if __name__ == "__main__":
    print(kafka.__version__)

    topic_avi = "topic_test_avi"
    consumer = ThreadConsumer([topic_avi])
    Thread(target=consumer.run).start()
    while not consumer.connected:
        sleep(1.0)

    producer = ThreadProducer(topic_avi)
    Thread(target=producer.run).start()
    while True:
        sleep(1)
