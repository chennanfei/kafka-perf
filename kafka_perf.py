import os
import time
from datetime import datetime
from threading import Thread
from confluent_kafka import Producer


KAFKA_MESSAGE_TOPIC = os.getenv('KAFKA_MESSAGE_TOPIC')
KAFKA_SERVERS = os.getenv('KAFKA_SERVERS')
THREAD_NUM = int(os.getenv('THREAD_NUM', 10))


def deliver_callback(err, msg):
    if err:
        print 'Failed to deliver message to kafka. Error: %s' % err
    else:
        print 'Succeeded to deliver message to %s [%d]' % (msg.topic(), msg.partition())


def send_message(msg):
    try:
        producer = Producer({'bootstrap.servers': KAFKA_SERVERS})
        producer.produce(KAFKA_MESSAGE_TOPIC, msg, callback=deliver_callback)
        producer.flush()
    except Exception as ex:
        print 'Failed to deliver message. Error: {} - {}'.format(type(ex), ex.message)

    time.sleep(0.05)


def multiple_run():
    def callback():
        nw = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = 'A new message is going to be send at %s' % nw
        send_message(msg)

    count = 0
    while count < THREAD_NUM:
        Thread(target=callback).start()

        print 'Thread %d started' % count
        count += 1


if __name__ == 'main':
    multiple_run()
