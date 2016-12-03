import os
import time
from datetime import datetime
from threading import Thread
from confluent_kafka import Producer


KAFKA_MESSAGE_TOPIC = os.getenv('KAFKA_MESSAGE_TOPIC')
KAFKA_SERVERS = os.getenv('KAFKA_SERVERS')
THREAD_NUM = int(os.getenv('THREAD_NUM', 10))
MAX_MESSAGE_NUM = int(os.getenv('MAX_MESSAGE_NUM', 100))


def deliver_callback(err, msg):
    nw = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if err:
        print '[%s] Failed to deliver message to kafka. Error: %s' % (nw, err)
    else:
        print '[%s] Succeeded to deliver message to %s [%d]' % (nw, msg.topic(), msg.partition())


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
        cnt = 0
        while cnt < MAX_MESSAGE_NUM:
            cnt += 1

            nw = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            msg = 'A new message %d is going to be sent at %s' % (cnt, nw)
            send_message(msg)

    count = 0
    while count < THREAD_NUM:
        Thread(target=callback).start()

        print 'Thread %d started' % count
        count += 1


multiple_run()
