import os
import time
from datetime import datetime
from threading import Thread
from confluent_kafka import Producer


KAFKA_MESSAGE_TOPIC = os.getenv('KAFKA_MESSAGE_TOPIC')
KAFKA_SERVERS = os.getenv('KAFKA_SERVERS')
THREAD_NUM = int(os.getenv('THREAD_NUM', 10))
MAX_MESSAGE_NUM = int(os.getenv('MAX_MESSAGE_NUM', 100))

thread_count = 0


def deliver_callback(err, msg):
    nw = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if err:
        print '[%s] Failed to deliver message to kafka. Error: %s' % (nw, err)
    else:
        print '[%s] Succeeded to deliver message \"%s\" to %s [%d]' % (
            nw, msg.value(), msg.topic(), msg.partition()
        )


def send_message(msg):
    try:
        producer = Producer({'bootstrap.servers': KAFKA_SERVERS})
        producer.produce(KAFKA_MESSAGE_TOPIC, msg, callback=deliver_callback)
        producer.flush()
    except Exception as ex:
        print 'Failed to deliver message. Error: {} - {}'.format(type(ex), ex.message)


def multiple_run():
    start_time = datetime.now()

    def callback(thread_num):
        global thread_count

        cnt = 0
        while cnt < MAX_MESSAGE_NUM:
            time.sleep(0.01)
            cnt += 1

            nw = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            msg = 'Thread %d sent a new message %d at %s' % (thread_num, cnt, nw)
            send_message(msg)

            print 'Message: %s' % msg

        thread_count += 1

    count = 0
    while count < THREAD_NUM:
        Thread(target=callback, args=(count,)).start()

        print 'Thread %d started' % count
        count += 1

    while thread_count < THREAD_NUM:
        time.sleep(1)

    end_time = datetime.now()
    print 'Running time %ds' % (end_time - start_time).total_seconds()


multiple_run()
