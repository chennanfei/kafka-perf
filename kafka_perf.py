import os
import time
from datetime import datetime
from threading import Thread
from confluent_kafka import Producer


FILE_LOOP_SIZE = int(os.getenv('FILE_LOOP_SIZE', 3))
KAFKA_MESSAGE_TOPIC = os.getenv('KAFKA_MESSAGE_TOPIC')
KAFKA_SERVERS = os.getenv('KAFKA_SERVERS')
THREAD_NUM = int(os.getenv('THREAD_NUM', 2))
MAX_MESSAGE_NUM = int(os.getenv('MAX_MESSAGE_NUM', 5))

thread_count = 0


def read_file():
    file_handler = open('test.log', 'r')
    try:
        file_content = file_handler.read()
        cnt = 0
        content = ''
        while cnt < FILE_LOOP_SIZE:
            cnt += 1
            content += file_content
        return content
    finally:
        file_handler.close()


def deliver_callback(err, msg):
    nw = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if err:
        print '[%s] Failed to deliver message to kafka. Error: %s' % (nw, err)
    else:
        print '[%s] Succeeded to deliver message to %s [%d]' % (
            nw, msg.topic(), msg.partition()
        )


def send_message(msg):
    try:
        producer = Producer({'bootstrap.servers': KAFKA_SERVERS})
        producer.produce(KAFKA_MESSAGE_TOPIC, msg, callback=deliver_callback)
        producer.flush()
    except Exception as ex:
        print 'Failed to deliver message. Error: {} - {}'.format(type(ex), ex.message)


def multiple_run():
    file_content = read_file()
    start_time = datetime.now()

    def callback(thread_num):
        global thread_count

        cnt = 0
        while cnt < MAX_MESSAGE_NUM:
            time.sleep(0.01)
            cnt += 1

            nw = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            msg = 'New message %s was sent by thread %d - %d at %s' % (
                file_content, thread_num, cnt, nw
            )
            send_message(msg)

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
