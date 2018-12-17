# coding:utf-8

# rabbitmq consumer 在独立的线程中运行

import time
import pika
import threading

from config import rabbit_config

#consumer的 connection全局唯一，每一个线程有自己的channel
conn_lock = threading.Lock()

# kwargs 与 channel.basic_consume 中的kwargs定义一致
def consumer_job(queue, callback, **kwargs):
    global conn_lock
    credentials = pika.PlainCredentials(rabbit_config['user'], rabbit_config['password'])
    parameters = pika.ConnectionParameters(host=rabbit_config['host'], port=rabbit_config['port'],
                                           virtual_host=rabbit_config['vhost'], credentials=credentials)
    if conn_lock.acquire():
        if 'conn' not in dir(consumer_job):
            consumer_job.conn = None
            consumer_job.reconn = False
        conn_lock.release()
    channel = None
    while True:
        try:
            if conn_lock.acquire():
                if consumer_job.conn is None or consumer_job.conn.is_open is not True:
                    consumer_job.conn = pika.BlockingConnection(parameters)
                    print('{}:new connection {}'.format(threading.get_ident(),consumer_job.conn))
                conn_lock.release()
            if channel is None or channel.is_open is not True:
                channel = consumer_job.conn.channel()
                channel.basic_consume(consumer_callback=callback, queue=queue, **kwargs)

            # print('set consumer {},{}'.format(queue, kwargs))
            while True:
                # channel.start_consuming()
                consumer_job.conn.process_data_events(time_limit=None)
        except Exception as e:
            print(e)
            print('{}:try to reconnect to {}:{}'.format(threading.get_ident(), rabbit_config['host'], rabbit_config['port']))

        # 连接失败，或者断开连接，3秒后重新连接
        time.sleep(3)


class ConsumerThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(ConsumerThread, self).__init__(target=consumer_job, args=args, kwargs=kwargs)
        # self.queue = queue
        # self.kwargs = kwargs
        self.setDaemon(True)

