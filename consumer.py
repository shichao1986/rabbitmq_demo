# coding:utf-8

# 本模块实现多种情景下的rabbitmq consumer

import sys
import time
import pika
from config import rabbit_config
from rabbit_utils import ConsumerDaemon

test_exchange = 'exchange_direct'

# message_producer1() 的consumer
def message_consumer1(channel, method, properties, body):
    # import pdb;pdb.set_trace()
    try:
        # print('message_consumer1:')
        # print('channel={}'.format(channel))
        # print('method={}'.format(method))
        # print('properties={}'.format(properties))
        # print('body={}'.format(body))
        pass
    finally:
        if properties.reply_to:
            channel.basic_publish(exchange='', routing_key=properties.reply_to,
                                  properties=pika.BasicProperties(correlation_id=properties.correlation_id), body='222')
        channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    c = ConsumerDaemon()
    c.register(channel_name='ch1')
    c.set(queue='exchange_direct_q1', callback=message_consumer1, channel_name='ch1', consumer_tag='2', arguments=dict(a=123))

    # c = ConsumerDaemon()
    # c.register(channel_name='ch2')
    # c.set(queue='exchange_direct_q2', callback=message_consumer1, channel_name='ch2', consumer_tag='333', arguments=dict(a=123))

    print('init finished!')
    while True:
        time.sleep(1)

    # close_rabbitmq()
    return 0

if __name__ == '__main__':
    sys.exit(main())