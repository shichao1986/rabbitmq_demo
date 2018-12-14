# coding:utf-8

# 本模块实现多种情景下的rabbitmq consumer

import sys
import pika
from config import rabbit_config

test_exchange = 'exchange_direct'

# 初始化rabbitmq连接
def init_rabbitmq():
    credentials = pika.PlainCredentials(rabbit_config['user'], rabbit_config['password'])
    parameters = pika.ConnectionParameters(host=rabbit_config['host'], port=rabbit_config['port'],
                                           virtual_host=rabbit_config['vhost'], credentials=credentials)
    try:
        init_rabbitmq.connection = pika.BlockingConnection(parameters)
        init_rabbitmq.channel = init_rabbitmq.connection.channel()
        # passive = True 验证exchange是否存在，不存在抛出异常
        init_rabbitmq.channel.exchange_declare(exchange=test_exchange, passive=True)
        init_rabbitmq.channel.basic_consume(consumer_callback=message_consumer1,
                                            queue='exchange_direct_q1',
                                            exclusive=True,consumer_tag='1',arguments=dict(a=123))
        init_rabbitmq.channel.start_consuming()
    except Exception as e:
        print(e)
        return -1

    return 0

def close_rabbitmq():
    try:
        init_rabbitmq.connection.close()
    except Exception as e:
        print(e)

    return

# message_producer1() 的consumer
def message_consumer1(channel, method, properties, body):
    import pdb;pdb.set_trace()
    print('message_consumer1:')
    print('channel={}'.format(channel))
    print('method={}'.format(method))
    print('properties={}'.format(properties))
    print('body={}'.format(body))

    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    init_rabbitmq()

    print('init finished!')
    # while True:
    #     import time
    #     time.sleep(1)

    # close_rabbitmq()
    return 0

if __name__ == '__main__':
    main()