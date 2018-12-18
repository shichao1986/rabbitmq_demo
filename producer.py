# coding:utf-8

# 本模块实现多种情景下的rabbitmq producer

import sys
import time
import pika
from config import rabbit_config
from consumer_thread import ConsumerThread

test_exchange = 'exchange_direct'

def ack_consumer_callback(channel, method, properties, body):
    # print('ack_callbask:{}'.format((channel, method, properties, body)))
    pass

# 初始化rabbitmq连接
def init_rabbitmq():
    credentials = pika.PlainCredentials(rabbit_config['user'], rabbit_config['password'])
    parameters = pika.ConnectionParameters(host=rabbit_config['host'], port=rabbit_config['port'],
                                           virtual_host=rabbit_config['vhost'], credentials=credentials)
    try:
        init_rabbitmq.connection = pika.BlockingConnection(parameters)
        init_rabbitmq.channel = init_rabbitmq.connection.channel()
        # passive = True 验证exchange是否存在，不存在抛出异常
        # init_rabbitmq.channel.exchange_declare(exchange=test_exchange, passive=True)
        init_rabbitmq.channel.basic_consume(consumer_callback=ack_consumer_callback,
                                            queue='amq.rabbitmq.reply-to',
                                            no_ack=True)
        # init_rabbitmq.channel.start_consuming()
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

# 重连rabbitmq
def reconnect_rabbitmq():
    return init_rabbitmq()

# 发送函数
def send_rabbitmq_message(message, exchange, routing_key, properties=None):
    channel = init_rabbitmq.channel
    try:
        # mandatory=False（找不到queue时直接丢弃，反之报错）, immediate=False（无consumer时排队，反之报错）
        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message, properties=properties)
    except Exception as e:
        print(e)
        print('try to reconnect rabbit...')
        if init_rabbitmq() == 0:
            print('reconnected!try to resend the message...')
            channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message, properties=properties)
            return 0
        else:
            print('reconnect failed!')
            return -1

    return 0

# 同步发送单条消息，发送完成后关闭连接
def message_producer1():
    # 发送消息到指定的rabbitmq，没有consumer时，消息会存储在rabbitmq
    # 重启rabbitmq， durable=True的队列会重新创建，而durable=False的队列会小时，
    # 两种情况下message都不会保存,仅当properties的delivery_mode=2时消息才会持久化
    # 如果消息发送时设定为持久化，而queue 或 exchange 未设置为持久化，则rabbitmq重启
    # 后，消息不会持久化保存

    # reply_to 的队列如果没有consumer 在相同的channel上，则该消息无法发送成功，同时会中断连接
    prop = pika.spec.BasicProperties(delivery_mode=2, reply_to='amq.rabbitmq.reply-to', correlation_id='1233')
    # prop = pika.spec.BasicProperties(delivery_mode=2)
    send_rabbitmq_message('hello world!', test_exchange, 'rout_direct', properties=prop)

# 异步发送单条消息，发送完成后关闭连接

# 同步发送单条消息，消息成功处理后关闭连接

# 异步发送单条消息，消息成功处理后关闭连接

# 异步发送多条消息，不验证处理，长连接

# 异步发送多条消息，验证处理，长连接

# 异步发送多条消息，验证处理，长连接，中断程序，验证数据持久性

# 初始化所有consumer
def init_consumer():
    reply_to_c = ConsumerThread(queue='amq.rabbitmq.reply-to', callback=ack_consumer_callback, no_ack=True)
    reply_to_c.start()

def main():
    init_rabbitmq()
    print('init_rabbitmq ok!')

    # init_consumer()
    # print('init_consumer ok!')

    time.sleep(5)

    # init_rabbitmq.channel.start_consuming()
    # close_rabbitmq()
    # print('close_rabbitmq ok!')
    while True:
        message_producer1()
        # print('message_producer1 ok!')
        init_rabbitmq.connection.process_data_events(time_limit=None)
        # time.sleep(1)


if __name__ == '__main__':
    sys.exit(main())