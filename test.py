# coding: utf-8

import pika
# from config import rabbit_config
# from rabbit_client import *
import time

# count = 1
# repeat = 0
# def handle_msg(channel, method, properties, body):
#     global  count, repeat
#     # print('message_consumer1:')
#     # print('channel={}'.format(channel))
#     # print('method={}'.format(method))
#     # print('properties={}'.format(properties))
#     # print('body={}'.format(body))
#     print('count={}'.format(count))
#     if count == 0:
#         repeat += 1
#     if repeat % 2 == 0 and count == 0:
#         count = 1
#     100 / count
#     count = (count + 1) % 10
#     if properties.reply_to:
#         channel.basic_publish(exchange='', routing_key=properties.reply_to,
#                               properties=pika.BasicProperties(correlation_id=properties.correlation_id), body='222')
#
#
# config_consumer(rabbit_config['vhost'],
#                        handle_msg,
#                        exchange_name='exchange_topic',
#                        exchange_type='topic',
#                        exchange_durable=True,
#                        routing_keys=['route.*.*.#'])
#
# start_consumer(rabbit_config['vhost'])
#
# while True:
#     time.sleep(1)


credentials = pika.PlainCredentials('useruser','password')
connection = pika.BlockingConnection(pika.ConnectionParameters(
'10.6.3.29',5672,'test-vhost',credentials))
channel = connection.channel()

# 声明queue
# channel.queue_declare(queue='balance')

# n RabbitMQ a message can never be sent directly to the queue, it always needs to go through an exchange.
while True:
    channel.basic_publish(exchange='exchange_direct',
                  routing_key='rout_direct',
                  body='Hello World!')
    time.sleep(0.1)
print(" [x] Sent 'Hello World!'")
connection.close()