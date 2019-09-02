# -*- coding: utf-8 -*-
"""
时间: 2019/9/2 13:35

作者: shichao

更改记录:

重要说明:
"""

import logging
import time
import pika
from pythonjsonlogger import jsonlogger
from python_logging_rabbitmq import RabbitMQHandler

# all log output to file
FORMAT = '%(asctime)s %(levelname)s %(name)s %(filename)s %(funcName)s(), %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)

# sys log output to console
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(filename)s %(funcName)s(), %(message)s')
console.setFormatter(formatter)


logger = logging.getLogger('iot-mgmt')
logger.addHandler(console)

# sys log to rabbitmq
rabbit = RabbitMQHandler(
    host='10.6.3.29',
    port=5672,
    username='useruser',
    password='password',
    exchange='exchange_topic',
    declare_exchange=True,
    connection_params={
        'virtual_host': 'test-vhost',
        'connection_attempts': None,
        'socket_timeout': 5000
    },
    fields={
        'appname': 'logger_test',
        'hostip': 'localhost',
    }
)

formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime) %(name) %(processName) %(filename)  %(funcName) %(levelname) %(lineno) %(module) %(threadName) %(message)")
rabbit.setFormatter(formatter)
rabbit.setLevel(logging.DEBUG)

logger.addHandler(rabbit)

# credentials = pika.PlainCredentials('useruser', 'password')
# parameters = pika.ConnectionParameters(host='10.6.3.29', port=5672,
#                                        virtual_host='test-vhost', credentials=credentials)
# try:
#     connection = pika.BlockingConnection(parameters=parameters)
#     channel1 = connection.channel()
#     channel1.queue_declare(queue='exchange_topic-queue', durable=True)
#     channel1.queue_bind(exchange='exchange_topic', queue='exchange_topic-queue', routing_key='*.*',
#                                     arguments=None)
# except Exception as e:
#     print('error:{}'.format(e))
#


while  True:

    cmd = input('#')
    logger.debug('{}'.format(cmd))

