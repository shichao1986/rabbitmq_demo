# -*- coding: utf-8 -*-
"""
时间: 2019/10/9 13:40

作者: shichao

更改记录:

重要说明: 本模块用于各个模块单元测试或函数功能演示
"""
import json
import pika
import logging
import config
import random
import time
import threading
from rabbitmq_client import send_rabbitmq_message, FPC_MES_VHOST, FPC_MES_EXCHANGE

logger = logging.getLogger(config.LOGGER_NAME)

def config_rabbitmq():
    # 使用list配置rabbitmq
    exchange_config = [
        {
            'exchange': FPC_MES_EXCHANGE,
            'durable': True,
            'exchange_type': 'direct',  # 使用routingkey将消息发送给指定的queue
            'queues': [
                {
                    'queue': 'exchange_direct_q1',
                    'routing_key': 'data_report',
                    'durable': True,
                    'arguments': None
                }, {
                    'queue': 'exchange_direct_q2',
                    'routing_key': 'alarm_report',
                    'durable': True,
                    'arguments': None
                }, {
                    'queue': 'exchange_direct_q3',
                    'routing_key': 'device_state',
                    'durable': True,
                    'arguments': None
                }, {
                    'queue': 'exchange_direct_q4',
                    'routing_key': 'qr_code_notify',
                    'durable': True,
                    'arguments': None
                }, {
                    'queue': 'exchange_direct_q5',
                    'routing_key': 'sn_notify',
                    'durable': True,
                    'arguments': None
                }
            ]
        }
    ]

    credentials = pika.PlainCredentials(config.RABBITMQ_USERNAME, config.RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(host=config.RABBITMQ_HOST, port=config.RABBITMQ_PORT,
                                           virtual_host=FPC_MES_VHOST, credentials=credentials)
    try:
        connection = pika.BlockingConnection(parameters=parameters)
        channel1 = connection.channel()
        for exchange in exchange_config:
            channel1.exchange_declare(exchange=exchange['exchange'], exchange_type=exchange['exchange_type'],
                                      durable=exchange['durable'])

            for q in exchange['queues']:
                channel1.queue_declare(queue=q['queue'], durable=q['durable'])
                channel1.queue_bind(exchange=exchange['exchange'], queue=q['queue'], routing_key=q['routing_key'],
                                    arguments=q['arguments'])

        connection.close()
    except Exception as e:
        print(e)
        return -1

    return 0

def test_rabbitmq_client():
    message = json.dumps({"device_id": "123333",
                "version": "V01",
               "payload":{
                   "cam_id": "",
                   "sn_list": [
                       {"sn": "",
                        "state": 0,
                        "error": ""},
                       {"sn": "",
                        "state": 0,
                        "error": ""}
                   ]
               }})
    routing_keys = ['qr_code_notify','data_report', 'alarm_report', 'device_state', 'sn_notify']

    def thread_random_run(func):
        #模拟线程调用func，调用间隔为0-2秒内的随机值
        while True:
            try:
                func()
            except Exception as e:
                logger.error(e)
            time.sleep(random.random()*2)

    def send_random_message():
        routing_key = random.choice(routing_keys)
        send_rabbitmq_message(message, routing_key)

    for i in range(5):
        threading.Thread(target=thread_random_run, args=(send_random_message, ), daemon=True).start()

if __name__ == '__main__':
    FORMAT = '%(asctime)s %(levelname)s %(name)s %(filename)s %(funcName)s(), %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=FORMAT)
    # sys log output to console
    console = logging.StreamHandler()
    # console.setLevel(logging.INFO)
    logger.addHandler(console)
    logger.setLevel(logging.DEBUG)

    # config_rabbitmq()

    test_rabbitmq_client()

    while True:
        time.sleep(1)
