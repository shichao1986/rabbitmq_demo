# coding:utf-8

# 本模块负责对rabbitmq进行配置
# 业务应用应该直接对rabbitmq进行使用
# 统一配置rabbitmq便于管理

import sys
import pika
from pika.exceptions import ConnectionClosed

rabbit_config = {
    'user':'useruser',
    'password':'password',
    'host':'10.6.3.29',
    'port':5672,
    'vhost':'test-vhost'
}

# rabbitmq 全局配置，各个应用只需要关注自身使用的配置即可
exchange_config = [
    {
        'exchange':'exchange_direct',
        'durable':True,
        'exchange_type':'direct',
        'queues':[
            {
                'queue':'exchange_direct_q1',
                'routing_key':'rout_direct',
                'durable':True,
                'arguments':None
            },{
                'queue':'exchange_direct_q2',
                'routing_key':'rout_direct',
                'durable':False,
                'arguments':None
            },{
                'queue':'exchange_direct_q3',
                'routing_key':'rout_direct_2',
                'durable':True,
                'arguments':None
            }
        ]
    },{
        'exchange':'exchange_topic',
        'durable':True,
        'exchange_type':'topic',
        'queues':[
            {
                'queue':'exchange_topic_q1',
                'routing_key':'rout.topic.a',
                'durable':True,
                'arguments':None
            },{
                'queue':'exchange_topic_q2',
                'routing_key':'*.direct.a',
                'durable':True,
                'arguments':None
            },{
                'queue':'exchange_topic_q3',
                'routing_key':'route.*.*.#',
                'durable':True,
                'arguments':None
            }
        ]
    },{
        'exchange':'exchange_fanout',
        'durable':True,
        'exchange_type':'fanout',
        'queues':[
            {
                'queue':'exchange_fanout_q1',
                'routing_key':None,
                'durable':True,
                'arguments':None
            },{
                'queue':'exchange_fanout_q2',
                'routing_key':None,
                'durable':True,
                'arguments':None
            },{
                'queue':'exchange_fanout_q3',
                'routing_key':None,
                'durable':True,
                'arguments':None
            }
        ]
    },{
        'exchange':'exchange_header',
        'durable':True,
        'exchange_type':'headers',
        'queues':[
            {
                'queue':'exchange_headers_q1',
                'routing_key':None,
                'durable':True,
                'arguments':{
                    'a':1,
                    'b':'ccc',
                    'x-match':'all'
                }
            },{
                'queue':'exchange_headers_q2',
                'routing_key':None,
                'durable':True,
                'arguments':{
                    'a':1,
                    'b':'ddd',
                    'x-match':'any'
                }
            },{
                'queue':'exchange_headers_q3',
                'routing_key':None,
                'durable':True,
                'arguments':{
                    'a':2,
                    'b':'ddd',
                    'x-match':'all'
                }
            }
        ]
    }
]

def config_rabbit():
    credentials = pika.PlainCredentials(rabbit_config['user'], rabbit_config['password'])
    parameters = pika.ConnectionParameters(host=rabbit_config['host'], port=rabbit_config['port'],
                                           virtual_host=rabbit_config['vhost'], credentials=credentials)
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
    except ConnectionClosed as e:
        print(e)
        return -1

    return 0


def main():
    config_rabbit()

    return 0

if __name__ == '__main__':
    sys.exit(main())