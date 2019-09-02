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
        'exchange_type':'direct',   # 使用routingkey将消息发送给指定的queue
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
        'exchange_type':'topic',    #topic 使用模糊匹配的方式， 例如符号“#”匹配一个或多个词，符号“*”匹配不多不少
                                        # 一个词。因此“log.#”能够匹配到“log.info.oa”，但是“log.*” 只会匹配到“log.error”
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
        'exchange_type':'fanout',   # fanout 是广播，所有到该exchane的消息都会快速广播给下边的所有队列
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
        'exchange_type':'headers',      #header exchange(头交换机)和主题交换机有点相似，但是不同于主题交换机的路由是基
                                            # 于路由键，头交换机的路由值基于消息的header数据。
                                            #主题交换机路由键只有是字符串,而头交换机可以是整型和哈希值
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