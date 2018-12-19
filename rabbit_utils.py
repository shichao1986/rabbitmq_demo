# coding:utf-8

# rabbitmq consumer 在独立的线程中运行

import time
import pika
import uuid
import threading

from config import rabbit_config

#consumer的 connection全局唯一，每一个线程有自己的channel
consumer_cond = threading.Condition()
producer_cond = threading.Condition()
channel_lock = threading.Lock()

# 线程安全装饰器，多线程调用时防止并发执行
def sychronized(func):
    func.__lock__ = threading.Lock()
    def inner(*args, **kwargs):
        with func.__lock__:
            return func(*args, **kwargs)
    return inner

def once(func):
    func.__lock__ = threading.Lock()
    def inner(*args, **kwargs):
        with func.__lock__:
            if not hasattr(func, '__once__'):#'__once__' not in func:
                func.__once__ = True
                return func(*args, **kwargs)
            else:
                # 不执行，直接返回
                return
    return inner

# conn_lock = threading.Lock()
# # kwargs 与 channel.basic_consume 中的kwargs定义一致
# def consumer_job(queue, callback, **kwargs):
#     global conn_lock
#     credentials = pika.PlainCredentials(rabbit_config['user'], rabbit_config['password'])
#     parameters = pika.ConnectionParameters(host=rabbit_config['host'], port=rabbit_config['port'],
#                                            virtual_host=rabbit_config['vhost'], credentials=credentials)
#     if conn_lock.acquire():
#         if 'conn' not in dir(consumer_job):
#             consumer_job.conn = None
#             consumer_job.reconn = False
#         conn_lock.release()
#     channel = None
#     while True:
#         try:
#             if conn_lock.acquire():
#                 if consumer_job.conn is None or consumer_job.conn.is_open is not True:
#                     consumer_job.conn = pika.BlockingConnection(parameters)
#                     print('{}:new connection {}'.format(threading.get_ident(),consumer_job.conn))
#                 conn_lock.release()
#             if channel is None or channel.is_open is not True:
#                 channel = consumer_job.conn.channel()
#                 channel.basic_consume(consumer_callback=callback, queue=queue, **kwargs)
#
#             # print('set consumer {},{}'.format(queue, kwargs))
#             while True:
#                 channel.start_consuming()
#                 # consumer_job.conn.process_data_events(time_limit=None)
#         except Exception as e:
#             print(e)
#             print('{}:try to reconnect to {}:{}'.format(threading.get_ident(), rabbit_config['host'], rabbit_config['port']))
#
#         # 连接失败，或者断开连接，3秒后重新连接
#         time.sleep(3)

# 一个线程一个connection 用于管理其下的所有channel
class ConsumerThread(threading.Thread):
    def __init__(self, rabbit_config):
        super(ConsumerThread, self).__init__()
        self.rabbit_host = rabbit_config['host']
        self.rabbit_port = rabbit_config['port']
        self.rabbit_user = rabbit_config['user']
        self.rabbit_pass = rabbit_config['password']
        self.rabbit_vhost = rabbit_config['vhost']
        self.cond = None
        self.conn = None
        self.channels_lock = threading.Lock()
        # 本例程不负责rabbitmq的配置，channels仅定义channel下的queue和callback的对应关系
        '''
        self.channels =
        {
            'channel': {
                'pika_channel': channel,
                'queues': [
                    {
                        'queue': 'queue_name1',
                        'callback': call_back1,
                        'kwargs': kwargs
                    }
                ],
                'qos': {
                    'prefetch_cnt': x,
                    'prefetch_size': x
                }
            }
        }
        '''
        self.channels = dict()
        self.setDaemon(True)

    def reconnect(self):
        credentials = pika.PlainCredentials(self.rabbit_user, self.rabbit_pass)
        parameters = pika.ConnectionParameters(host=self.rabbit_host, port=self.rabbit_port,
                                               virtual_host=self.rabbit_vhost, credentials=credentials)
        self.conn = pika.BlockingConnection(parameters)

        # with self.channels_lock:
        config_channels = self.channels
        self.channels = dict()
        for channelname, configuration in config_channels.items():
            if self.register_channel(channelname):
                for cfg in configuration['queues']:
                    self.consumer_set(cfg['queue'], cfg['callback'], channel_name=channelname, **cfg['kwargs'])
                    print('rebuild conn succeed:{}'.format(cfg))

    def run(self):
        while True:
            try:
                self.reconnect()
                print('{}:new connection {}'.format(threading.get_ident(), self.conn))
                self.clear_runflag()
                while True:
                    self.conn.process_data_events(time_limit=None)
            except Exception as e:
                print(e)
                print('{}:try to reconnect to {}:{}'.format(threading.get_ident(), self.rabbit_host, self.rabbit_port))
                # 重连间隔设定为3秒
                time.sleep(3)

    # 我一直纠结要不要提供默认channel，虽然这使得设计看上去完整，但是却带来了一个额外的风险
    # 当共享channel发生异常时，所有基于这个channel的订阅者将不复存在
    # 一切使用默认channel的业务需要自己完成channel的恢复，守护进程仅在发生重连时对正常的channel进行恢复
    def register_channel(self, channel_name='default_channel'):
        with self.channels_lock:
            if channel_name not in self.channels:
                try:
                    d = {}
                    d['pika_channel'] = self.conn.channel()
                    d['queues'] = []
                    self.channels[channel_name] = d
                except Exception as e:
                    print(e)
                    return False
            return True

    # 参数参考pika.channel.basic_qos(),未实现
    def channel_qos(self, channel_name='default_channel', **kwargs):
        pass

    def consumer_set(self, queue, callback, channel_name='default_channel', **kwargs):
        with self.channels_lock:
            if channel_name in self.channels:
                try:
                    self.channels[channel_name]['pika_channel'].basic_consume(consumer_callback=callback, queue=queue, **kwargs)
                    self.channels[channel_name]['queues'].append(dict(queue=queue, callback=callback, kwargs=kwargs))
                except Exception as e:
                    print('{}:{}'.format('consumer_set', e))
                    del self.channels[channel_name]
                    return False
                return True

            return False

    def publish(self, exchange, routing_key, body, channel_name='default_channel', callback=None, corr_id=None):
        channel = self.channels[channel_name]['pika_channel']
        if channel is not None and channel.is_open:
            try:
                if callback:
                    if corr_id is None:
                        print('corr_id is invalid!')
                        return False
                    prop = pika.BasicProperties(delivery_mode=2, reply_to='amq.rabbitmq.reply-to',
                                                correlation_id=corr_id)
                    channel.basic_publish(exchange=exchange, routing_key=routing_key, properties=prop, body=body)
                else:
                    channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)
            except Exception as e:
                print (e)
                return False
            return True
        return False

    # 用于同步初始化，保证守护进程启动后再设置consumer或发送消息
    def set_runflag(self, cond):
        self.cond = cond

    # 通知主线程connection就绪并清除同步初始化标志，防止在断线重连时调用
    def clear_runflag(self):
        if self.cond:
            with self.cond:
                self.cond.notify()
                self.cond = None

# 单例模式对ConsumerThread类进行封装
class ConsumerDaemon(object):
    @sychronized
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ConsumerDaemon, cls).__new__(cls)
        return cls.instance

    @once
    def __init__(self):
        global consumer_cond
        print('ConsumerDaemon init!id={}'.format(id(self)))
        self.consumer_thread = ConsumerThread(rabbit_config)
        self.consumer_thread.set_runflag(consumer_cond)
        self.consumer_thread.start()
        # 主线程等待子线程建立连接成功
        with consumer_cond:
            consumer_cond.wait()

    def register(self, channel_name='default_channel'):
        return self.consumer_thread.register_channel(channel_name=channel_name)

    def set(self, queue, callback, channel_name='default_channel', **kwargs):
        return self.consumer_thread.consumer_set(queue=queue, callback=callback, channel_name=channel_name, **kwargs)

class ProducerDaemon(object):
    @sychronized
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ProducerDaemon, cls).__new__(cls)
        return cls.instance

    @once
    def __init__(self):
        global producer_cond
        self.producer_thread = ConsumerThread(rabbit_config)
        self.producer_thread.set_runflag(producer_cond)
        self.producer_thread.start()
        self.response_callbacks = {}
        with producer_cond:
            producer_cond.wait()
        self.producer_thread.register_channel()
        self.producer_thread.consumer_set(queue='amq.rabbitmq.reply-to', callback=self.reply_response, no_ack=True)

    def reply_response(self, channel, method, properties, body):
        corr_id = properties.correlation_id
        # corr_id 为各个线程注册的唯一id，ProducerDaemon为不同的corr_id保存各自的函数
        if corr_id in self.response_callbacks:
            return self.response_callbacks[corr_id](channel, method, properties, body)
        else:
            print('response func missed!')
        return

    def publish(self, exchange, message, routing_key, callback=None, corr_id=None):
        if callback and corr_id:
            self.response_callbacks[corr_id] = callback
        elif callback:
            corr_id = uuid.uuid4()
            self.response_callbacks[corr_id] = callback

        return self.producer_thread.publish(exchange=exchange, routing_key=routing_key, body=message, callback=callback,
                                     corr_id=corr_id)