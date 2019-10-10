# -*- coding: utf-8 -*-
"""
时间: 2019/10/9 10:11

作者: shichao

更改记录:

重要说明:用模块提供全局到rabbitmq的单例，其他模块直接调用接口send_rabbitmq_message即可，消息的持久性（发送失败）
由调用者处理；

_reconnect：使用同步的方式连接rabbitmq，当rabbitmq工作异常时可能造成长时间的阻塞
_async_reconnect：使用异步的方式连接rabbitmq，不会因为在连接时产生阻塞而阻塞业务模块自身运行

备注：不论是blockingconnection 还是selectconnection，连接成功后都会将socket设置为非阻塞模式，所以
使用basicpublish发送消息时均不会造成阻塞

"""
import threading
import pika

import config
import logging

FPC_MES_VHOST = 'dev2mes'
FPC_MES_EXCHANGE = 'dev2mes_exchange'

logger = logging.getLogger(config.LOGGER_NAME)

credentials = pika.PlainCredentials(config.RABBITMQ_USERNAME, config.RABBITMQ_PASSWORD)
parameters = pika.ConnectionParameters(config.RABBITMQ_HOST, config.RABBITMQ_PORT, FPC_MES_VHOST, credentials, socket_timeout=3)
singleton_client_connection = None
singleton_client_channel = None
_in_connect = False
_lock = threading.Lock()
class InConnectionException(Exception):
    def __str__(self):
        return 'The main thread is connecting the rabbitmq host'


def _reconnect():
    '''
    因为连接rabbitmq时会存在阻塞（阻塞会导致系统进行线程切换），所以本函数需要考虑多线程调用时的线程安全问题
    我们期望处于连接过程中时，其他线程不要在进行连接，而是直接抛出异常
    :return:
    '''
    global singleton_client_connection
    global singleton_client_channel
    global _in_connect
    global _lock

    with _lock:
        if not _in_connect:
            _in_connect = True
            dispatch = 'do_connect'
        else:
            dispatch = 'raise_exception'

    if dispatch == 'do_connect':
        try:
            singleton_client_connection = pika.BlockingConnection(parameters)
            singleton_client_channel = singleton_client_connection.channel()
        finally:
            # 此处仅保证_in_connect一定被置为false，异常交给外层函数处理
            _in_connect = False
    else:
        raise InConnectionException

def _async_reconnect():
    '''
        相比同步的连接rabbitmq的方式，异步连接可以减少连接时由于rabbitmq本身的不响应，导致连接阻塞时间过长，
        进而产生影响系统业务的后果
        :return:
        '''
    global singleton_client_connection
    global singleton_client_channel
    global _in_connect
    global _lock

    with _lock:
        if not _in_connect:
            _in_connect = True
            dispatch = 'do_connect'
        else:
            dispatch = 'raise_exception'

    if dispatch == 'do_connect':
        def _on_open_callback(*args, **kwargs):
            global singleton_client_connection
            global singleton_client_channel
            global _in_connect

            def _on_channel_open(*args, **kwargs):
                global _in_connect
                _in_connect = False

            try:
                singleton_client_channel = singleton_client_connection.channel(_on_channel_open)
            except Exception as e:
                logging.error(e)
                _process_exception()
                _in_connect = False

        def _on_open_error_callback(*args, **kwargs):
            global _in_connect
            _process_exception()
            _in_connect = False

        def _rabbit_ioloop_process(connection):
            try:
                connection.ioloop.start()
            except Exception as e:
                logging.error(e)
                _process_exception()

        try:
            singleton_client_connection = pika.SelectConnection(parameters=parameters,
                                                                on_open_callback=_on_open_callback,
                                                                on_open_error_callback=_on_open_error_callback)
            threading.Thread(target=_rabbit_ioloop_process, args=(singleton_client_connection,)).start()

        except Exception as e:
            logging.error(e)
            # 开始异步连接失败时_in_connect被置为false，连接开始后由callback函数修改_in_connect
            _in_connect = False
    else:
        raise InConnectionException

def _process_exception():
    global singleton_client_connection
    global singleton_client_channel

    try:
        if singleton_client_channel:
            singleton_client_channel.close()
        if singleton_client_connection:
            singleton_client_connection.close()
    except Exception as e:
        logger.error('close rabbitmq connection failed:{}'.format(e))
    finally:
        singleton_client_channel = None
        singleton_client_connection = None

def send_rabbitmq_message(message: str, routing_key: str, durable: bool = True) -> tuple:
    '''
    :param message:
    :param routing_key:
    :param durable: 消息不需要持久化时设置为False，默认为True
    :return: boolean, 发送成功返回True，发送失败返回false
    '''
    global singleton_client_connection
    global singleton_client_channel

    ret = (True, 'OK')
    try:
        if not singleton_client_connection or singleton_client_connection.is_closed or not singleton_client_channel or\
                singleton_client_channel.is_closed:
            _async_reconnect()

        if singleton_client_channel:
            if durable:
                send_properties = pika.BasicProperties(delivery_mode=2)
            else:
                send_properties = None

            singleton_client_channel.basic_publish(exchange=FPC_MES_EXCHANGE,
                                           routing_key=routing_key,
                                           body=message,
                                           properties=send_properties)
        else:
            ret = (False, 'Connection is not ready')
    except InConnectionException as e:
        logger.warning(e)
        ret = (False, 'Connection is not ready')
    except Exception as e:
        logger.error(
            'send msg({}) to rabbitmq({}) port({}) vhost({}) exchange({}) routingkey({}) failed!'.format(message,
                                                                                                         config.RABBITMQ_HOST,
                                                                                                         config.RABBITMQ_PORT,
                                                                                                         FPC_MES_VHOST,
                                                                                                         FPC_MES_EXCHANGE,
                                                                                                         routing_key))
        logger.error('Exception:{}'.format(e))
        _process_exception()
        ret = (False, 'Exception error')

    return ret

