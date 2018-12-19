# coding:utf-8

# 本模块实现多种情景下的rabbitmq producer

import sys
import time
from rabbit_utils import ProducerDaemon

test_exchange = 'exchange_direct'

def ack_consumer_callback(channel, method, properties, body):
    print('ack_callbask:{}'.format((channel, method, properties, body)))
    pass


# 同步发送单条消息，发送完成后关闭连接

# 异步发送单条消息，发送完成后关闭连接

# 同步发送单条消息，消息成功处理后关闭连接

# 异步发送单条消息，消息成功处理后关闭连接

# 异步发送多条消息，不验证处理，长连接

# 异步发送多条消息，验证处理，长连接

# 异步发送多条消息，验证处理，长连接，中断程序，验证数据持久性

def main():
    p = ProducerDaemon()
    while True:
        p.publish(test_exchange, 'hello world!', 'rout_direct', callback=ack_consumer_callback, corr_id='p1')
        p.publish(test_exchange, 'hello world!2', 'rout_direct')
        time.sleep(2)


if __name__ == '__main__':
    sys.exit(main())