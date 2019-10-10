# coding: utf-8

from config import rabbit_config
from rabbit_client import *

while True:
    a=input('one more time#')
    message = 'message:' + a
    ret = send_amsg(rabbit_config['vhost'], message, exchange='exchange_topic', routing_key='route.g.g.g')
    print('ret={}'.format(ret))