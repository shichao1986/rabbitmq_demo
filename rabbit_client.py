# -*- coding: UTF-8 -*-
"""
# Rabbit client API.
#
# Change History:
#   Time        Author      Content
#   2016-9-7    Liu Bin     Init the file.
#                           Implements the MessagePublisher class.
#
"""
# system modules
import logging
import threading
import uuid
import time

# third party modules
import pika

# user modules
import config
from config import rabbit_config

logger = logging.getLogger('rabbit-client')


class MessagePublisher(threading.Thread):
    """ The message publisher interface class.
    """

    def __init__(self, vhost):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.vhost = vhost
        self.response = {}
        self.connection = None
        self._setup_connection()
        self.stopped = True

    def _setup_connection(self):
        """ As publisher/consumer, it setup the rabbitmq broker connection.
        """
        credentials = pika.PlainCredentials(rabbit_config['user'],
                                            rabbit_config['password'])
        parameters = pika.ConnectionParameters(rabbit_config['host'],
                                               rabbit_config['port'],
                                               self.vhost,
                                               credentials)
        # noinspection PyBroadException
        try:
            self.connection = pika.BlockingConnection(parameters)
        except Exception as e:
            logger.error('Connect to broker(vhost %s) ... failed.' % self.vhost)
            return False

        if self.connection.is_open:
            self.channel = self.connection.channel()

            # bind RPC call back handler
            result = self.channel.queue_declare(exclusive=True)
            self.callback_queue = result.method.queue
            self.channel.basic_consume(self._on_response, no_ack=True, queue=self.callback_queue)
        return True

    def send(self, exchange, message, routing_key, call_back):
        """ As publisher, it sends message to a exchange with the routingKey.
        """
        logger.debug('send msg %s to exchange (%s) with key(%s)' % (message, exchange, routing_key))

        if self.connection is None or not self.connection.is_open:
            logger.error('send msg, but connection is closed.')
            return False

        if call_back:
            corr_id = str(uuid.uuid4())
            # self.response[corr_id] = None
            self.response[corr_id] = call_back
            basis = pika.BasicProperties(reply_to=self.callback_queue,
                                         correlation_id=corr_id,
                                         delivery_mode=2)
            self.channel.basic_publish(exchange=exchange,
                                       routing_key=routing_key,
                                       properties=basis,
                                       body=message)

        else:
            self.channel.basic_publish(exchange=exchange,
                                       routing_key=routing_key,
                                       body=message)
        return True

    # noinspection PyUnusedLocal
    def _on_response(self, channel, method, props, body):
        """ As publisher, the call back function to handle the response.
        """
        if props.correlation_id in self.response:
            self.response[props.correlation_id](body)
            self.response.pop(props.correlation_id)

    def stop(self):
        """ As publisher, it close the connection with server.
        """
        self.stopped = True
        self.connection.close()

    def run(self):
        """ As publisher, it needs to check & process connection events.
        """
        self.stopped = False
        while not self.stopped:
            if self.connection is not None and self.connection.is_open:
                # noinspection PyBroadException
                try:
                    self.connection.process_data_events(time_limit=None)
                except Exception as e:
                    logger.error('Publisher connection with broker closed.')
                    time.sleep(1)
                    continue
            else:
                if not self._setup_connection():
                    time.sleep(1)


publisher_vhosts = {}


def get_publisher(vhost):
    """ The function to get or create a publisher on given virtual host.
    :param vhost: string, the virtual host name.
    :return: MessagePublisher, the publisher connected with the vhost.
    """
    global publisher_vhosts
    if vhost in publisher_vhosts:
        return publisher_vhosts[vhost]
    else:
        publisher_vhosts[vhost] = MessagePublisher(vhost)
        publisher_vhosts[vhost].start()
        return publisher_vhosts[vhost]


def send_amsg(vhost,
              message,
              exchange='',
              routing_key='',
              call_back=None):
    """ Asyn message send interface.
    msg to queue(as publishier):
        routing_key:    queue name,
        call_back:      call back to handle the response
    msg to fanout exchange(as publishier):
        exchange:       exchange name
    msg to direct/topic exchange(as publishier):
        exchange:       exchange name
        routing_key:    routing key/topic
        call_back:      call back to handle the response
    :param vhost: string, the virtual host name.
    :param message: string, the json format message body.
    :param routing_key: string, broker routing key.
    :param call_back: callable function, the callback function when get response.
    :return: boolean, True(success),False(fail)
    """
    publisher = get_publisher(vhost)
    return publisher.send(exchange, message, routing_key, call_back)


def stop_publisher(vhost):
    """ Close the vhost publisher connection.
    """
    publisher = get_publisher(vhost)
    publisher.stop()


class MessageConsumer(threading.Thread):
    """ The message consumer interface class.
    """

    def __init__(self, vhost):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.vhost = vhost
        self.response = {}
        self.containers = []
        self.connection = None
        self._setup_connection()
        self.stopped = True

    def _setup_connection(self):
        """ As publisher/consumer, it setup the rabbitmq server connection.
        """
        credentials = pika.PlainCredentials(rabbit_config['user'],
                                            rabbit_config['password'])
        parameters = pika.ConnectionParameters(rabbit_config['host'],
                                               rabbit_config['port'],
                                               self.vhost,
                                               credentials)
        # noinspection PyBroadException
        try:
            self.connection = pika.BlockingConnection(parameters)
        except Exception as e:
            logger.error('Connect to broker(vhost %s) ... failed.' % self.vhost)
            return False

        if self.connection.is_open:
            self.channel = self.connection.channel()
        return True

    def _bind(self,
              exchange_name,
              exchange_type,
              exchange_durable,
              queue_name,
              queue_durable,
              routing_keys,
              consumer,
              no_ack):
        """ The function do the real binding operation.
        """
        if self.connection is None or not self.connection.is_open:
            logger.error('Try to bind container, but connection is not opened.')
            return False

        if exchange_type is None:
            # msg to queue prefetch=1
            if queue_name is None:
                logger.error('Bind message port failed, missing queue_name')
                return False
            self.channel.queue_declare(queue=queue_name, durable=queue_durable)
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(consumer_callback=consumer,
                                       queue=queue_name,
                                       no_ack=no_ack)
        else:
            # msg to exchange
            if exchange_name is None:
                logger.error('Bind message port failed, missing exchange_name')
                return False
            if exchange_type == 'fanout':
                # broadcast msg to all queues
                self.channel.exchange_declare(exchange=exchange_name,
                                              exchange_type=exchange_type,
                                              durable=exchange_durable)
                result = self.channel.queue_declare(exclusive=True)
                queue_name = result.method.queue
                self.channel.queue_bind(exchange=exchange_name,
                                        queue=queue_name)
                self.channel.basic_consume(consumer_callback=consumer,
                                           queue=queue_name,
                                           no_ack=True)
            elif exchange_type == 'direct' or exchange_type == 'topic':
                if routing_keys is None:
                    logger.error('Bind message port failed, missing routing_keys')
                    return False
                self.channel.exchange_declare(exchange=exchange_name,
                                              exchange_type=exchange_type,
                                              durable=exchange_durable)
                result = self.channel.queue_declare(exclusive=True)
                queue_name = result.method.queue
                for key in routing_keys:
                    self.channel.queue_bind(exchange=exchange_name,
                                            queue=queue_name,
                                            routing_key=key)
                self.channel.basic_consume(consumer_callback=consumer,
                                           queue=queue_name,
                                           no_ack=no_ack)
        return True

    # noinspection PyPep8
    def bind(self, exchange_name, exchange_type, exchange_durable, queue_name, queue_durable, routing_keys, consumer,
             no_ack):
        """ As consumer, it binds a queue to a exchange by key.
        """
        container = {'exchange_name': exchange_name,
                     'exchange_type': exchange_type,
                     'exchange_durable': exchange_durable,
                     'queue_name': queue_name,
                     'queue_durable': queue_durable,
                     'routing_keys': routing_keys,
                     'consumer': consumer,
                     'no_ack': no_ack}
        self.containers.append(container)
        container_id = self.containers.index(container)
        logger.info('Server bind container(%d) (exchange, routing key, queue)(%s:%s:%s)' % (container_id,
                                                                                            exchange_name,
                                                                                            routing_keys,
                                                                                            queue_name))

        return self._bind(exchange_name,
                          exchange_type,
                          exchange_durable,
                          queue_name,
                          queue_durable,
                          routing_keys,
                          consumer, no_ack)

    # noinspection PyTypeChecker
    def _rebind(self):
        """ As consumer, it rebinds the registered containers.
        """
        logger.info('rebind all containers')

        if self.connection is None or not self.connection.is_open:
            logger.error('Try to rebind containers, but connection is not opened.')
            return False

        for container in self.containers:
            self._bind(container['exchange_name'],
                       container['exchange_type'],
                       container['exchange_durable'],
                       container['queue_name'],
                       container['queue_durable'],
                       container['routing_keys'],
                       container['consumer'],
                       container['no_ack'])
        return True

    def run(self):
        """ As consumer, it blocks the calling thread.
        """
        logger.info('Start to consume %d containers' % len(self.containers))
        self.stopped = False
        while not self.stopped:
            if self.connection is None or not self.connection.is_open:
                if not self._setup_connection():
                    time.sleep(1)
                    continue
                self._rebind()
            # noinspection PyBroadException
            try:
                self.channel.start_consuming()
            except Exception as e:
                logger.error('Consumer connection with broker closed.')
                time.sleep(1)
        return True

    def stop(self):
        """ As consumer, it disconnects a consumer with a queue.
        """
        self.stopped = True
        # self.channel.basic_cancel()
        self.connection.close()
        return True

    def msg_ack(self, method):
        """ As consumer, it ack the receive of the message.
        This method must be called in the registered on_request function
        if the noAck is False.
        """
        if self.connection is None or not self.connection.is_open:
            logger.error('Send consumer ack, but connection is not opened.')
            return False

        self.channel.basic_ack(delivery_tag=method.delivery_tag)

    # noinspection PyUnusedLocal
    def msg_response(self, method, props, body):
        """ As consumer, it sends the response back to the original RPC queue.
        """
        if self.connection is None or not self.connection.is_open:
            logger.error('Send response, but connection is not opened.')
            return False

        if props.reply_to is None:
            logger.info('Consumer respond to source queue without to')
            return True
        else:
            logger.debug('Consumer send response msg %s' % body)
            self.channel.basic_publish(exchange='',
                                       routing_key=props.reply_to,
                                       properties=pika.BasicProperties(correlation_id=props.correlation_id),
                                       body=body)
        return True


consumer_vhosts = {}


def get_consumer(vhost):
    """ The function to get or create a publisher on given virtual host.
    """
    global consumer_vhosts
    if vhost in consumer_vhosts:
        return consumer_vhosts[vhost]
    else:
        consumer_vhosts[vhost] = MessageConsumer(vhost)
        return consumer_vhosts[vhost]


def config_consumer(vhost,
                    consumer,
                    exchange_name=None,
                    exchange_type=None,
                    exchange_durable=False,
                    queue_name=None,
                    queue_durable=False,
                    routing_keys=None,
                    no_ack=True):
    """ The function to bind a message server point.
    vhost and consumer are must parameters.
    Others:
    msg to queue(as consumer):
        queue_name:         queue name
        queue_durable:      queue durable
        no_ack:             no ack flage
    msg to fanout exchange(as consumer):
        exchange_name:      exchange name
        exchange_type:      fanout
        exchange_durable:   exchange durable
    msg to direct/topic exchange(as consumer):
        exchange_name:      exchange name
        exchange_type:      direct/topic
        exchange_durable:   exchange durable
        routing_keys:       routing keys,
        no_ack:             no ack flag.
    """
    msg_consumer = get_consumer(vhost)
    if not msg_consumer.bind(exchange_name, exchange_type, exchange_durable, queue_name, queue_durable, routing_keys,
                             consumer, no_ack):
        return False
    else:
        return True


def start_consumer(vhost):
    """ The function starts the message consumer
    """
    msg_consumer = get_consumer(vhost)
    return msg_consumer.start()


def stop_consumer(vhost):
    """ The function stops the message consumer
    """
    msg_consumer = get_consumer(vhost)
    return msg_consumer.stop()
