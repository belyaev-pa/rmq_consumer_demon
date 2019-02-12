# -*- coding: utf-8 -*-
import pika
import threading
import functools
import conf
import syslog
import datetime
import work
from pika import exceptions


class RabbitMQListener:

    def __init__(self, queue_name, log_name, ab_sb):
        """
        конструктор демона
        :param pidfile: путь к pid файлу, обязательный агрумент
        :param stdin: путь к файлу для хранения stdin, по умолчанию никуда не сохраняет
        :param stdout: путь к файлу для хранения stdout, по умолчанию никуда не сохраняет
        :param stderr: путь к файлу для хранения stderr, по умолчанию никуда не сохраняет
        """
        # self.stdin = stdin
        # self.stdout = stdout
        # self.stderr = stderr
        # self.pidfile = pidfile
        self.queue_name = queue_name
        self.log_name = log_name
        self.ab_sb = ab_sb
        syslog.openlog(self.log_name)
        syslog.syslog(syslog.LOG_INFO, '{} Initialising security agent demon'.format(datetime.datetime.now()))
        self.run()

    def close_connect(self):
        self.channel.stop_consuming()
        self.connection.close()
        # Wait for all threads to complete
        for thread in self.threads:
            thread.join()

    def run(self):
        while True:
            try:
                self.connect()
                self.threads = []
                on_message_callback = functools.partial(self.on_message)
                self.channel.basic_consume(on_message_callback,
                                           queue=self.queue_name)
                self.channel.start_consuming()
            except exceptions.ConnectionClosed:
                # Wait for all threads to complete
                for thread in self.threads:
                    thread.join()
                # write log here


    def connect(self):
        """
        NOTE: prefetch is set to 1 here for test to keep the number of threads created
        to a reasonable amount. We can to test with different prefetch values
        to find which one provides the best performance and usability for your solution

        :return: None (void)
        """
        self.params = pika.ConnectionParameters(
            host=conf.RABBITMQ_HOST,
            port=conf.RABBITMQ_PORT,
            credentials=pika.credentials.PlainCredentials(self.principal, self.token),
            heartbeat_interval=conf.HEARTBEAT_INTERVAL,
            blocked_connection_timeout=conf.BLOCKED_CONNECTION_TIMEOUT,
        )
        self.connection = pika.BlockingConnection(
            parameters=self.params,
        )
        self.channel = self.connection.channel()
        self.queue = self.channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            exclusive=False,
            auto_delete=False,
        )
        self.channel.basic_qos(prefetch_count=1)


    def ack_message(self, ch, delivery_tag):
        """
        функция возврата ack для RabbitMQ
        Note that `channel` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if self.channel.is_open:
            self.channel.basic_ack(self.delivery_tag)
        else:
            # Channel is already closed, so we can't ACK this message;
            # log and/or do something that makes sense for your app in this case.
            pass


    def do_work(self, conn, ch, delivery_tag, body):
        """
        функция выполнения работы
        # thread_id = threading.get_ident()
        # fmt1 = 'Thread id: {} Delivery tag: {} Message body: {}'
        # LOGGER.info(fmt1.format(thread_id, delivery_tag, body))
        # Sleeping to simulate 10 seconds of work (we need to code work here)
        """
        # time.sleep(10)
        if self.ab_sb == 'ab':
            work.AgentJobHandler(body)
        elif self.ab_sb == 'sb':
            work.SBJobHandler(body)
        callback = functools.partial(self.ack_message, ch, delivery_tag)
        self.connection.add_callback_threadsafe(callback)


    def on_message(self, ch, method_frame, header_frame, body):
        """
        функция выполняемая при получении сообщения
        формирует поток обработки сообщения, при этом поддерживая
        соединения с RabbitMQ
        example from https://github.com/pika/pika/blob/master/examples/basic_consumer_threaded.py
        :param method_frame: метод (мета параметры используемые rabbitmq)
        :param header_frame: заголовки пакета AMQP
        :param body: сообщение
        :return: void
        """
        self.delivery_tag = method_frame.delivery_tag
        thr = threading.Thread(target=self.do_work, args=(self.connection, ch, self.delivery_tag, body))
        thr.start()
        self.threads.append(thr)


    # @property
    # def queue_name(self):
    #     """
    #     TODO: необходимо реализовать свойство возвращающее имя очереди
    #     :return: наименование очереди прослушиваемой демоном
    #     """
    #     return conf.QUEUE_NAME

    @property
    def principal(self):
        """
        не будет участвовать в аутентификации поэтому может быть любым
        :return: principal пользователя
        """
        return 'guest'


    @property
    def token(self):
        """
        TODO: необходимо добавить SPNEGO аутентификацию (генерацию токена) сюда
        :return: GSSAPI token (либо пароль в тестовой среде)
        """
        return 'guest'


if __name__ == "__main__":
    daemon = RabbitMQListener(conf.QUEUE_NAME, conf.LOG_NAME, 'ab')