# -*- coding: utf-8 -*-
import sys
import os
import time
import atexit
import signal
import pika
import threading
import functools
import conf
import syslog
import datetime
from pika import exceptions


class Demon:

    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        """
        конструктор демона
        :param pidfile: путь к pid файлу, обязательный агрумент
        :param stdin: путь к файлу для хранения stdin, по умолчанию никуда не сохраняет
        :param stdout: путь к файлу для хранения stdout, по умолчанию никуда не сохраняет
        :param stderr: путь к файлу для хранения stderr, по умолчанию никуда не сохраняет
        """
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        syslog.openlog(conf.LOG_NAME)
        syslog.syslog(syslog.LOG_INFO, '{} Initialising security agent demon'.format(datetime.datetime.now()))

    def daemonize(self):
        """
        производит UNIX double-form магию,
        Stevens "Advanced Programming in the UNIX Environment"
        for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, e:
            syslog.syslog(syslog.LOG_INFO, '{} fork #1 failed: {}'.format(datetime.datetime.now(), e.errno))
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, e:
            syslog.syslog(syslog.LOG_INFO, '{} fork #2 failed: {}'.format(datetime.datetime.now(), e.errno))
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

            # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        syslog.syslog(syslog.LOG_INFO, '{} writing pid file'.format(datetime.datetime.now()))
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write("%s\n" % pid)

    def delpid(self):
        syslog.syslog(syslog.LOG_INFO, '{} deleting pid file...'.format(datetime.datetime.now()))
        os.remove(self.pidfile)

    def handle_exit(self):

        self.channel.stop_consuming()
        self.connection.close()
        # Wait for all threads to complete
        for thread in self.threads:
            thread.join()
        self.delpid()
        syslog.syslog(syslog.LOG_INFO, '{} handled exit functions'.format(datetime.datetime.now()))

    def signal_assign(self):
        # assignee = SignalHandler()
        # for i in iter(self.sigDict):
        #     assignee.register(i, self.sigDict[i])
        signal.signal(signal.SIGTERM, self.handle_exit)

    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exist. Daemon already running?\n"
            syslog.syslog(syslog.LOG_INFO, '{} {}'.format(datetime.datetime.now(), message))
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

        # Start the daemon
        syslog.syslog(syslog.LOG_INFO, '{} starting demon process pid: {}'.format(datetime.datetime.now(), pid))
        self.daemonize()
        self.signal_assign()
        self.run()

    sigDict = {}

    def stop(self):
        """
        Stop the daemon
        """
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            syslog.syslog(syslog.LOG_INFO, '{} {}'.format(datetime.datetime.now(), message))
            sys.stderr.write(message % self.pidfile)
            return  # not an error in a restart

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, signal.SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        syslog.syslog(syslog.LOG_INFO, '{} restarting demon process'.format(datetime.datetime.now()))
        self.stop()
        self.start()

    def close_connect(self):
        self.channel.stop_consuming()
        self.connection.close()
        # Wait for all threads to complete
        for thread in self.threads:
            thread.join()

    def run(self):
        atexit.register(self.close_connect)
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

    def ack_message(self):
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

    def do_work(self, body):
        """
        функция выполнения работы
        # thread_id = threading.get_ident()
        # fmt1 = 'Thread id: {} Delivery tag: {} Message body: {}'
        # LOGGER.info(fmt1.format(thread_id, delivery_tag, body))
        # Sleeping to simulate 10 seconds of work (we need to code work here)
        """
        time.sleep(10)
        print body
        callback = functools.partial(self.ack_message)
        self.connection.add_callback_threadsafe(callback)

    def on_message(self, method_frame, header_frame, body):
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
        thr = threading.Thread(target=self.do_work, args=body)
        thr.start()
        self.threads.append(thr)

    @property
    def queue_name(self):
        """
        TODO: необходимо реализовать свойство возвращающее имя очереди
        :return: наименование очереди прослушиваемой демоном
        """
        return conf.QUEUE_NAME

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

