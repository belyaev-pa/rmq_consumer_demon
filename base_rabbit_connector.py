# -*- coding: utf-8 -*-
import pika


class BaseRabbitMQ(object):

    def __init__(self, queue_name):
        self.queue_name = queue_name

    def connect(self):
        """
        connect to rabbitmq
        NOTE: prefetch is set to 1 here for test to keep the number of threads created
        to a reasonable amount. We can to test with different prefetch values
        to find which one provides the best performance and usability for your solution

        :return: None (void)
        """
        self.params = pika.ConnectionParameters(
            host=self.get_settings('RABBITMQ_HOST'),
            port=self.get_settings('RABBITMQ_PORT'),
            credentials=pika.credentials.PlainCredentials(self.principal, self.token),
            heartbeat_interval=self.get_settings('HEARTBEAT_INTERVAL'),
            blocked_connection_timeout=self.get_settings('BLOCKED_CONNECTION_TIMEOUT'),
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

    def get_settings(self, setting):
        try:
            import django
            prop = getattr(django.conf.settings, setting, None)
        except:
            import conf
            prop = getattr(conf, setting, None)
        if prop is None:
            raise SettingIsNoneException
        return prop

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

class SettingIsNoneException(Exception):
    def __init___(self, *args):
        Exception.__init__(self, "Can`t find {0} in django settings or config file".format(*args))