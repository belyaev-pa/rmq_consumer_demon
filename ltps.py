# -*- coding: utf-8 -*-
import pika
import json
import datetime
#from django.conf import settings
from pika import exceptions
from base_rabbit_connector import BaseRabbitMQ


class LTPSSend(BaseRabbitMQ):
    def __init__(self, msg_to, msg_type, msg_from, time_out, task_id,
                 msg_cmd=None, msg_files=None, date_init=None, date_start=None,
                 date_stop=None, result=None, reply_to=None, agent_reply=None):
        """
        для работы модуля нужно добавить в django settings.py следующее
        REPLY_TO = "reply_queue" название очереди
        RABBITMQ_HOST = '10.128.152.30' (ip адрес rabbit`a должно быть выставлено ansibl`ом)
        RABBITMQ_PORT = 5672 (порт работы рабита default = 5672)
        HEARTBEAT_INTERVAL = 600 (интервал серцебиения раббита)
        BLOCKED_CONNECTION_TIMEOUT = 300 (интервал остановки соединения клентом)
        :param msg_to:!To: Получатель сообщения
        :param msg_type:!Type: тип сообщения, определяющий какую функцию вызывать
        :param msg_from:!From: источник сообщения (отправитель)
        :param task_id:!Task_id: ID задачи породившей сообщение
        :param time_out:!Time_out: тйамаут выполнения задачи (сколько ждать)
        :param msg_cmd:~Cmd: комманды для выполнения на удаленном агенте
        :param msg_files:~Files: файлы сообщения или лог ошибки
        :param date_init: время порождения сообщения задачей
        :param date_start: время начала выполнения задачи агентом
        :param date_stop: время завершения выполнения задачи агентом
        :param result: результат выполнения задачи
        :param reply_to: наименование очереди для ответа
        :param agent_reply: указывает на то, что класс вызывает агент в json не записывается
        """
        self.msg_to = msg_to
        self.msg_type = msg_type
        self.msg_from = msg_from
        self.time_out = time_out
        self.task_id = task_id
        self.msg_cmd = msg_cmd
        self.msg_files = msg_files
        self.date_init = datetime.datetime.now() if date_init is None else date_init
        self.date_start = date_start
        self.date_stop = date_stop
        self.result = result
        self.reply_to = self.get_settings('REPLY_TO') if reply_to is None else reply_to
        self.agent_reply = False if agent_reply is None else True
        # если это агент безопасности то послать нужно по полю reply_to
        super(LTPSSend, self).__init__(self.get_queue_name)
        self.proceed()

    def proceed(self):
        self.make_message()
        self.connect()
        self.send()
        self.close_connect()

    def make_message(self):
        """
        формируем json
        для разворачивания строки времени в обратную сторону используй
        date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
        :return:
        """
        message = dict(msg_to=self.msg_to,
                       msg_type=self.msg_type,
                       msg_from=self.msg_from,
                       time_out=self.time_out,
                       task_id=self.task_id,
                       msg_cmd=self.msg_cmd,
                       msg_files=self.msg_files,
                       date_init=self.date_init,
                       date_start=self.date_start,
                       date_stop=self.date_stop,
                       result=self.result,
                       reply_to=self.reply_to)
        self.message = json.dumps(message, sort_keys=False, default=str)

    def send(self):
        """send message must be called after connect()"""
        self.channel.basic_publish(
            exchange='',
            routing_key=self.queue_name,
            body=self.message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )

    def close_connect(self):
        self.connection.close()

    @property
    def get_queue_name(self):
        return self.reply_to if self.agent_reply else self.msg_to


def send_message(msg_to, msg_type, msg_from, time_out, task_id, msg_cmd=None, msg_files=None):
    try:
        LTPSSend(msg_to, msg_type, msg_from, time_out, task_id, msg_cmd, msg_files)
    except exceptions.ConnectionClosed as e:
        return 'Connection error - {}'.format(e)
    except exceptions.ProbableAuthenticationError as e:
        return 'Authentication error - {}'.format(e)
    else:
        return 'OK'

if __name__ == "__main__":
    d = dict(
        msg_to = "vm_mail@che.ru.ab",
        msg_type = "proc",
        msg_from = "vm_sb@che.ru.sb",
        time_out = 30,
        task_id = "524816b1-72c6-446d-bcb0-c22f4879b200",
        msg_cmd = "ps aux | grep python",
    )
    daemon = send_message(**d)
