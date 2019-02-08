# -*- coding: utf-8 -*-
import pika
import json
import datetime
import ltps


class WorkCaller:

    def __init__(self, msg):
        """
        :param msg: - сообщение
        """

        self.msg = json.loads(msg)

        self.msg_to = self.msg['msg_to']
        self.msg_type = self.msg['msg_type']
        self.msg_from = self.msg['msg_from']
        self.time_out = self.msg['time_out']
        self.task_id = self.msg['task_id']
        self.msg_cmd = self.msg['msg_cmd']
        self.msg_files = self.msg['msg_files']
        self.date_init = self.msg['date_init']
        self.date_start = datetime.datetime.now()
        self.date_stop = None
        self.result = None
        self.reply_to = self.msg['reply_to']


