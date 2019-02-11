# -*- coding: utf-8 -*-
import pika
import json
import datetime
import ltps
import types
import requests



class AgentJobHandler:

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
        self.handle_job()


    def handle_job(self):
        job_to_call = getattr(self, self.msg_type, None)
        if not isinstance(job_to_call, types.FunctionType):
            pass # make here to return result is bade type_func to suck function
        self.result = job_to_call()
        self.date_stop = datetime.datetime.now()
        ltps.LTPSSend(msg_to=self.msg_to,
                      msg_type=self.msg_type,
                      msg_from=self.msg_from,
                      time_out=self.time_out,
                      task_id=self.task_id,
                      msg_cmd=None,
                      msg_files=None,# добавить в self.files запись лога
                      date_init=self.date_init,
                      date_start=self.date_start,
                      date_stop=self.date_stop,
                      result=self.result,
                      reply_to=self.reply_to,
                      agent_reply=True)


class SBJobHandler:

    def __init__(self, msg):
        """
        :param msg: - сообщение
        """
        self.msg = msg
        self.handle_job()


    def handle_job(self):
        url = "http://localhost:8080/rest/tasks/current-tasks/close/"
        data = {'data': self.msg}
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        r = requests.post(url, data=json.dumps(data), headers=headers)

