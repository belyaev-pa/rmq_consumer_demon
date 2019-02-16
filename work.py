# -*- coding: utf-8 -*-
import json
import datetime
import ltps
import types
import requests
import time
import subprocess
import base64



class AgentJobHandler(object):

    def __init__(self, msg, log_file_path):
        """
        :param msg: - сообщение
        """
        self.log_file_path = log_file_path

        with open(self.log_file_path, 'a') as log_file:
            log_file.write("Received message at {}".format(datetime.datetime.now()))
            self.msg = json.loads(msg)
            self.msg_to = self.msg['msg_to']
            log_file.write("msg_to: {}".format(self.msg_to))
            self.msg_type = self.msg['msg_type']
            log_file.write("msg_type: {}".format(self.msg_type))
            self.msg_from = self.msg['msg_from']
            log_file.write("msg_from: {}".format(self.msg_from))
            self.time_out = self.msg['time_out']
            log_file.write("time_out: {}".format(self.time_out))
            self.task_id = self.msg['task_id']
            log_file.write("task_id: {}".format(self.task_id))
            self.msg_cmd = self.msg['msg_cmd']
            log_file.write("msg_cmd: {}".format(self.msg_cmd))
            self.msg_files = self.msg['msg_files']
            # may be write files names here
            self.date_init = self.msg['date_init']
            self.date_start = datetime.datetime.now()
            self.date_stop = None
            self.result = None
            self.reply_to = self.msg['reply_to']
            log_file.write("reply_to: {}".format(self.reply_to))
        self.handle_job()


    def handle_job(self):
        job_to_call = getattr(self, self.msg_type, None)
        #if isinstance(job_to_call, types.FunctionType):
        with open(self.log_file_path, 'a') as log_file:
            try:
                log_file.write("start handle job...")
                job_to_call()
            except:
                log_file.write("No job with name: {}...".format(self.msg_type))
                self.result = 1
                log_file.write("Return code: {}...".format(self.result))
            self.date_stop = datetime.datetime.now()
            log_file.write("Stop handling sending ticket: {}...".format(self.result))
        with open(self.log_file_path, 'r') as log_file:
            self.msg_files = base64.b64encode(log_file.read())
        ltps.LTPSSend(msg_to=self.msg_to,
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
                      reply_to=self.reply_to,
                      agent_reply=True,
                      )

    def proc(self):
        with open(self.log_file_path, 'a') as log_file:
            process = subprocess.Popen(self.msg_cmd,
                                       shell=True,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)

            for line in iter(process.stdout.readline, b''):
                log_file.write(line.strip())

    def vcard(self):
        time.sleep(10)
        self.result = 0
        self.msg_cmd = 'completed'

    def schedule(self):
        time.sleep(10)
        self.result = 0
        self.msg_cmd = 'completed'

    def control_aide(self):
        time.sleep(10)
        self.result = 0

    def control_avp(self):
        time.sleep(10)
        self.result = 0

    def whitelist(self):
        time.sleep(10)
        self.result = 0



class SBJobHandler(object):

    def __init__(self, msg, log_file_path):
        """
        :param msg: - сообщение
        """
        self.msg = msg
        self.log_file_path = log_file_path
        self.handle_job()


    def handle_job(self):
        url = "http://10.128.152.51:8000/rest/tasks/current-tasks/close/"
        headers = {'Content-type': 'application/json'}
        requests.put(url, data=self.msg, headers=headers)

