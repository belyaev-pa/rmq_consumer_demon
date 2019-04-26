# -*- coding: utf-8 -*-
import os
import sys
import json
import subprocess
import re
import syslog
from datetime import datetime
from base_db import BaseDB
from collections import OrderedDict


class SingletonMeta(type):
    def __init__(cls, *args, **kwargs):
        cls._instance = None
        cls.get_instance = classmethod(lambda c: c._instance)
        super(SingletonMeta, cls).__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instance


class JobHandler(BaseDB):
    """
    Класс необходимо создавать через контекстный менеджер, для его корректного завершения!
    """
    __metaclass__ = SingletonMeta

    def __init__(self, job_id, conf_dict):
        """
        Конструктор обработчика заданий
        :param job_id: id задачи из БД, которую нужно выполнить
        :param conf_dict: словарь с настройками
        :param arguments: список аргументов, файлы и пути к ним: ['log_txt_file=/home/pavel/test_log.txt']
        :param manager_type: тип меджера, который запустил функцию (net или local)
        """
        self.job_id = job_id
        super(JobHandler, self).__init__(conf_dict)
        syslog.openlog(self.get_settings('LOG_NAME'))
        self.job_handling_error = self.get_job_handling_error
        self.job_type = self.get_job_type
        self.job_files = self.make_job_files_dict()
        self.completed_step = self.get_completed_steps()
        with open(self.get_settings('JOB_JSON_CONF_PATH')) as conf:
            json_conf = json.load(conf)
            self.job = json_conf.get(self.job_type, None)
            key = self.job.get('job', None).get('handling', None)
            self.dict_steps = OrderedDict(key)
        if self.job is None or key is None:
            raise WrongJsonFormatException("Wrong handling json file format in section: {0}".format(self.job))

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.job_handling_error:
            self.make_system_reverse()
        super(JobHandler, self).__exit__(exc_type, exc_val, exc_tb)

    def run_job(self):
        """
        вызывает job_handler для каждого шага
        парсим дату
        date_time_obj = datetime.datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S.%f")

        :return:
        """
        print(self.job['job']['files']['count'])
        print(len(self.job_files.keys()))
        if int(self.job['job']['files']['count']) != len(self.job_files.keys()):
            syslog.syslog(
                syslog.LOG_INFO,
                '{} кол-во файлов пререданных не совпадает с количеством файлов в конфиге'.format(datetime.now())
            )
            sys.exit('кол-во файлов пререданных не совпадает с количеством файлов в конфиге')
        for step_number, cmd in self.dict_steps.iteritems():
            if step_number not in self.completed_step:
                if self.job_handler(step_number, self.dict_steps):
                    self.job_handling_error = True
                    break
                self.update_db_column('step_number',
                                      step_number,
                                      'job_id',
                                      self.job_id)
                self.completed_step.append(step_number)
                self.update_db_column('completed_steps',
                                      ' '.join(self.completed_step),
                                      'job_id',
                                      self.job_id)
        else:
            self.update_db_column('status', 0, 'job_id', self.job_id)
            self.update_db_column('date_finish',
                                  datetime.now().strftime(self.get_settings('DATE_FORMAT')),
                                  'job_id',
                                  self.job_id)

    def job_handler(self, current_step, steps_dict):
        """
        выполняет работу для текущего шага
        необходимо вызвать для каждого шага
        :return:  void
        """
        # current_step = self.select_db_column('step_number', 'job_id', self.job_id)[0]['step_number']
        step_cmd = steps_dict.get(current_step, None)
        if step_cmd is None:
            raise WrongJsonFormatException()
        new_cmd = self.files_in_cmd_inject(step_cmd)
        process = subprocess.Popen(new_cmd,
                                   shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        for line in iter(process.stdout.readline, b''):
            print(line.strip())
        return process.wait()
        # TODO: write this into log
        # for line in iter(process.stdout.readline, b''):
        #     print(line.strip())

    def get_completed_steps(self):
        completed_steps = self.select_db_column('completed_steps', 'job_id', self.job_id)[0]['completed_steps']
        return completed_steps.split()

    @staticmethod
    def check_pattern(cmd_str):
        """
        проверяет регуляркой есть ли в строке cmd {*}

        :param cmd_str: вызываемая командная строка
        :return: bool
        """
        pattern = re.compile('\*.*\*')
        return True if re.search(pattern, cmd_str) else False

    def make_job_files_dict(self):
        job_files = dict()
        job_files_string = self.select_db_column('arguments', 'job_id', self.job_id)[0]['arguments']
        for obj in job_files_string.split():
            file_param = obj.split('=')
            job_files[file_param[0]] = file_param[1]
            print('{} - {}'.format(file_param[0], file_param[1]))
        return job_files

    @property
    def get_job_handling_error(self):
        return self.select_db_column('error', 'job_id', self.job_id)[0]['error']

    @property
    def get_job_type(self):
        return self.select_db_column('task_type', 'job_id', self.job_id)[0]['task_type']

    def files_in_cmd_inject(self, step_cmd):
        """
        Вставляет в строку cmd пути до файлов

        :param step_cmd: строка cmd
        :return: новую строку cmd
        """
        new_cmd_string = step_cmd
        if self.check_pattern(step_cmd):
            for obj in self.job['job']['files']['names']:
                rep_str = '*{}*'.format(obj)
                new_cmd_string = new_cmd_string.replace(rep_str, self.job_files[obj])
        return new_cmd_string

    def make_system_reverse(self):
        with open(self.get_settings('JOB_JSON_CONF_PATH')) as conf:
            json_conf = json.load(conf)
            recovery_job = json_conf.get(self.job_type, None)
            key = self.job.get('error', None).get('handling', None)
            recovery_dict = OrderedDict(key)
        if recovery_job is None or key is None:
            raise WrongJsonFormatException()
        for step_number, cmd in recovery_dict.iteritems():
            if step_number in self.completed_step:
                self.job_handler(step_number, recovery_dict)
                self.update_db_column('step_number',
                                      step_number,
                                      'job_id',
                                      self.job_id)
                self.completed_step.remove(step_number)
                self.update_db_column('completed_steps',
                                      ' '.join(self.completed_step),
                                      'job_id',
                                      self.job_id)
        else:
            self.update_db_column('status', 0, 'job_id', self.job_id)
            self.update_db_column('recovery', 0, 'job_id', self.job_id)
            self.update_db_column('date_finish',
                                  datetime.now().strftime(self.get_settings('DATE_FORMAT')),
                                  'job_id',
                                  self.job_id)


class WrongJsonFormatException(Exception):
    def __init___(self, *args):
        Exception.__init__(self, "Wrong handling json file format in section: {0}".format(*args))
