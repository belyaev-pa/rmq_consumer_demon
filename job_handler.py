# -*- coding: utf-8 -*-
import sys
import json
import subprocess
import re
import datetime
from base_db import BaseDB
from collections import OrderedDict


JSON_CONF_PATH = 'handle_scheme.json'
SQLLITE_PATH = 'ab.sqlite3'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%f'


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
    __metaclass__ = SingletonMeta

    def __init__(self, job_id, job_type, job_files):
        super(JobHandler, self).__init__(SQLLITE_PATH)
        self.job_id = job_id
        self.job_type = job_type
        self.job_files = job_files
        self.completed_step = self.get_completed_steps()
        with open(JSON_CONF_PATH ,'r') as conf:
            json_conf = json.load(conf.read())
            self.job = json_conf.get(self.job_type, None)
        if self.job is None:
            sys.exit(1)
            # печатаем что такой работы не существует
        else:
            self.dict_steps = OrderedDict(self.job['job']['handling'])
            self.pre_handle_job()

    def pre_handle_job(self):
        """
        парсим дату
        date_time_obj = datetime.datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S.%f")

        :return:
        """
        if self.job['job']['files']['count'] != self.job_files.keys().count():
            print('кол-во файлов пререданных не совпадает с количеством файлов в конфиге')
            sys.exit(1)
        self.db_connect_open()
        for step_number, cmd in self.dict_steps.iteritems():
            if step_number not in self.completed_step:
                self.update_db_column('step_number',
                                      step_number,
                                      'job_id',
                                      self.job_id)
                self.job_handler()
                self.completed_step.append(step_number)
                self.update_db_column('completed_steps',
                                      ''.join(self.completed_step),
                                      'job_id',
                                      self.job_id)
        else:
            self.update_db_column('status', 'completed', 'job_id', self.job_id)
            self.update_db_column('date_finish',
                                  datetime.datetime.now().strftime(DATE_FORMAT),
                                  'job_id',
                                  self.job_id)

    def job_handler(self):
        """
        выполняет работу для текущего шага
        необходимо вызвать для каждого шага
        :return:  void
        """
        current_step = self.select_db_column('step_number', 'job_id', self.job_id)
        step_cmd = self.dict_steps.get(current_step, None)
        if step_cmd is None:
            print('в конфигурационном файле нет шага с именем {}'.format(current_step))
            sys.exit(1)
        self.files_in_cmd_inject(step_cmd)
        process = subprocess.Popen(step_cmd,
                                   shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        for line in iter(process.stdout.readline, b''):
            print(line.strip())

    def get_completed_steps(self):
        completed_steps = self.select_db_column('completed_steps', 'job_id', self.job_id)[0]
        return completed_steps['completed_steps'].split()

    @staticmethod
    def check_pattern(cmd_str):
        """
        проверяет регуляркой есть ли в строке cmd {*}

        :param cmd_str: вызываемая командная строка
        :return: bool
        """
        pattern = re.compile('{.*}')
        return True if re.search(pattern, cmd_str) else False

    def files_in_cmd_inject(self, step_cmd):
        """
        Вставляет в строку cmd пути до файлов

        :param step_cmd: строка cmd
        :return: новую строку cmd
        """
        new_cmd_string = step_cmd
        if self.check_pattern(step_cmd):
            for obj in self.job['files']['names']:
                new_cmd_string = new_cmd_string.replace('{'+obj+'}', self.job_files[obj])
        return new_cmd_string


if __name__ == '__main__':
    print(sys.argv)
    job_type = sys.argv[1] or None
    job_id = sys.argv[2] or None
    job_files = dict()
    # сделать проверку введенных данных
    # возможно нужно передать id задачи уникальный, что бы понять,
    # что мы это уже выполняли до перезагрузки
    for arg in sys.argv[3:]:
        arg_split = arg.split('=')
        job_files[arg[0]] = arg[1]
    JobHandler(job_id, job_type, job_files)
