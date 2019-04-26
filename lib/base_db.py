# -*- coding: utf-8 -*-
import sqlite3
import syslog
import sys
from datetime import datetime


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class BaseDB(object):
    """
    Класс для реализации менеджера контекста для создания и закрытия соединения с БД
    + пару полезных функций
    """
    def __init__(self, conf_dict):
        """
        конструктор класса
        тут подключаемся к базе sqllite и смотрим есть ли табличка с которой мы будем работать
        если нет - создаем
        :param conf_dict:
        """
        self.conf_dict = conf_dict
        syslog.openlog(self.get_settings('LOG_NAME'))
        try:
            self.conn = sqlite3.connect(self.get_settings('DB_PATH'))
        except sqlite3.Error as e:
            syslog.syslog(syslog.LOG_INFO, '{} can`t connect to DB...{}'.format(datetime.now(), e))
            sys.exit('can`t connect to DB...')
        else:
            syslog.syslog(syslog.LOG_INFO, '{} Connection is successful...'.format(datetime.now()))
            self.check_table_or_create()

    def __enter__(self):
        """
        Необходимые "магические методы" для реалзации функционала with
        Использование:
        with RabbitMQSender() as sender_obj:
            # use sender_obj (используем объект тут)
        :return:
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Необходимые "магические методы" для реалзации функционала with
        :param exc_type:
        :param exc_value:
        :param traceback:
        :return:
        """
        # syslog.syslog(syslog.LOG_INFO,
        #               '{} Close database connection...{}'.format(datetime.now(),
        #                                                          traceback.format_exc(limit=2)))
        self.conn.close()

    def update_db_column(self, column, value, condition, parameter):
        """
        Обновление ячейки по условию

        :param column: столбец для обновления
        :param value: новое значение
        :param condition: условие
        :param parameter: значение условия
        :return: void
        """
        self.conn.row_factory = dict_factory
        c = self.conn.cursor()
        c.execute("""
                        UPDATE ab_tasks 
                        SET {} = '{}'
                        WHERE {} = '{}';
                        """.format(column, value, condition, parameter))
        c.close()
        self.conn.commit()

    def select_db_column(self, column, condition, parameter):
        """
        выбор нужного столбца с указанными условиями поиска

        :param column: выбираемый столбец
        :param condition: условие
        :param parameter: значение условия
        :return: список словарей со значениями
        """
        self.conn.row_factory = dict_factory
        c = self.conn.cursor()
        c.execute("""
                        SELECT {}
                        FROM ab_tasks 
                        WHERE {} = '{}';
                        """.format(column, condition, parameter))
        return c.fetchall()

    def select_db_row(self, condition, parameter):
        """
        выбор нужного столбца с текущим айди

        :param condition: условие
        :param parameter: значение условия
        :return: список слвоарей со значениями
        """
        self.conn.row_factory = dict_factory
        c = self.conn.cursor()
        c.execute("""
                        SELECT *
                        FROM ab_tasks 
                        WHERE {} = '{}';
                        """.format(condition, parameter))
        return c.fetchall()

    def insert_into_table(self, insert_tuple):
        """
        вставка новой записи при инициализации новой задачи на основе кортежа сл формата:
        ('job_id', 'status', 'step_number', 'arguments', 'task_type',
        'manager_type', 'completed_steps', 'date_start', 'date_finish',)
        :param insert_tuple:
        :return: void
        """
        c = self.conn.cursor()
        c.execute("""
                        INSERT INTO ab_tasks (                       
                        job_id,
                        status,
                        error,                        
                        step_number,
                        arguments,
                        task_type,
                        manager_type,
                        completed_steps,
                        date_start,
                        date_finish)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, insert_tuple)
        c.close()
        self.conn.commit()

    def check_table_or_create(self):
        c = self.conn.cursor()
        c.execute("""            
                        CREATE TABLE IF NOT EXISTS ab_tasks (
                        'id'              INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        'job_id'          VARCHAR                           NOT NULL,
                        'status'          INTEGER                           NOT NULL,
                        'error'           INTEGER                           NOT NULL,                        
                        'step_number'     VARCHAR                           NOT NULL,
                        'arguments'       TEXT                              NOT NULL,
                        'task_type'       VARCHAR                           NOT NULL,
                        'manager_type'    VARCHAR                           NOT NULL,                        
                        'completed_steps' VARCHAR,
                        'date_start'      VARCHAR,
                        'date_finish'     VARCHAR,
                        'recovery'        INTEGER,                        
                        UNIQUE ('job_id')
                        );
                        """)
        c.close()
        self.conn.commit()

    def get_settings(self, setting):
        if type(self.conf_dict) is not dict:
            raise AttributeError('conf_dict must be a dict.')
        try:
            prop = self.conf_dict[setting]
        except KeyError as e:
            raise SettingIsNoneException("Can`t find {0} in config file".format(e))
        return prop


class SettingIsNoneException(Exception):
    pass
