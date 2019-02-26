# -*- coding: utf-8 -*-
import sqlite3


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class BaseDB(object):

    def __init__(self, db_path):
        self.db_path = db_path
        
    def db_connect_open(self):
        """
        тут подключаемся к базе sqllite и смотрим есть ли табличка с которой мы будем работать
        если нет - создаем

        :return: void
        """
        try:
            self.conn = sqlite3.connect(self.db_path)
            # TODO: find and add connection error from sqlite3 exceptions
        except:
            print('can`t connect')
        else:
            print('okey')
            self.check_table_or_create()

    def db_connect_close(self):
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
                        step_number,
                        arguments,
                        task_type,
                        manager_type,
                        completed_steps,
                        date_start,
                        date_finish)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """, insert_tuple)
        c.close()
        self.conn.commit()

    def check_table_or_create(self):
        c = self.conn.cursor()
        c.execute("""            
                        CREATE TABLE IF NOT EXISTS ab_tasks (
                        'id'              INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        'job_id'          VARCHAR                           NOT NULL,
                        'status'          VARCHAR                           NOT NULL,
                        'step_number'     VARCHAR                           NOT NULL,
                        'arguments'       TEXT                              NOT NULL,
                        'task_type'       VARCHAR                           NOT NULL,
                        'manager_type'    VARCHAR                           NOT NULL,
                        'completed_steps' VARCHAR,
                        'date_start'      VARCHAR,
                        'date_finish'     VARCHAR,
                        UNIQUE ('job_id')
                        );
                        """)
        c.close()
        self.conn.commit()



if __name__ == '__main__':
    db = BaseDB('ab.sqlite3')
    db.db_connect_open()
    insert_tuple = (
        'G4G3G2G1-G6G5-G8G7-G9G10-G11G12G13G14G15G16',
        'start',
        '1',
        'file1=/path/to/file file2=/path/to/file2',
        'type1',
        'local',
        '',
        '',
    )
    print(insert_tuple)
    # db.insert_into_table(insert_tuple)
    db.update_db_column('status', 'ended', 'job_id', 'G4G3G2G1-G6G5-G8G7-G9G10-G11G12G13G14G15G16')
    print(db.select_db_column('id', 'job_id', 'G4G3G2G1-G6G5-G8G7-G9G10-G11G12G13G14G15G16'))
    print(db.select_db_row('job_id', 'G4G3G2G1-G6G5-G8G7-G9G10-G11G12G13G14G15G16'))
