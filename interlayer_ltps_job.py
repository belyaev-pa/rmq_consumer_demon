# -*- coding: utf-8 -*-
import os
import sys
import datetime
import errno
from job_handler import JobHandler, SQLLITE_PATH, DATE_FORMAT, \
    BaseDB, JOB_HANDLER_PID_FILE_PATH


def get_col_with_param_rows_len(col, param):
    """
    возвращает количество строк находящихся в БД в колонке col равно param
    :param col: выбираемая колонка
    :param param: параметр чем равна колонка
    :return: int(кол-во строк)
    """
    db = BaseDB(SQLLITE_PATH)
    rows = db.select_db_row(col, param)
    return len(rows)


def get_job_id(status):
    """
    возвращает job_id одной единственной записи с переданным статусом

    :param status: статус "completed"
    :return: job_id
    """
    db = BaseDB(SQLLITE_PATH)
    return db.select_db_column('job_id', 'status', status)[0]['job_id']


def get_job_manager_type(job_id):
    """
    возвращает manager_type строки с переданных job_id
    :param job_id: айди задачи
    :return: manager_type
    """
    db = BaseDB(SQLLITE_PATH)
    return db.select_db_column('manager_type', 'job_id', job_id)[0]['manager_type']


def check_process_handling():
    """
    проверяет выполняется ли какая то задача на основе существования pid файла
    и существует ли процесс с pid`ом из этого файла

    ESRCH == Нет такого процесса
    EPERM == Процесс не позволил узнать его статус (ошибка доступа)
    EINVAL == Сбой в работе питона
    существует всего три типа ошибки, по причине которых мы не сможем
    узнать статус процесса: (EINVAL, EPERM, ESRCH) следовательно
    мы никогда не должны прийти в ветку "else -> raise err"
    если мы туда попали, что то реально пошло не так в самом питоне

    :return: int(статус)
    список статусов:
    0 - файла нет, можно работать
    1 - файл есть, процесс тоже есть, нужно подождать выолнения
    2 - файл есть, процесса нет - необходимы дополнительные процерки
    err - возвращаемая ошибка питона - если что то пошло не так
    """
    if os.path.isfile(JOB_HANDLER_PID_FILE_PATH):
        with open(JOB_HANDLER_PID_FILE_PATH, 'r') as pid_file:
            pid = pid_file.read().strip()
        try:
            os.kill(int(pid), 0)
        except OSError as err:
            if err.errno == errno.ESRCH:
                return 2
            elif err.errno == errno.EPERM:
                return 1
            else:
                raise err
        else:
            return 1
    else:
        return 0


def check_db_process_handling(job_id, task_type, arguments, manager_type):
    """
    проверяем выполнение процессов, на основании данных из БД
    если существует файл, но процесса не существует:
      проверяем есть ли в БД запись со "status" - "processing"
      если есть - смотрим "manager_type" обекта
        если "manager_type" == "net"
          сотрим "job_id" и сравнимаем его с пришедшим на выполнение
            если совпал - запускаем довыполнение
            если нет, запускаем довыполнение, но возвращаем ошибку, чт опришел не тот job_id, но выполняем его

    :param job_id: id задания
    :param task_type: тип задачи
    :param arguments: словарь аргументов, файлы и пути к ним
    :param manager_type: тип вызвавшего менеджера локальный или по сети "local" или "net"
    :return:
    """
    rows_len = get_col_with_param_rows_len("status", "processing")
    if rows_len == 1:
        if get_job_manager_type(job_id) == manager_type:
            if job_id == get_job_id("processing"):
                redo_regular_job(job_id)
            else:
                # TODO: подумать над необходимыми действиями
                # довыполняем незаконченную задачу отбиваем лтпс обратчно, что бы он не
                # убирал акк с сообщения и попробовал выполнить задачу позже
                pass
        else:
            sys.exit("""Не могу продолжить выполнять поставленную задачу, 
            так как она поставлена менеджером другого типа. Дождитесь ее выполнения""")
    elif rows_len == 0:
        print('Warning: был найден pid файл, но в БД нет ни одной невыполненной задачи')
        # TODO: нужно где то проверить есть ли БД задача с таким job_id и посмотреть ее статус если есть
        do_regular_job(job_id, task_type, arguments, manager_type)
    else:
        pass
        # это случай, когда все пошло не так как задумывалось и
        # БД появилось слишком много записей со статусом "processing"
        # отбить ЛТПС подождать пока система попытается восстановиться
        # нужно запустить отдельный процесс, который попытается все доделать


def main(job_id, task_type, arguments, manager_type):
    """
    проверяем выполняется ли какая либо задача сейчас по пид файлу и процессу

    дальше отбиваем, что уже что то выполняется возвращаем ошибку
    + нужно где то добавить слип, что бы снова все это не пробывать сразу

    если работа не выполняется, создаем в БД запись куда записываем
    необходимую информацию для выполнения работы и создаем инстанс класса обработчика задач
    формируем поле файлов для работы

    пишем в БД:
    job_id - входящая инфа
    status - 'processing'
    step_number - 'step_1'
    arguments - входящая инфа
    task_type - входящая инфа
    manager_type - net или local
    completed_steps - ''
    date_start - datetime.datetime.now().strftime(DATE_FORMAT)
    date_finish - ''

    :param job_id: id задания
    :param task_type: тип задачи
    :param arguments: словарь аргументов, файлы и пути к ним
    :param manager_type: тип вызвавшего менеджера локальный или по сети "local" или "net"
    :return: void
    """
    stat = check_process_handling()
    if stat == 1:
        sys.exit('Обработчик уже занят выполнением задачи, асинхронное выполнение запрещено, подождите')
    elif stat == 0:
        if get_col_with_param_rows_len('job_id', job_id) > 0:
            check_db_process_handling(job_id, task_type, arguments, manager_type)
        else:
            do_regular_job(job_id, task_type, arguments, manager_type)
    elif stat == 2:
        check_db_process_handling(job_id, task_type, arguments, manager_type)
    else:
        sys.exit('все в говне питон упал')


def do_regular_job(job_id, task_type, arguments, manager_type):
    """
    выполнение работы в нормальных условия, когда записи с таким айди нет,
    создает новую запись в БД и запускает работу @job_id
    :param job_id: id задания
    :param task_type: тип задачи
    :param arguments: словарь аргументов, файлы и пути к ним
    :param manager_type: тип вызвавшего менеджера локальный или по сети "local" или "net"
    :return: void
    """
    db = BaseDB(SQLLITE_PATH)
    insert_tuple = (job_id, 'processing', 'step_1',
                    ' '.join(map(str, arguments)),
                    task_type, manager_type, '',
                    datetime.datetime.now().strftime(DATE_FORMAT), '',)
    db.insert_into_table(insert_tuple)
    JobHandler(job_id)


def redo_regular_job(job_id):
    """
    запускает выполнение задачи с заданным job_id
    :param job_id:
    :return:
    """
    JobHandler(job_id)


if __name__ == '__main__':
    # print('выполняем работу с аргументами: {}'.format(sys.argv))
    # job_type = sys.argv[1] or None
    # job_id = sys.argv[2] or None
    # if job_id is None:
    #     sys.exit('не передан job_id')
    # if job_type is None:
    #     sys.exit('не передан job_type для выполнения')
    # sys.argv[3:]
    job_id = '1234567899'
    job_type = 'test_job'
    main(job_id, job_type, ['log_txt_file=/home/pavel/test_log.txt'], 'net')
