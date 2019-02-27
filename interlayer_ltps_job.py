# -*- coding: utf-8 -*-
import sys
import datetime
from job_handler import JobHandler, SQLLITE_PATH, DATE_FORMAT, BaseDB


def main(job_id, task_type, arguments):
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
    manager_type - net
    completed_steps - ''
    date_start - datetime.datetime.now().strftime(DATE_FORMAT)
    date_finish - ''

    :return:
    """
    db = BaseDB(SQLLITE_PATH)
    insert_tuple = (job_id, 'processing', 'step_1',
        ' '.join(map(str, arguments)),
        task_type,
        'net',
        '',
        datetime.datetime.now().strftime(DATE_FORMAT),
        '',
    )
    db.insert_into_table(insert_tuple)


if __name__ == '__main__':
    print('выполняем работу с аргументами: {}'.format(sys.argv))
    job_type = sys.argv[1] or None
    job_id = sys.argv[2] or None
    if job_id is None:
        sys.exit('не передан job_id')
    if job_type is None:
        sys.exit('не передан job_type для выполнения')
    main(job_id, job_type, sys.argv[3:])