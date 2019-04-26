# -*- coding: utf-8 -*-
class A:


    def __init__(self, job_id, conf_dict, arguments, manager_type):
        """
        Конструктор обработчика заданий
        TODO: доделать ветку возврата после сбоя
        TODO: сделать создание pid файла и запись pid процесса в файл
        TODO: добавить + os.remove(JOB_HANDLER_PID_FILE_PATH)
        :param job_id: id задачи из БД, которую нужно выполнить
        :param conf_dict: словарь с настройками
        :param arguments: список аргументов, файлы и пути к ним: ['log_txt_file=/home/pavel/test_log.txt']
        :param manager_type: тип меджера, который запустил функцию (net или local)
        """
        self.job_id = job_id
        self.job_handler_pid_file_path = self.get_settings('JOB_HANDLER_PID_FILE_PATH')
        self.manager_type = manager_type
        self.arguments = arguments
        self.job_first_time_handling = False
        self.job_handling_error = False
        super(JobHandler, self).__init__(conf_dict)
        syslog.openlog(self.get_settings('LOG_NAME'))
        self.job_memory = None
        self.check_process_handling()
        # TODO здесь нужно уже проверить есть ли пид файл и тд...
        self.job_type = self.get_job_type
        self.job_files = self.make_job_files_dict()
        self.completed_step = self.get_completed_steps()
        with open(self.get_settings('JOB_JSON_CONF_PATH')) as conf:
            json_conf = json.load(conf)
            self.job = json_conf.get(self.job_type, None)
        if self.job is None:
            syslog.syslog(syslog.LOG_INFO,
                          '{} не найдено работы с именем {} в конфиг файле...'.format(datetime.now(),
                                                                                      self.job_type))
            sys.exit("не найдено работы с именем {} в конфиг файле...".format(self.job_type))
        else:
            self.dict_steps = OrderedDict(self.job['job']['handling'])
            if self.job_memory is not None:
                # выполняем нужную функцию
                self.job_memory()
            else:
                sys.exit('Ошибка в коде: нет выполняемой задачи!')


    def check_process_handling(self):
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

        проверяем выполняется ли какая либо задача сейчас по пид файлу и процессу

        дальше отбиваем, что уже что то выполняется возвращаем ошибку
        TODO: + нужно где то добавить слип, что бы снова все это не пробывать сразу

        если работа не выполняется, создаем в БД запись куда записываем
        необходимую информацию для выполнения работы и создаем инстанс класса обработчика задач
        формируем поле файлов для работы
        :return: int(статус)
        список статусов возможных исходов:
        - файла нет, можно работать
        - файл есть, процесс тоже есть, нужно подождать выолнения
        - файл есть, процесса нет - необходимы дополнительные проверки
        err - возвращаемая ошибка питона - если что то пошло не так
        """
        if os.path.isfile(self.job_handler_pid_file_path):
            with open(self.job_handler_pid_file_path, 'r') as pid_file:
                pid = pid_file.read().strip()
            try:
                os.kill(int(pid), 0)
            except OSError as err:
                if err.errno == errno.ESRCH:
                    self.check_from_db_process_handling()
                elif err.errno == errno.EPERM:
                    syslog.syslog(syslog.LOG_INFO,
                                  '{} Обработчик уже занят выполнением задачи, асинхронное выполнение запрещено, подождите'.format(
                                      datetime.now(), self.job_type))
                    sys.exit('Обработчик уже занят выполнением задачи, асинхронное выполнение запрещено, подождите')
                else:
                    raise err
            else:
                syslog.syslog(syslog.LOG_INFO,
                              '{} Обработчик уже занят выполнением задачи, асинхронное выполнение запрещено, подождите'.format(
                                  datetime.now(), self.job_type))
                sys.exit('Обработчик уже занят выполнением задачи, асинхронное выполнение запрещено, подождите')
        else:
            if self.select_db_row('job_id', self.job_id):
                self.check_from_db_process_handling()
            else:
                self.job_first_time_handling = True
                self.job_memory = self.do_regular_job

    def check_from_db_process_handling(self):
        """
        проверяем выполнение процессов, на основании данных из БД
        если существует файл, но процесса не существует:
          проверяем есть ли в БД запись со "status" - "processing"
          если есть - смотрим "manager_type" обекта
            если "manager_type" == "net"
              сотрим "job_id" и сравнимаем его с пришедшим на выполнение
                если совпал - запускаем довыполнение
                если нет, запускаем довыполнение, но возвращаем ошибку, чт опришел не тот job_id, но выполняем его
        """
        rows = self.select_db_row("status", "processing")
        if rows:
            if len(rows) == 1:
                if self.select_db_column('manager_type',
                                         'job_id',
                                         self.job_id)[0]['manager_type'] == self.manager_type:
                    if self.job_id == self.select_db_column('job_id', 'status', "processing")[0]['job_id']:
                        self.job_first_time_handling = True
                        self.job_memory = self.do_regular_job
                    else:
                        self.cancel_job_spawn_child()

                else:
                    sys.exit("""Не могу продолжить выполнять поставленную задачу, 
                        так как она поставлена менеджером другого типа. Дождитесь ее выполнения""")
            else:
                pass
                # TODO: запустить процесс довыполнения всех поставленных задач
                # это случай, когда все пошло не так как задумывалось и
                # БД появилось слишком много записей со статусом "processing"
                # отбить ЛТПС подождать пока система попытается восстановиться
                # нужно запустить отдельный процесс, который попытается все доделать
        else:
            syslog.syslog(syslog.LOG_INFO,
                          '{} Warning: был найден pid файл, но в БД нет ни одной невыполненной задачи'.format(
                              datetime.now()))
            # TODO: нужно где то проверить есть ли БД задача с таким job_id и посмотреть ее статус если есть сообщить, что уже выполнена, и все OK
            self.job_memory = self.do_regular_job




    def do_job(self):
        """
        выполнение работы в нормальных условия, когда записи с таким айди нет,
        создает новую запись в БД и запускает работу @job_id
        """
        if self.job_first_time_handling:
            insert_tuple = (self.job_id, 'processing', 'step_1',
                            ' '.join(map(str, self.arguments)),
                            self.job_type, self.manager_type, '',
                            datetime.now().strftime(self.get_settings('DATE_FORMAT')), '',)
            self.insert_into_table(insert_tuple)
        self.run_job()

    def cancel_job_spawn_child(self):
        # TODO: подумать над необходимыми действиями (возможно достаточно породить процесс и отпустить его, а этот завершить с ошибкой)
        # довыполняем незаконченную задачу отбиваем лтпс обратчно, что бы он не
        # убирал акк с сообщения и попробовал выполнить задачу позже
        pass

