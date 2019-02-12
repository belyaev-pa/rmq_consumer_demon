# -*- coding: utf-8 -*-
import sys
import conf
import demon


class ReactFunction:
    """
    Класс команд обрабатываемых агентом
    для добавления команды, необходимо добавить
    новую функцию в класс из консоли будет вызываться
    python ab_demon.py new_command
    """
    def __init__(self, _daemon):
        """
        конструктор
        :param _daemon: инстанс демона
        """
        self._daemon = _daemon

    def start(self):
        """
        команда для запуска демона
        :return: void
        """
        self._daemon.start()

    def stop(self):
        """
        команда для остановки демона
        :return: void
        """
        self._daemon.stop()

    def restart(self):
        """
        команда для рестарта демона
        :return:
        """
        self._daemon.restart()

    def stmess(self, message):
        """
        команда для демонстрации функционала
        выводит принятое сообщение, потом запускает демона
        :param message: строку для печати
        :return: void
        """
        print message
        self._daemon.start()


class DaemonConfigurator:

    def __init__(self, _daemon):
        self._demon = _daemon

    def get_reacts_for_demon(self):
        """
        формирует словарь команд на основании класса ReactFunction
        :return: словарь методов
        """
        localCon = ReactFunction(self._demon)
        react_dict = {}
        for react in dir(localCon):
            if react[0:1] != '_':
                react_dict[react] = getattr(localCon, react)
        return react_dict


if __name__ == "__main__":
    daemon = demon.Demon(conf.PID_FILE_PATH, conf.QUEUE_NAME, conf.LOG_NAME, 'ab')
    config = DaemonConfigurator(daemon)
    react_dict = config.get_reacts_for_demon()

    if len(sys.argv) > 1:
        if sys.argv[1] in react_dict.keys():
            try:
                react_dict[sys.argv[1]](*sys.argv[2:len(sys.argv)])
                sys.exit(0)
            except TypeError, error:
                print error
                sys.exit(2)
        else:
            print "usage: %s %s" % (sys.argv[0], react_dict)
            sys.exit(2)
