# -*- coding: utf-8 -*-
# имя очереди для тестов, в дальнейшем нужно изменить метод
QUEUE_NAME = 'vm_user@che.ru.ab'
RABBITMQ_HOST = '10.128.152.30'
RABBITMQ_PORT = 5672
HEARTBEAT_INTERVAL = 600
BLOCKED_CONNECTION_TIMEOUT = 300
PID_FILE_PATH = "/var/run/ab_demon.pid"
LOG_NAME = "ab_demon"
REPLY_TO = "reply_queue"