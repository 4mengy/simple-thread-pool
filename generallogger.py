#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import logging
import logging.handlers
import sys
import threading
import os

from wrapt import synchronized


# NOTSET = 0
LOG_LEVEL_NOTSET = logging.NOTSET
LOG_LEVEL_DEBUG = logging.DEBUG
LOG_LEVEL_INFO = logging.INFO
LOG_LEVEL_WARNING = logging.WARNING
LOG_LEVEL_ERROR = logging.ERROR
# CRITICAL = 50
LOG_LEVEL_CRITICAL = logging.CRITICAL

# 日志输出目标
LOG_TARGET_CONSOLE = 0x1
LOG_TARGET_LOG_FILE = 0x10
LOG_TARGET_LOG_HTTP = 0x100

# 单个进程可以拥有的最大线程数量，cat /proc/sys/kernel/threads-max
_LOGGER_FORMAT = "[%(levelname)7s] [%(asctime)s] [%(thread)d] [%(module)s] - %(message)s"


class InfoOrLessCritical(logging.Filter):
    # handler的日志过滤器
    def filter(self, record):
        return record.levelno < LOG_LEVEL_WARNING

# rlock绑定到类
@synchronized
class HandlerFactory(object):
    handlers = {}

    @classmethod
    def get_std_out_handler(cls):
        if 'std_out_handler' not in cls.handlers:
            std_out_handler = logging.StreamHandler(sys.stdout)
            std_out_handler.setFormatter(logging.Formatter(_LOGGER_FORMAT))
            std_out_handler.addFilter(InfoOrLessCritical())
            cls.handlers['std_out_handler'] = std_out_handler

        return cls.handlers['std_out_handler']

    @classmethod
    def get_std_err_handler(cls):
        if 'std_err_handler' not in cls.handlers:
            std_err_handler = logging.StreamHandler(sys.stderr)
            std_err_handler.setFormatter(logging.Formatter(_LOGGER_FORMAT))
            std_err_handler.setLevel(LOG_LEVEL_WARNING)
            cls.handlers['std_err_handler'] = std_err_handler

        return cls.handlers['std_err_handler']

    @classmethod
    def get_rotating_file_handler(cls, log_path, max_bytes, backup_count):
        if 'rotating_file_handler' not in cls.handlers:
            cls.handlers['rotating_file_handler'] = {}

        if log_path not in cls.handlers['rotating_file_handler']:
            rotating_file_handler = logging.handlers.RotatingFileHandler(
                log_path, 'a', max_bytes, backup_count)
            rotating_file_handler.setFormatter(logging.Formatter(_LOGGER_FORMAT))
            cls.handlers['rotating_file_handler'][log_path] = rotating_file_handler

        return cls.handlers['rotating_file_handler'][log_path]

    @classmethod
    def get_timed_rotating_file_handler(cls, log_path, when, interval, backup_count):
        if 'timed_rotating_file_handler' not in cls.handlers:
            cls.handlers['timed_rotating_file_handler'] = {}

        if log_path not in cls.handlers['timed_rotating_file_handler']:
            timed_rotating_file_handler = logging.handlers.TimedRotatingFileHandler(
                log_path, when, interval, backup_count)
            timed_rotating_file_handler.setFormatter(logging.Formatter(_LOGGER_FORMAT))
            cls.handlers['timed_rotating_file_handler'][log_path] = timed_rotating_file_handler

        return cls.handlers['timed_rotating_file_handler']

# 获取根记录器，用来记录本模块自己的日志信息
logger = logging.getLogger(__name__)


# 利用Module只会加载一次实现的单例模式
def singleton(cls, *args, **kw):
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return _singleton


@singleton
class GeneralLogger(object):
    def __init__(self, level=LOG_LEVEL_DEBUG, log_by_thread=False, log_path='', max_bytes=0, backup_count=0):
        '''
        通用日志记录器，当单个文件大小超过限制后，会进行切分，例如foo.log, foo.log.1, foo.log.2, ...
        :param level: 默认记录等级
        :param log_by_thread: 线程是否记录到单独的文件
        :param log_path: 日志文件路径，文件名或路径
        :param max_bytes: 单个日志文件的最大大小
        :param backup_count: 保留的日志文件数
        '''
        logging.getLogger().setLevel(LOG_LEVEL_NOTSET)
        # 日志会经过多个handler处理
        logging.getLogger().addHandler(HandlerFactory.get_std_out_handler())
        logging.getLogger().addHandler(HandlerFactory.get_std_err_handler())
        logger.info("General logger initializing...")
        self._loggers = {}
        self._log_level = level
        # 该类是单例的，所以它对应主线程的ID
        self._main_thread_id = str(self.get_current_thread_id())
        self._log_destination = LOG_TARGET_CONSOLE
        self._log_by_thread = log_by_thread
        self._log_path = log_path
        self._log_file_max_bytes = max_bytes
        self._log_file_backup_count = backup_count

    @staticmethod
    def get_current_thread_id():
        return threading.current_thread().ident

    @staticmethod
    def get_current_thread_name():
        return threading.current_thread().name

    def get_log_file_name(self):
        # 对传入的log_path参数分情况处理
        log_path = os.path.abspath(self._log_path)
        base_name = os.path.basename(log_path)
        base_dir = os.path.dirname(log_path)

        if self._log_by_thread:
            base_name = '{}_{}_{}'.format(self.get_current_thread_id(),
                                          self.get_current_thread_name(), base_name)
        # 如果传入的是一个目录，生成文件名
        if os.path.isdir(log_path):
            return os.path.join(log_path, base_name)
        elif base_name and '.' not in base_name:
            # 类似'/tmp/a'的路径，应该创建文件夹
            os.makedirs(log_path)
            return os.path.join(log_path, base_name)
        else:
            return os.path.join(base_dir, base_name)

    @synchronized
    def get_logger(self):
        name = self._main_thread_id

        if self._log_by_thread:
            current_id = str(self.get_current_thread_id())
            # 如果不是主线程，将子线程的记录器设置为主记录器的子记录器
            # 记录器的父子关系是根据名字确定的，分隔符是'.'
            if current_id != self._main_thread_id:
                name = self._main_thread_id + '.' + current_id
        # 如果不分线程记录日志，子线程将使用主记录器
        if name not in self._loggers:
            self.set_logger(name)
        return self._loggers[name]

    @synchronized
    def set_logger(self, name):
        # 添加新的logger，并做相应的配置，添加handler
        if name not in self._loggers:
            new_logger = logging.getLogger(name)
            new_logger.setLevel(self._log_level)

            if self._log_path:
                log_path = self.get_log_file_name()
                # 添加新的handler
                new_logger.addHandler(HandlerFactory.get_rotating_file_handler(
                    log_path, self._log_file_max_bytes, self._log_file_backup_count))

            self._loggers[name] = new_logger

    @synchronized
    def set_log_path(self, file_path, max_bytes=0, backup_count=0):
        '''
        设置基本配置信息，也可以在实例化时设置
        :param file_path:
        :param max_bytes:
        :param backup_count:
        :return:
        '''
        if isinstance(file_path, str):
            self._log_path = file_path
        if isinstance(max_bytes, int):
            self._log_file_max_bytes = max_bytes
        if isinstance(backup_count, int):
            self._log_file_backup_count = backup_count

    @synchronized
    def set_log_level(self, new_level):
        self._log_level = new_level
        # 将各子logger的设置更新
        for instance_logger in self._loggers.values():
            instance_logger.setLevel(self._log_level)

    @synchronized
    def set_log_by_thread_log(self, log_by_thread):
        self._log_by_thread = log_by_thread
        # 如果不是分线程记录日志，只启用主记录器
        for instance_logger in self._loggers.values():
            instance_logger.disabled = not self._log_by_thread

        try:
            self._loggers[self._main_thread_id].disabled = self._log_by_thread
        except KeyError:
            pass

