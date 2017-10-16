#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

'''
任务处理线程
'''

import sys
import threading
import queue
from wrapt import synchronized
from generallogger import GeneralLogger
from queuedata.workqueuedata import WorkRequest, WorkResult


class WorkerThread(threading.Thread):
    '''
    后台线程，真正的工作线程，从请求队列request_queue中获取work，
    并将执行后的结果添加到结果队列result_queue
    '''
    def __init__(self, request_queue, result_queue, poll_timeout=5, name=None):
        '''

        :param request_queue: 请求队列
        :param result_queue: 结果队列
        :param poll_timeout: 从请求队列取请求时的超时时间，单位：秒
        :param name: 该worker的名字，默认为None
        '''
        threading.Thread.__init__(self, name=name)
        self._request_queue = request_queue
        self._result_queue = result_queue
        self._poll_timeout = poll_timeout
        # 设置一个flag信号，用来表示该线程是否还被dismiss,默认为false
        self._dismissed = threading.Event()
        self._logger = GeneralLogger().get_logger()
        # 启动线程，线程会自行调用run()方法
        self.start()

    def run(self):
        '''
        每个线程尽可能多的执行work，所以采用loop，
        只要线程可用，并且request_queue有work未完成，则一直loop
        :return:
        '''
        self._logger.info('WorkerThread started.')
        while True:
            # 是否要终止线程
            if self._dismissed.is_set():
                self._logger.info('WorkerThread has dismissed.')
                break
            try:
                # Queue.Queue队列已经实现了线程同步策略，并且可以设置timeout。
                # 一直block，直到request_queue有值，或者超时
                request = self._request_queue.get(True, self._poll_timeout)
            except queue.Empty:
                continue

            # 再次判断dimissed，是因为之前的timeout时间里，有可能，该线程被dismiss掉了
            # 管理线程有该线程对象的引用，所以在管理线程可以调用对象的方法
            if self._dismissed.is_set():
                # 如果dismissed，需要将已经取出的任务放回请求队列
                self._request_queue.put(request)
                self._logger.info('WorkerThread has dismissed and request reput in queue.')
                break

            # 如果取出的不是正确类型的数据，忽略，要先检查是否dismissed
            if not isinstance(request, WorkRequest):
                continue

            # 默认返回值
            return_value = None
            status = True
            err_msg = None
            try:
                print(request.kwds, threading.current_thread().ident)
                # 执行callable，将结果保存到WorkResult
                return_value = request.callable(*request.args, **request.kwds)
                self._logger.info('logger ' + str(request.kwds))
                print(request.kwds, return_value, threading.current_thread().ident)
            except:
                status = False
                err_msg = sys.exc_info()
                msg = 'WorkerThread got an exception in processing task: {}'
                self._logger.warning(msg.format(err_msg))
            result = WorkResult(request, status, return_value, err_msg)
            self._result_queue.put(result)

    def dismiss(self):
        '''
        设置一个标志，表示完成当前work之后，退出
        :return:
        '''
        self._dismissed.set()
        self._logger.info('WorkerThread are requred to dismiss.')


class ThreadPool(object):
    '''
    该类是线程安全的
    '''
    def __init__(self, num_workers, reqq_size=0, resq_size=0, poll_timeout=5,
                 comm_reqq_size=0, comm_resq_size=0):
        '''
        线程池管理类
        :param num_workers: 初始化的线程数量
        :param reqq_size: request队列的初始大小，默认0表示无限制
        :param resq_size: result队列的初始大小，默认0表示无限制
        :param poll_timeout: 设置工作线程WorkerThread等待request_queue的timeout，单位：秒
        '''
        self._poll_timeout = poll_timeout
        self.request_queue = queue.Queue(reqq_size)
        self.result_queue = queue.Queue(resq_size)

        # 所有激活的线程的引用
        self._workers = []
        # 保存dismiss的线程对象，对象可能已经运行结束，也可能没有
        self._dismissed_workers = []
        self._num_workers = num_workers
        self._logger = GeneralLogger().get_logger()

    def start(self):
        '''
        创建线程池，开始运行
        :return:
        '''
        self.create_workers(self._num_workers)
        self._logger.info('Thread Pool Started with {} workers.'.format(self._num_workers))

    # rlock绑定到类实例
    @synchronized
    def create_workers(self, num_workers):
        '''
        创建num_workers个WorkThread,默认等待request_queue的timeout为5
        :param num_workers:
        :return:
        '''
        for i in range(num_workers):
            self._workers.append(WorkerThread(self.request_queue,
                                              self.result_queue, poll_timeout=self._poll_timeout))

    # rlock绑定到类实例
    @synchronized
    def add_workers(self, num_workers):
        self.create_workers(num_workers)
        self._logger.info('Thread Pool add {} workers.'.format(num_workers))

    # rlock绑定到类实例
    @synchronized
    def dismiss_workers(self, num_workers):
        '''
        停用num_workers数量的线程，并加入dismiss_list
        该方法不会阻塞，不会等待线程结束，会立即返回，所以不能保证方法返回后，线程就已经全部终止。
        :param num_workers:
        :return:
        '''
        dismiss_list = []
        # 当self._workers为空时，for内的语句不会执行
        dismiss_num = min(num_workers, len(self._workers))
        for i in range(dismiss_num):
            worker = self._workers.pop()
            # 向线程发送信号，线程处理完当前的任务就会结束
            worker.dismiss()
            dismiss_list.append(worker)
        self._dismissed_workers.extend(dismiss_list)
        self._logger.info('Thread Pool dismiss {} workers'.format(dismiss_num))

    # rlock绑定到类实例
    @synchronized
    def join_all_dismissed_workers(self):
        '''
        等待所有已经停用的thread结束，一个线程对象结束后也可以调用join
        该函数会阻塞，直到所有已停用的线程结束
        :return:
        '''
        # 当self._dismissed_workers为空时，for语句不会执行
        for worker in self._dismissed_workers:
            worker.join()
        self._dismissed_workers = []
        self._logger.info('Thread Pool\'s workers all dismissed')

    # rlock绑定到类实例
    @synchronized
    def clean_joined_workers(self):
        '''
        将已经结束的线程从停用线程列表中删除，该函数不会阻塞
        :return:
        '''
        dismissed_num = len(self._dismissed_workers)
        alive_workers = []
        # 当self._dismissed_workers为空时，for语句不会执行
        for worker in self._dismissed_workers:
            if not worker.is_alive():
                alive_workers.append(worker)
        self._dismissed_workers = alive_workers
        msg = 'Thread Pool dismissed workers: {} is still alive, {} have stopped and cleaned'
        self._logger.info(msg.format(dismissed_num - len(alive_workers), len(alive_workers)))

    # rlock绑定到类实例
    @synchronized
    def is_pool_alive(self):
        if self.worker_size == 0:
            return False
        return True

    # rlock绑定到类实例
    @synchronized
    def worker_size(self):
        # 当前激活的工作线程数量
        return len(self._workers)

    def req_queue_size(self):
        return self.request_queue.qsize()

    def res_queue_size(self):
        return self.result_queue.qsize()

    # rlock绑定到类实例
    @synchronized
    def stop(self):
        '''join 所有的thread,确保所有的线程都执行完毕'''
        self.dismiss_workers(self.worker_size())
        self.join_all_dismissed_workers()
        self._logger.info('Thread Pool has stopped.')

