#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

'''
线程池工作队列的数据结构
'''


class WorkRequest(object):
    def __init__(self, callable, args=None, kwds=None, request_id=None,
                 callback=None, exc_handler=None):
        '''
        请求任务
        :param callable: 可定制的，执行work的函数
        :param args: 列表参数
        :param kwds: 字典参数
        :param request_id: id
        :param callback: 可定制的，处理result_queue队列元素的函数
        :param exc_handler: 可定制的，处理异常的函数
        '''
        if request_id == None:
            self.request_id = id(self)
        else:
            try:
                self.request_id = hash(request_id)
            except TypeError:
                raise TypeError("request_id must be hashable")
        self.callback = callback
        self.exc_callback = exc_handler
        self.callable = callable
        self.args = args or []
        self.kwds = kwds or {}

    def __str__(self):
        msg = "WorkRequest->\nid: {}\ncallable: {}\nargs: {}\nkwargs: {}\ncallback: {}\nexec_handler: {}"
        return msg.format(self.request_id, self.callable, self.args, self.kwds, self.callback, self.exc_callback)


class WorkResult(object):
    def __init__(self, request, status, return_val, error_msg):
        '''
        任务运行结果
        :param request: 对应的请求任务数据
        :param status: 执行成功or失败
        :param return_val: 返回值
        :param error_msg: 异常信息
        '''
        assert isinstance(request, WorkRequest)
        assert isinstance(status, bool)
        self.request = request
        self.status = status
        self.return_val = return_val
        self.error_msg = error_msg

    def __str__(self):
        msg = "WorkResult->\nrequest: {}\nstatus: {}\nreturn value: {}\nerror msg: {}"
        return msg.format(self.request, self.status, self.return_val, self.error_msg)

