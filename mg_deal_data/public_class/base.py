#coding:utf8
from mg_config import mg_log
import traceback
import gevent
import StringIO
def catch_except(class_name):
    def catchexcept(func):
        def add_catchexcept(*args, **kw):
            while 1:
                try:
                    return func(*args,**kw)
                except Exception as e:
                    fp = StringIO.StringIO()    #创建内存文件对象
                    traceback.print_exc(file=fp)
                    message = str(fp.getvalue())
                    mg_log.error('[mg] [%s][%s] %s' % (class_name,func.__name__,message))
                    gevent.sleep(1)
        return add_catchexcept
    return catchexcept