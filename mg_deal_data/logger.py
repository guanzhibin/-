#!/usr/bin/env python
#coding: utf-8

import logging
import logging.handlers
import os

class not_log(object):
    def __init__(self):
        pass
    
    def debug(self, msg):
        pass
    
    def error(self, msg):
        pass


def set_log(log_file_name, base_level=logging.NOTSET, max_bytes=20*1024*1024, backup_count=10):
    log_dir = os.path.dirname(log_file_name)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    handler = logging.handlers.RotatingFileHandler(log_file_name,
                                                   maxBytes=max_bytes, backupCount=backup_count)
    formatter = logging.Formatter('[%(asctime)s] %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
    handler.formatter = formatter

    this_logger = logging.getLogger()
    this_logger.addHandler(handler)
    this_logger.setLevel(base_level)

    return this_logger