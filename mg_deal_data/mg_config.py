#!/usr/bin/env python
# coding: utf-8

from ConfigParser import ConfigParser
from logger import *
import argparse
import os
import sys
def parse_config(config_file):
    if not os.path.isfile(config_file):
        sys.exit("No config file: {0}".format(config_file))

    parser = ConfigParser()
    parser.read(config_file)
    config = {}
    config['log_enable'] = parser.getboolean('log', 'enabled') 
    config['log_file'] = parser.get('log', 'file')
    config['r_ip'] = parser.get('d_redis','r_ip')
    config['r_db'] = parser.getint('d_redis','r_db')
    config['r_port'] = parser.getint('d_redis','r_port')
    config['r_password'] = parser.get('d_redis','r_pwd')
    config['m_ip'] = parser.get('mysqldb','m_ip')
    config['m_port'] = parser.getint('mysqldb','m_port')
    config['m_user'] = parser.get('mysqldb','m_user')
    config['m_pwd'] = parser.get('mysqldb','m_pwd')
    config['m_db'] = parser.get('mysqldb','m_db')
    return config

parser = argparse.ArgumentParser(description='task_mange')
parser.add_argument('--config', dest='config_file', default='mg_process.cfg')
parser.add_argument('--no_pay_distribution', action='store_true', default=False,
                    help='Do not start the pay_distribution')

parser.add_argument('--no_deal_servers', action='store_true', default=False,
                    help='Do not start the deal_servers')

parser.add_argument('--no_deal_regist', action='store_true', default=False,
                    help='Do not start the deal_regist')

parser.add_argument('--no_deal_login', action='store_true', default=False,
                    help='Do not start the deal_login')

args = parser.parse_args()
config = parse_config(args.config_file)

if config.get('log_enable'):
    mg_log = set_log(config['log_file'])
else:
    mg_log = not_log()

flag_error_params = {
     '4012':'data_type is notfind',
     '4013':'params error',
     '4010':'request is not find'   
}