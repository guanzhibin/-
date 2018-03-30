#coding:utf8
import gevent
import random
from mg_config import mg_log
from public_class.base import catch_except
from mysql_data.sqlprocess import connDB
import datetime

##  处理servers_data  
class deal_servers(object):
    @catch_except('deal_servers')
    def __init__(self):
        self.init = True  ## 预留参数
        self.database = connDB() ## 数据库实例
    @catch_except('deal_servers')
    def starting_deal(self,fetch_count=100):
        while 1:
            table_name = 'servers_data'
            key = 'server_data'
            gevent.sleep(0.1)
            #  逻辑代码
            ##  server_data处理代码未完
            datas = self.database.r.lrange(key,0,-1)
            if len(datas)==0:
                gevent.sleep(1)
            for data in datas:
                _data =  self.database.r.get(data)
                if _data:
                    _data = eval(_data)
                    _uid = _data.get('uid','')
                    ##  是否需要回查数据库
                    ret_dict = self.database.select(table_name,dict(uid = _uid))
                    if ret_dict:
                        ## 存在更新 
                        if _data.get('name','')!=ret_dict.get('name',''):
                            ret =self.database.update(table_name,dict(name = _data.get('name')),
                                dict(uid = _uid),update_time = datetime.datetime.now())
                        ret = 1
                    else:
                        ## 不存在添加
                        ret = self.database.add(table_name,dict(
                            uid = _uid,
                            name = _data.get('name','')))
                    if ret:
                        self.database.r.delete(data)
                        self.database.r.lrem(key,data)
                else:
                    ## 如果找不到就直接删除列表中的值
                    self.database.r.lrem(key,data)

## 处理注册
class deal_regist(object):
    @catch_except('deal_regist')
    def __init__(self):
        self.init = True
        self.database = connDB()

    @catch_except('deal_regist')
    def starting(self):
        while 1:
            table_name = 'regist_player'
            key = 'register'
            gevent.sleep(0.1)
            datas = self.database.r.lrange(key,0,-1)
            if not datas:
                gevent.sleep(1)
            for data in datas:
                _data = self.database.r.get(data)
                if _data:
                    _data = eval(_data)
                    s_uid =  _data.get('s_uid','')
                    uid = _data.get('uid','')
                    ch = _data.get('ch','')
                    chmc = _data.get('chmc','')
                    chsc = _data.get('chsc','')
                    check_condition = dict(
                                            s_uid = s_uid,
                                            uid = uid,
                                            ch = ch,
                                            chmc = chmc,
                                            chsc = chsc
                                        )
                    ret_dict = self.database.select(table_name,check_condition)
                    if ret_dict:
                        ##  存在则丢弃掉这条数据
                        self.database.r.lrem(key,data)
                        self.database.r.delete(data)
                    else:
                        ret = self.database.add(table_name,dict(
                                                            s_uid = s_uid,
                                                            uid = uid,
                                                            ch  = ch,
                                                            chmc = chmc,
                                                            chsc = chsc,
                                                            uuid = _data.get('uuid',''),
                                                            ip = _data.get('ip',''),
                                                            os = _data.get('os',''),
                                                            reg_time = datetime.datetime.strptime(_data.get('reg_time'), "%Y-%m-%d %H:%M:%S")
                                                    ))
                        if ret:
                            ## 添加注册表成功 查登录表是否存在
                            login_data = self.database.select('player_login',check_condition)
                            if login_data:
                                reg_date = (_data.get('reg_time','')).split(' ')[0]
                                start_time = reg_date + ' 00:00:00'
                                end_time = reg_date + ' 23:59:59'
                                item = self.database.get_byFilter('mg_daily_newspaper',dict(
                                                                                s_uid = s_uid,
                                                                                start_time = start_time,
                                                                                end_time =end_time
                                                                    ))
                                if item:
                                    ## 在分服日报表修改新登账号字段
                                    self.database.update('mg_daily_newspaper',dict(
                                                                          new_login_accont  = item.get('new_login_accont',0) +1 
                                                                    ),dict(id =item.get('id')))
                                else:
                                    ## 添加一条记录
                                    self.database.add('mg_daily_newspaper', dict(uid = s_uid,new_login_accont = 1))
                            self.database.r.lrem(key,data)
                            self.database.r.delete(data)


                else:
                    self.database.r.lrem(key,data)


###  登录信息
class deal_login(object):
    @catch_except('deal_login')
    def __init__(self):
        self.database = connDB()

    @catch_except('deal_login')
    def starting(self):
        while 1:
            table_name = 'player_login'
            key = 'login'
            datas = self.database.r.lrange(key,0,-1)
            gevent.sleep(0.1)
            if len(datas)==0:
                gevent.sleep(1)
            for data in datas:
                _data  =self.database.r.get(data)
                if _data:
                    _data = eval(_data)
                    s_uid =  _data.get('s_uid','')
                    uid = _data.get('uid','')
                    ch = _data.get('ch','')
                    chmc = _data.get('chmc','')
                    chsc = _data.get('chsc','')
                    login_date = datetime.datetime.strptime(_data.get('login_time'), "%Y-%m-%d %H:%M:%S")
                    start_time = login_date.strftime('%Y-%m-%d') + ' 00:00:00'
                    end_time = login_date.strftime('%Y-%m-%d') + ' 23:59:59'
                    check_condition = dict(
                                            s_uid = s_uid,
                                            uid = uid,
                                            ch = ch,
                                            chmc = chmc,
                                            chsc = chsc,
                                            start_time = start_time,
                                            end_time = end_time
                                        )
                    ret_dict = self.database.get_byFilter(table_name,check_condition)
                    if ret_dict:
                        ##  如果当天存在则丢弃掉这条数据
                        self.database.r.lrem(key,data)
                        self.database.r.delete(data)
                        continue
                    self.database.add(table_name,dict(
                                                s_uid = s_uid,
                                                uid = uid,
                                                ch = ch,
                                                chmc = chmc,
                                                chsc = chsc,
                                                uuid  = _data.get('uuid',''),
                                                pid = _data.get('pid',''),
                                                ip = _data.get('ip',''),
                                                os = _data.get('os',''),
                                                login_time = login_date
                                        ))

                    ## 查找是否是当天新登账号如果存在新登账号数+1
                    new_regist_data = self.database.get_byFilter('regist_player',check_condition)
                    check_data = dict(
                                    s_uid = s_uid,
                                    start_time = start_time,
                                    end_time =end_time
                                    )
                    item = self.database.get_byFilter('mg_daily_newspaper',check_data)

                    if item:
                        ##  修改
                        new_update = dict(login_account = item.get('login_account',0)+1)
                        if new_regist_data:
                            new_update['new_login_accont']  = item.get('new_login_accont',0) + 1
                            ##  查看时间算留存数
                            self.deal_retain(new_regist_data,new_update,login_date,item)
                        self.database.update('mg_daily_newspaper',new_update,dict(id = item.get('id')))
                    else:
                        ## 新增加
                        add_data = dict(login_account = 1,uid = s_uid)
                        if new_regist_data:
                            add_data['new_login_accont'] = 1
                            self.deal_retain(new_regist_data,add_data,login_date)
                        self.database.add('mg_daily_newspaper',add_data)

                    new_login_accont = item.get('new_login_accont',0) + 1
                    ## 还需要留存表处理

                    self.database.r.lrem(key,data)
                    self.database.r.delete(data)
                else:
                    self.database.r.lrem(key,data)
    ##  处理玩家留存
    @catch_except('deal_login')
    def deal_retain(self,new_regist_data,data,login_date,item = dict()):
        reg_time = new_regist_data.get('reg_time')
        if isinstance(reg_time,datetime.datetime):
            login_date_str = login_date.strftime('%Y%m%d')
            ## 次日留存
            tomorrow = reg_time + datetime.timedelta(days=1)
            ## 3日留存
            three_days = reg_time + datetime.timedelta(days=2)
            ##  七日留存
            seventh_day = reg_time + datetime.timedelta(days=6)
            if login_date_str == tomorrow.strftime('%Y%m%d'):
                data['one_retain_days'] = item.get('one_retain_days',0)+1
            elif login_date_str == three_days.strftime('%Y%m%d'):
                data['three_retain_days'] = item.get('three_retain_days',0) + 1
            elif login_date_str == seventh_day.strftime('%Y%m%d'):
                data['seven_retain_days']  = item.get('seven_retain_days',0) + 1
    ## 玩家留存表
    @catch_except('deal_login')
    def player_retian(self,reg_time,login_date,check_data,new_login_accont):
        ## 查找是否有当天记录存在
        player_retian = self.database.get_byFilter('player_pers',check_data)
        player_data  = dict()
        login_date_str = login_date.strftime('%Y%m%d')
        ## 次日留存
        tomorrow = reg_time + datetime.timedelta(days=1)
        three_days= reg_time + datetime.timedelta(days=2)
        fourth_day  = reg_time + datetime.timedelta(days= 3)
        fifth_day =  reg_time + datetime.timedelta(days=4)
        sixth_day = reg_time + datetime.timedelta(days=5)
        seventh_day = reg_time + datetime.timedelta(days=6)
        fifteen_day = reg_time + datetime.timedelta(days=14)
        thirty_day = reg_time + datetime.timedelta(days=29)
        sixty_day = reg_time + datetime.timedelta(days = 59)
        ninety_day = reg_time + datetime.timedelta(days = 89) 
        if login_date_str == tomorrow.strftime('%Y%m%d'):
            player_data['morrow_pers'] = player_retian.get('morrow_pers',0)+1
        elif login_date_str == three_days.strftime('%Y%m%d'):
            player_data['threeth_d_pers'] = player_retian.get('threeth_d_pers',0)  +1
        elif login_date_str == fourth_day.strftime('%Y%m%d'):
            player_data['fourth_d_pers'] = player_retian.get('fourth_d_pers',0) + 1
        elif login_date_str == fifth_day.strftime('%Y%m%d'):
            player_data['fifth_d_pers'] = player_retian.get('fifth_d_pers',0) + 1
        elif login_date_str ==sixth_day.strftime('%Y%m%d'):
            player_data['sixth_d_pers'] = player_retian.get('sixth_d_pers',0)  + 1
        elif login_date_str == seventh_day.strftime('%Y%m%d'):
            player_data['seventh_d_pers'] = player_retian.get('seventh_d_pers',0) +1
        elif login_date_str == fifteen_day.strftime('%Y%m%d'):
            player_data['fifteen_d_pers']  = player_retian.get('fifteen_d_pers',0) + 1
        elif login_date_str == thirty_day.strftime('%Y%m%d'):
            player_data['thirty_d_pers'] = player_retian.get('thirty_d_pers',0) + 1
        elif login_date_str == sixty_day.strftime('%Y%m%d'):
            player_data['sixty_d_pers'] = player_retian.get('sixty_d_pers',0) + 1
        elif login_date_str == ninety_day.strftime('%Y%m%d'):
            player_data['ninety_d_pers']  = player_retian.get('ninety_d_pers',0) + 1 
        player_data['total_newpnum'] = new_login_accont

        if player_retian.get('id'):
            ##  存在id就更新
            self.database.update('player_pers',player_data,dict(id = player_retian['id']))
        else:
            ## 不存在则插入
            player_data['d_date'] = login_date
            player_data['s_uid'] = check_data.get('s_uid','')
            self.database.add('player_pers',player_data)

## 充值
class deal_atm(object):
    @catch_except('deal_atm')
    def __init__(self):
        self.database = connDB()

    @catch_except('deal_atm')
    def starting(self):
        while 1:
            key = 'atm'
            datas = self.database.r.lrange(key,0,-1)
            gevent.sleep(0.1)
            if len(datas)==0:
                gevent.sleep(1)
            for data in datas:
                _data = self.database.r.get(data)
                if _data:
                    _data = eval(_data)
                    s_uid =  _data.get('s_uid','')
                    uid = _data.get('uid','')
                    ch = _data.get('ch','')
                    chmc = _data.get('chmc','')
                    chsc = _data.get('chsc','')
                else:
                    self.database.r.lrem(key,data)


##3 算当天新增并且有登录的玩家个数
class deal_test(object):

    def __init__(self):
        self.database = connDB()

    def starting(self):
        import time
        while 1:
            gevent.sleep(1)
            now  = datetime.datetime.now()
            ymd = int(now.strftime('%Y%m%d'))

            ## 当天注册的玩家数
            regist_data = self.database.player_regist(ymd)
            new_data = []
            reg_list = []
            log_list = []
            key_data = dict()
            t = time.time()
            if regist_data:
                ### 当天登录的玩家数
                login_data  = self.database.player_login(ymd)

                if login_data:
                    for reg_data in regist_data:
                        reg_ = reg_data.get('ch')+','+reg_data.get('s_uid')+','+str(reg_data.get('uid'))
                        if reg_ not in reg_list:
                            reg_list.append(reg_)

                    for lg_data in login_data:
                        log_ = lg_data.get('ch')+','+lg_data.get('s_uid')+','+str(lg_data.get('uid'))
                        if log_ not in log_list:
                            log_list.append(log_)

            # ret_data = [reg for reg in reg_list if reg in log_list]
            for reg in reg_list:
                if reg in log_list:
                    print reg.split(',')
                    new_data.append(reg)
            # print new_data,len(new_data)
            print time.time() - t
            gevent.sleep(20000)



 