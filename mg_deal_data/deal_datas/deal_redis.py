#coding:utf8
import gevent
from public_class.base import catch_except
from public_class import caching, deal_base
from mysql_data.sqlprocess import connDB
from collections import Counter
import datetime
import hashlib
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import copy
import time
from mg_config import mg_log
Prefix = ':data:'

ONE_DAY_S = 60*60*24

_redis = connDB()
r = _redis.r
##  处理servers_data  
class dealServersofRedis(object):
    @catch_except('dealServersofRedis')
    def __init__(self):
        self.init = True  ## 预留参数
        self.database = connDB() ## 数据库实例
    @catch_except('dealServersofRedis')
    def starting(self,fetch_count=100):
        ##数量不会很大还要检查是否存在，单条插入就好
        while 1:
            table_name = 'servers_data'
            key = Prefix +'server_data'
            gevent.sleep(0.1)
            #  逻辑代码
            ##  server_data处理代码
            datas = r.lrange(key,0,100)
            if len(datas)==0:
                gevent.sleep(2)
            for data in datas:
                gevent.sleep(0.01)
                _data = json.loads(data)

                _uid = _data.get('uid','')
                ##  是否需要回查数据库
                ret_dict = self.database.select(table_name,dict(uid = _uid))
                if ret_dict:
                    ## 存在更新 
                    if _data.get('name','')!=ret_dict.get('name',''):
                        ret =self.database.update(table_name,dict(name = _data.get('name'),update_time = datetime.datetime.now()),
                            dict(uid = _uid))
                    ret = 1
                else:
                    ## 不存在添加
                    ret = self.database.add(table_name,dict(
                        uid = _uid,
                        name = _data.get('name','')))
                if ret:
                    r.lrem(key,data,1)


## 处理启动设备 
class deal_fetch_server(object):

    @catch_except('deal_fetch_server')
    def __init__(self):
        self.database = connDB()

    @catch_except('deal_fetch_server')
    def starting(self):
        while 1:
            cols = ('uuid','ch','os','dn','fetch_time','d_date','new_flag')
            key = Prefix+'fetch_server'
            datas = r.lrange(key,0,1000)
            gevent.sleep(0.1)
            datas_len = len(datas)
            if len(datas) == 0:
                gevent.sleep(2)
            values = []
            for data in datas:
                gevent.sleep(0.01)
                _data = json.loads(data)
                # _data = eval(data)
                if _data.get('suid'):
                    r.lrem(key,data,1)
                    continue
                _data.pop('suid',None)
                fetch_time  = datetime.datetime.utcfromtimestamp(int(_data.get('fetch_time'))) + datetime.timedelta(hours=8)
                _data['fetch_time'] = fetch_time
                _data['d_date'] = int(fetch_time.strftime('%Y%m%d'))
                chsc = _data.pop('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                ch = _data.get('ch','')
                _data['new_flag'] =0
                fetch_server_d =  self.database.select('fetch_server',dict(uuid = _data.get('uuid','')))
                if not fetch_server_d:
                    _data['new_flag'] =1
                # ret = self.database.add('fetch_server',_data)
                # if ret:
                #     r.lrem(key,data)
                value = []
                for i in cols:
                    _value = _data.get(i)
                    if _value is not None:
                        value.append(_value)
                    else :
                        print i,_value,key
                values.append(tuple(value))
            if values:
                sql = self.database.add2('fetch_server',cols)
                self.database.addall(sql,values)
                r.ltrim(key,datas_len,-1)
## 注册
class dealRegisterOfRedis(object):

    @catch_except('dealRegisterOfRedis')
    def __init__(self):
        self.database = connDB()

    @catch_except('dealRegisterOfRedis')
    def starting(self):
        while 1:
            key = Prefix + 'register'
            datas = r.lrange(key,0,1000)
            gevent.sleep(0.1)
            if len(datas) == 0:
                gevent.sleep(2)
            for data in datas:
                gevent.sleep(0.01)
                _data = json.loads(data)
                reg_time  = datetime.datetime.utcfromtimestamp(int(_data.get('reg_time'))) + datetime.timedelta(hours=8)
                _data['reg_time'] = reg_time
                d_date = int(reg_time.strftime('%Y%m%d'))
                _data['d_date'] = d_date
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                ch = _data.get('ch','')

                if ch:
                    channel_ch_server(self.database,ch,_data.get('s_uid',''))

                ### 加回查
                check_atm = dict(s_uid = _data.get('s_uid',''),
                                d_date = d_date,
                                ch = _data.get('ch',''),
                                uid = _data.get('uid',''),
                                orderstate = 5
                            )
                check_atm_data = self.database.select('atm',check_atm)
                if check_atm_data:
                    self.database.update('atm',dict(new_reg_flag =1),dict(id = check_atm_data.get('id')))
                ret = self.database.add('player_regist',_data)
                r.lrem(key,data,1)

## 玩家创建
class dealCreateOfRedis(object):

    @catch_except('dealCreateOfRedis')
    def __init__(self):
        self.database = connDB()

    @catch_except('dealCreateOfRedis')
    def starting(self):
        while 1:
            key = Prefix + 'create_player'
            datas = r.lrange(key,0,1000)
            gevent.sleep(0.1)
            if len(datas) == 0:
                gevent.sleep(2)
            for data in datas:
                gevent.sleep(0.01)
                _data = json.loads(data)
                create_time  = datetime.datetime.utcfromtimestamp(int(_data.pop('create_player_time',''))) + datetime.timedelta(hours=8)
                _data['create_time'] = create_time
                _data['d_date'] = int(create_time.strftime('%Y%m%d'))
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                ch = _data.get('ch','')
                if ch:
                    channel_ch_server(self.database,ch,_data.get('s_uid',''))
                ret = self.database.add('player_create',_data)
                r.lrem(key,data,1)


def channel_ch_server(database,ch,s_uid):
    ch_id = caching.get(ch)
    if ch_id:
        ch_id  = int(ch_id)
        channel_server_key = str(ch_id) + str(s_uid)
        _check_ch_s = dict(ch_id =ch_id, s_uid = s_uid)
        _ch_s_data = caching.get(channel_server_key)
        if not  _ch_s_data:
            ch_s_data = database.select('channel_server',_check_ch_s)
            if not ch_s_data:
                database.add('channel_server',_check_ch_s)
            caching.set(channel_server_key,1)
    else:
        ch_id = check_ch(database, 'channels',dict(name = ch))
        if ch_id:
            caching.set(ch,ch_id)
            channel_server_key = str(ch_id) + str(s_uid)
            _check_ch_s = dict(ch_id =ch_id, s_uid = s_uid)
            _ch_s_data = caching.get(channel_server_key)
            if not  _ch_s_data:
                ch_s_data = database.select('channel_server',_check_ch_s)
                if not ch_s_data:
                    database.add('channel_server',_check_ch_s)
                caching.set(channel_server_key,1)
## 登录
class dealLoginOfREdis(object):

    @catch_except('dealLoginOfREdis')
    def __init__(self):
        self.database = connDB()

    @catch_except('dealLoginOfREdis')
    def starting(self):
        while 1:
            key = Prefix + 'login'
            gevent.sleep(0.1)
            datas = r.lrange(key,0,1000)
            if len(datas)==0:
                gevent.sleep(2)
            for data in datas:
                gevent.sleep(0.01)
                _data = json.loads(data)
                login_time = datetime.datetime.utcfromtimestamp(int(_data.get('login_time'))) \
                +datetime.timedelta(hours=8)
                _data['login_time'] = login_time
                _data['d_date'] = int(login_time.strftime('%Y%m%d'))
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                ch = _data.get('ch','')
                if ch:
                    channel_ch_server(self.database,ch,_data.get('s_uid',''))
                self.update_whale(_data)
                ret = self.database.add('player_login',_data)
                if ret:
                    r.lrem(key,data,1)
    ## 更新鲸鱼用户最后的活跃时间
    @catch_except('dealLoginOfREdis')
    def update_whale(self,data):
        update_cond = dict(
                            channel_name = data.get('ch',''),
                            s_uid = data.get('s_uid',''),
                            uid = data.get('uid',0),
                            pid = data.get('pid',0)
                        )
        update_data = dict(llogin_time = data.get('login_time'),ac_level = data.get('level',1))
        self.database.update('whale_player',update_data,update_cond)
## 登出
class dealLogoutOfREdis(object):

    @catch_except('dealLogoutOfREdis')
    def __init__(self):
        self.database = connDB()

    @catch_except('dealLogoutOfREdis')
    def starting(self):
        while 1:
            gevent.sleep(0.1)
            key = Prefix + 'logout'
            datas = r.lrange(key,0,1000)
            if len(datas)==0:
                gevent.sleep(2)
            for data in datas:
                gevent.sleep(0.01)
                _data = json.loads(data)
                logout_time = datetime.datetime.utcfromtimestamp(int(_data.get('logout_time')))+ \
                datetime.timedelta(hours=8)
                _data['logout_time'] = logout_time
                _data['d_date'] = int(logout_time.strftime('%Y%m%d'))
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                ch = _data.get('ch','')
                if ch:
                    channel_ch_server(self.database,ch,_data.get('s_uid',''))
                ret = self.database.add('player_logout',_data)
                if ret:
                    r.lrem(key,data,1)

## 玩家消费
class dealPlayerPayOfREdis(object):

    @catch_except('dealPlayerPayOfREdis')
    def __init__(self):
        self.database = connDB()
    @catch_except('dealPlayerPayOfREdis')
    def starting(self):
        while 1:
            gevent.sleep(0.1)
            cols = ('uuid','uid','pid','account','player','ip','ch','chmc',
                    'chsc','os','cost_name','cost_count','left_count','time','s_uid','d_date','unique_id')
            key =Prefix + 'pay'
            datas = r.lrange(key,0,1000)
            datas_len = len(datas)
            if datas_len==0:
                # print time.time() - self.t
                gevent.sleep(2)
            values = []
            for data in datas:
                # gevent.sleep(0.1)
                _data = json.loads(data)
                if not _data.get('unique_id'):
                    _data['unique_id'] = ''
                p_time = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + \
                datetime.timedelta(hours=8)
                _data['time'] = p_time
                _data['d_date'] = int(p_time.strftime('%Y%m%d'))
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                ch = _data.get('ch')
                if ch:
                    channel_ch_server(self.database,ch,_data.get('s_uid',''))
                value = []
                for i in cols:
                    _value = _data.get(i)
                    if _value is not None:
                        value.append(_value)
                    else :
                        print i,_value,key
                values.append(tuple(value))

            if values:
                sql = self.database.add2('pay',cols)
                self.database.addall(sql,values)
                r.ltrim(key,datas_len,-1)

## 玩家充值
class dealPlayerTopupsOfREdis(object):

    @catch_except('dealPlayerTopupsOfREdis')
    def __init__(self):
        self.database = connDB()

    @catch_except('dealPlayerTopupsOfREdis')
    def starting(self):
        while 1:
            gevent.sleep(0.1)
            key =Prefix + 'atm'
            datas = r.lrange(key,0,1000)
            if len(datas)==0:
                gevent.sleep(1)
            
            for data in datas:
                mg_log.info('%s' % (data))
                # gevent.sleep(0.01)
                _data = json.loads(data)
                gameorderid = _data.get('gameorderid',None)
                if gameorderid is None:
                    r.lrem(key,data,1)
                    mg_log.warning('%s' % (data))
                    continue
                orderstate = _data.get('orderstate',2)
                ___data =  self.database.select('atm',dict(gameorderid=gameorderid, orderstate = orderstate))
                if ___data:
                    r.lrem(key,data,1)
                    mg_log.warning('%s' % (data))
                    continue
                a_ct = datetime.datetime.utcfromtimestamp(int(_data.get('a_ct'))) + \
                datetime.timedelta(hours=8)
                _data['a_ct'] = a_ct
                _data['time'] = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + \
                datetime.timedelta(hours=8)
                _data['d_date']  = int(_data.get('time').strftime('%Y%m%d'))
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                ch =  _data.get('ch','')

                _check_condition = dict(
                                        s_uid = _data.get('s_uid',''),
                                        uid = _data.get('uid',''),
                                        ch =ch ,
                                        chmc = _data.get('chmc',''),
                                        chsc = _data.get('chsc',''),
                                        orderstate = 5
                                    )
                __data =  self.database.select('atm',_check_condition)
                _check_condition.pop('orderstate',None)
                orderstate = _data.get('orderstate',0)
                if orderstate!=5:
                    _data['ltv_flag'] = 1
                ##  判断是否是当天新增付费玩家
                if not __data and orderstate==5:
                    _data['new_pay_flag'] = 1
                if _data.get('new_pay_flag',0)==1:
                    _check_condition['d_date'] = _data.get('d_date',0)
                    __new_reg = self.database.select('player_regist',_check_condition)
                    if __new_reg:
                        _data['new_reg_flag'] = 1
                if ch:
                    channel_ch_server(self.database,ch,_data.get('s_uid',''))
                ret = self.database.add('atm',_data)
                if orderstate == 5:
                    self.whale_player(_data)
                if ret:
                    r.lrem(key,data,1)

    @catch_except('dealPlayerTopupsOfREdis')
    def whale_player(self,data):
        ##查询条件channel_name,s_uid,uid,pid
        _check = dict(
                    channel_name = data.get('ch',''),
                    s_uid = data.get('s_uid',''),
                    uid = data.get('uid',0),
                    pid = data.get('pid',0)
            )
        ## 
        _data = dict(last_pay_time = data.get('time'),
                    rechargemoney = data.get('ordermoney',0),
                    ac_level = data.get('rechargelevel',0),
                    has_diamond = data.get('cornown',0),
                    cons_diamond = data.get('cornused',0),

                )
        whale_data = self.database.select('whale_player',_check)
        if not whale_data:
            ## 不存在插入数据
            _check['ch'] = _check.pop('channel_name','')
            _check.pop('pid','')
            reg_data = self.database.select('player_regist',_check)
            if reg_data:
                _data['reg_time'] = reg_data.get('reg_time')
            ## 获取服务器名称
            s_uid  = data.get('s_uid')
            server_name = caching.get(s_uid)
            if server_name is None:
                server_name = ''
                _server_d = self.database.select('servers_data',dict(uid =s_uid ))
                if _server_d:
                    server_name = _server_d.get('name','')
                    caching.set(s_uid,server_name)
            _data['server_name'] = server_name
            _data['channel_name'] = data.get('ch','')
            _data['s_uid'] = s_uid
            _data['player'] = data.get('player','')
            _data['uid'] = data.get('uid',0)
            _data['pid'] = data.get('pid',0)
            _data['uuid'] = data.get('uuid','')
            _data['fp_level'] = data.get('rechargelevel',0)
            self.database.add('whale_player',_data)
        else:
            ## 存在更新数据
            _data['rechargemoney'] = _data.get('rechargemoney',0) + float(whale_data.get('rechargemoney',0))
            self.database.update('whale_player',_data,dict(id = whale_data['id']))



## 每天十二点报送在线情况
class dealDONLOfREdis(object):

    @catch_except('dealDONLOfREdis')
    def __init__(self):
        self.database = connDB()
        ## 每天12激活
        self.activation = True
    @catch_except('dealDONLOfREdis')
    def starting(self):
        while 1:
            gevent.sleep(0.1)
            hms = datetime.datetime.now().strftime('%H%M%S')
            if hms=='000000':
                self.activation  =True

            if self.activation is True:
                key =Prefix + 'daily_online'
                datas = r.lrange(key,0,1000)
                if len(datas)==0:
                    gevent.sleep(1)
                if len(datas) ==0 and hms=='002000':
                    ##  20分钟时如果没有任务时设置
                    self.activation  = False
                for data in datas:
                    gevent.sleep(0.01)
                    _data = json.loads(data)
                    start_time = datetime.datetime.utcfromtimestamp(int(_data.pop('time',''))) + \
                    datetime.timedelta(hours=8)
                    _data['start_time'] = start_time
                    _data['end_time'] = datetime.datetime.utcfromtimestamp(int(_data.pop('shutdown',''))) + \
                    datetime.timedelta(hours=8)
                    _data['d_date'] = int(start_time.strftime('%Y%m%d'))
                    ret = self.database.add('daily_online',_data)
                    if ret:
                        self.database.update('mg_daily_newspaper',dict(
                                                                    average_number_online = _data.get('avg',''),
                                                                    highest_online = _data.get('num','')
                                            ),dict(
                                                s_uid = _data.get('s_uid',''),
                                                d_date = _data.get('d_date','')
                                            ))
                        r.lrem(key,data,1)

## 每天十二点报送等级分布
class dealDLEVELOfREdis(object):

    @catch_except('dealDLEVELOfREdis')
    def __init__(self):
        self.database = connDB()
        ## 每天12激活
        self.activation = True
    @catch_except('dealDLEVELOfREdis')
    def starting(self):
        while 1:
            gevent.sleep(0.1)
            hms = datetime.datetime.now().strftime('%H%M%S')
            if hms=='000000':
                self.activation  =True
            if self.activation is True:
                key =Prefix +  'daily_levelmap'
                datas =r.lrange(key,0,1000)
                if len(datas)==0:
                    gevent.sleep(1)

                if len(datas) ==0 and hms=='002000':
                    self.activation = False
                for data in datas:
                    gevent.sleep(0.01)
                    _data = json.loads(data)
                    l_time = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                    _data['time'] = l_time
                    _data['d_date'] = int(l_time.strftime('%Y%m%d'))
                    __data = copy.deepcopy(_data)
                    _data['all'] = json.dumps(_data.get('all'))
                    _data['date'] = json.dumps(_data.get('date'))
                    _data['freeze'] = json.dumps(_data.get('freeze'))
                    _data['left'] = json.dumps(_data.get('left'))
                    self.deal_level_map(__data)

                    ret = self.database.add('daily_levelmap',_data)
                    if ret:
                        r.lrem(key,data,1)
    ## 等级分布

    def deal_level_map(self,data):

        t_time = data.get('time') # + datetime.timedelta(days= -1)
        s_uid = data.get('s_uid','')
        d_date = data.get('d_date',0)
        all_d = dict() 
        new_level_map = data.get('date',dict)
        all_level_map = data.get('all',dict())
        freeze_level_map = data.get('freeze',dict())
        left_level_map = data.get('left',dict())
        self.deal_d(all_d, all_level_map, t_time, s_uid, d_date, 'all')
        self.deal_d(all_d, new_level_map, t_time, s_uid, d_date, 'date')
        self.deal_d(all_d, freeze_level_map, t_time, s_uid, d_date, 'freeze')
        self.deal_d(all_d, left_level_map, t_time, s_uid, d_date, 'left')

        for v in all_d.values():
            ##  全部等级
            if v.get('all'):
                self.add_mysql(v, 1, 'all')
            ## 新增等级
            if v.get('date'):
                self.add_mysql(v, 2, 'date')
            if v.get('left'):
                self.add_mysql(v, 3, 'left')
    def add_mysql(self,data, type_flag, key):
        ## 查找是否存在，不存在插入
        _time = data.get('time')
        ## 20171215修改流失时间倒退7天
        if type_flag == 3:
            _time = _time + datetime.timedelta(days = -7)
        
        check_daily_levelbase = self.database.select('daily_levelbase',dict(
                                            channel_name = data.get('channel_name'),
                                            s_uid = data.get('s_uid',''),
                                            d_date = int(_time.strftime('%Y%m%d')),
                                            type_flag = type_flag

            ))
        if not check_daily_levelbase:
            last_id =self.database.add('daily_levelbase',dict(channel_name =data.get('channel_name'),
                                                    s_uid = data.get('s_uid',''),
                                                    server_name = data.get('server_name'),
                                                    time = _time,
                                                    d_date = int(_time.strftime('%Y%m%d')),
                                                    type_flag = type_flag
                ))
            all_ = data.get(key,dict())
            for k, v in all_.items():
                self.database.add('level_dis',dict(b_id = last_id,
                                                    level = k,
                                                    num = v
                                                ))
    def deal_d(self,all_d,all_level_map, t_time, s_uid, d_date, key):
        for all_k,all_v in all_level_map.items():
            all_index = all_k.find('::')
            channel_name = all_k[:all_index]
            if all_d.get(channel_name):
                all_ = all_d[channel_name].get(key,dict())
                all_,all_v = Counter(all_), Counter(all_v)
                all_d[channel_name][key] = dict(all_ + all_v)
            else:
                server_name  =  ''
                server_d = self.database.select('servers_data',dict(uid = s_uid))
                if server_d:
                    server_name = server_d.get('name','')
                all_d[channel_name] = {
                                'time' : t_time,
                                's_uid' : s_uid,
                                'd_date' : d_date,
                                key : all_v,
                                'channel_name':channel_name,
                                'server_name' : server_name

                    }

## 产出消耗-金币
class dealLgoldOfREdis(object):

    @catch_except('dealLgoldOfREdis')
    def __init__(self):
        self.database = connDB()
    @catch_except('dealLgoldOfREdis')
    def starting(self):
        ## 预计量大的数据
        cols = ('status_flag','d_date','s_uid','ch','type','time',
            'uid','pid','have','diff','chmc','chsc','unique_id')
        while 1:
            gevent.sleep(0.1)
            key = Prefix + 'lgold'
            datas = r.lrange(key,0,2000)
            datas_len = len(datas)
            if datas_len==0:
                gevent.sleep(2)
            values = []
            for data in datas:
                _data = json.loads(data)
                if _data.get('unique_id') is None:
                    _data['unique_id'] = ''
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') + '_'+chsc
                _time  = datetime.datetime.utcfromtimestamp(int(_data.get('time')))  + datetime.timedelta(hours=8)
                _data['time'] = _time
                _data['d_date'] = int(_time.strftime('%Y%m%d'))
                _type = _data.get('type')
                s_uid =  _data.get('s_uid','')
                p_name = '金币'
                _id =cons_mode(key,self.database,_type,p_name)
                _data['type'] = _id
                have = int(_data.get('have',0))
                diff = int(_data.get('diff',0))
                if diff<0:
                    status_flag =2
                else:
                    status_flag = 1
                _data['status_flag'] = status_flag
                value = []
                for i in cols:
                    _value = _data.get(i)
                    if _value is not None:
                        value.append(_value)
                    else :
                        print i,_value,key
                values.append(tuple(value))

            if values:
                sql = self.database.add2('lgold',cols)
                self.database.addall(sql,values)
                r.ltrim(key,datas_len,-1)

def check_cons_mode(database, table_name, csm_codition):
    p_data = database.select(table_name, csm_codition)
    if p_data:
        csm_id = p_data.get('id')
    else:
        csm_id = database.add(table_name, csm_codition)
    return csm_id


def cons_mode(key,database,_type,p_name):
    pid = caching.get(key)
    if pid is None:
        pid = check_cons_mode(database,'cons_mode',dict(name = p_name))
        caching.set(key,pid)
    type_key = _type + str(pid)
    _id = caching.get(type_key)
    if _id is None:
        mode_data = database.select('cons_mode',dict(name = _type, pid = pid))
        if not mode_data:
            _id = database.add('cons_mode',dict(name = _type, pid = pid))
        else:
            _id = mode_data['id']
        caching.set(type_key, _id)
    return _id

## 产出消耗-钻石
class dealLdiamondfREdis(object):

    @catch_except('dealLdiamondfREdis')
    def __init__(self):
        self.database = connDB()

    @catch_except('dealLdiamondfREdis')
    def starting(self,limit=500):
        ## 预计是量大的数据
        cols = ('status_flag','d_date','s_uid','ch','type','time',
            'uid','pid','have','diff','chmc','chsc','unique_id')
        while 1:
            gevent.sleep(0.1)
            key =Prefix + 'ldiamond'
            datas = r.lrange(key,0,limit)
            datas_len = len(datas)
            if datas_len==0:
                gevent.sleep(2)
            values = []
            for data in datas:
                _data = json.loads(data)
                if _data.get('unique_id') is None:
                    _data['unique_id'] = ''
                _time  = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                _data['time'] = _time
                _data['d_date'] =  int(_time.strftime('%Y%m%d'))
                s_uid =  _data.get('s_uid','')
                _type = _data.get('type')
                p_name = '钻石'
                _id =cons_mode(key,self.database,_type,p_name)
                _data['type'] = _id
                have = int(_data.get('have',0))
                diff = int(_data.get('diff',0))
                status_flag = 0
                if diff<0:
                    status_flag = 2
                else:
                    status_flag = 1
                if _type.find('充值')>-1:
                    status_flag = 3
                _data['status_flag'] = status_flag
                value = []
                for i in cols:
                    _value = _data.get(i)
                    if _value is not None:
                        value.append(_value)
                    else :
                        print i,_value,key
                values.append(tuple(value))

            if values:
                sql = self.database.add2('ldiamond',cols)
                self.database.addall(sql,values)
                r.ltrim(key,datas_len,-1)


## 产出消耗-探索值
class dealexploreREdis(object):

    @catch_except('dealexploreREdis')
    def __init__(self):
        self.database = connDB()

    @catch_except('dealexploreREdis')
    def starting(self,limit=500):
        ## 预计是量大的数据
        cols = ('status_flag','d_date','s_uid','type','time','uid','pid','have','diff','item_desc','unique_id','goods_id')
        while 1:
            gevent.sleep(0.1)
            key =Prefix + 'lexplore'
            datas = r.lrange(key,0,limit)
            datas_len = len(datas)
            if datas_len==0:
                gevent.sleep(2)
            values = []
            for data in datas:
                _data = json.loads(data)
                if _data.get('unique_id') is None :
                    _data['unique_id'] = ''
                _time  = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                _data['time'] = _time
                _data['d_date'] =  int(_time.strftime('%Y%m%d'))
                s_uid =  _data.get('s_uid','')
                _type = _data.get('type')
                p_name = '探索值'
                _id = cons_mode(key,self.database,_type,p_name)
                _data['type'] = _id
                have = int(_data.get('have',0))
                diff = int(_data.get('diff',0))
                _data['goods_id'] = _data.pop('character',0)
                item_desc = str(_data.pop('max','0'))+',' + str(_data.pop('max_chg','0'))
                status_flag = 0
                if diff<0:
                    status_flag = 2
                else:
                    status_flag = 1
                if _type.find('充值')>-1:
                    status_flag = 3
                _data['status_flag'] = status_flag
                _data['item_desc'] = item_desc
                value = []
                for i in cols:
                    _value = _data.get(i)
                    if _value is not None:
                        value.append(_value)
                    else :
                        print i,_value,key
                values.append(tuple(value))

            if values:
                sql = self.database.add2('lexplore',cols)
                self.database.addall(sql,values)
                r.ltrim(key,datas_len,-1)


## 产出消耗-贡献值
class dealelcontribute(object):

    @catch_except('dealelcontribute')
    def __init__(self):
        self.database = connDB()

    @catch_except('dealelcontribute')
    def starting(self,limit=500):
        ## 预计是量大的数据
        cols = ('status_flag','d_date','s_uid','type','time','uid','pid','have','diff','unique_id','guild_id')
        while 1:
            gevent.sleep(0.1)
            key =Prefix + 'lcontribute'
            datas = r.lrange(key,0,limit)
            datas_len = len(datas)
            if datas_len==0:
                gevent.sleep(2)
            values = []
            for data in datas:
                _data = json.loads(data)
                if _data.get('unique_id') is None:
                    _data['unique_id'] = ''
                _time  = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                _data['time'] = _time
                _data['d_date'] =  int(_time.strftime('%Y%m%d'))
                s_uid =  _data.get('s_uid','')
                _type = _data.get('type')
                p_name = '贡献值'
                _id = cons_mode(key,self.database,_type,p_name)
                _data['type'] = _id
                have = int(_data.get('have',0))
                diff = int(_data.get('diff',0))
                status_flag = 0
                if diff<0:
                    status_flag = 2
                else:
                    status_flag = 1
                _data['status_flag'] = status_flag
                value = []
                for i in cols:
                    _value = _data.get(i)
                    if _value is not None:
                        value.append(_value)
                    else :
                        print i,_value,key
                values.append(tuple(value))

            if values:
                sql = self.database.add2('lcontribute',cols)
                self.database.addall(sql,values)
                r.ltrim(key,datas_len,-1)


def check_server_name(s_uid,database):
    name = caching.get(s_uid)
    if name is None:
        server_d = database.select('servers_data',dict(uid = s_uid))
        if server_d:
            name = server_d.get('name')
            caching.set(s_uid, name)
    return name

## 产出消耗-物品
class dealLitemfREdis(object):

    @catch_except('dealLitemfREdis')
    def __init__(self):
        self.database = connDB()

    @catch_except('dealLitemfREdis')
    def starting(self,limit=500):
        ## 预计是量大的数据
        litem_cols = ('uid','pid', 'type','created', 'deleted', 'time','s_uid', 'd_date',
            'ch', 'chmc','chsc','unique_id')
        while 1:
            gevent.sleep(0.1)
            key = Prefix + 'litem'
            datas = r.lrange(key,0,limit)
            datas_len = len(datas)
            if datas_len==0:
                gevent.sleep(2)
            values = []
            for data in datas:
                # gevent.sleep(0.01)
                _data = json.loads(data)
                if _data.get('unique_id') is None:
                    _data['unique_id'] =''
                _time = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                _data['time'] =  _time
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                _data['d_date'] = int(_time.strftime('%Y%m%d'))
                _type = _data.get('type')
                p_name = '物品'
                _id =cons_mode(key,self.database,_type,p_name)
                _data['type'] = _id
                value = []
                for i in litem_cols:
                    _value = _data.get(i)
                    if _value is not None:
                        value.append(_value)
                values.append(tuple(value))
            if values:
                sql = self.database.add2('litem',litem_cols)
                self.database.addall(sql,values)
                r.ltrim(key,datas_len,-1)


## 产出消耗-装备
class deallequipmentfREdis(object):

    @catch_except('deallequipmentfREdis')
    def __init__(self):
        self.database = connDB()

    @catch_except('deallequipmentfREdis')
    def starting(self,limit=500):
        ## 预计是量大的数据
        litem_cols = ('uid','pid', 'type','created', 'deleted', 'time','s_uid', 'd_date',
            'ch', 'chmc','chsc','unique_id')
        while 1:
            gevent.sleep(0.1)
            key = Prefix + 'lequipment'
            datas = r.lrange(key,0,limit)
            datas_len = len(datas)
            if datas_len==0:
                gevent.sleep(2)
            values = []
            for data in datas:
                # gevent.sleep(0.01)
                _data = json.loads(data)
                if _data.get('unique_id') is None:
                    _data['unique_id'] = ''
                _time = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                _data['time'] =  _time
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                _data['d_date'] = int(_time.strftime('%Y%m%d'))
                _type = _data.get('type')
                p_name = '装备'
                _id = cons_mode(key,self.database,_type,p_name)
                _data['type'] = _id
                value = []
                for i in litem_cols:
                    _value = _data.get(i)
                    if _value is not None:
                        value.append(_value)
                values.append(tuple(value))
            if values:
                sql = self.database.add2('lequipment',litem_cols)
                self.database.addall(sql,values)
                r.ltrim(key,datas_len,-1)
## 付费点
class click_pay_tomysql(object):

    @catch_except('click_pay_tomysql')
    def __init__(self):
        self.database = connDB()

    @catch_except('click_pay_tomysql')
    def starting(self):
        while 1:
            gevent.sleep(0.1)
            key =Prefix +  'click_pay'
            datas = r.lrange(key,0,500)
            if len(datas)==0:
                gevent.sleep(1)
            for data in datas:
                _data = json.loads(data)
                _time = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                _data['time'] =  _time
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                _data['d_date'] = int(_time.strftime('%Y%m%d'))
                ret = self.database.add('click_pay',_data)
                if ret:
                    r.lrem(key,data,1)


## 体力购买
class physical_buy_tomysql(object):

    @catch_except('physical_buy_tomysql')
    def __init__(self):
        self.database = connDB()

    @catch_except('physical_buy_tomysql')
    def starting(self):
        while 1:
            gevent.sleep(0.1)
            key =Prefix + 'physical_buy'
            datas =r.lrange(key,0,1000)
            if len(datas)==0:
                gevent.sleep(2)
            for data in datas:
                # gevent.sleep(0.01)
                _data = json.loads(data)
                # _data = eval(data)
                _time = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                _data['time'] =  _time
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                hsm = _time.strftime('%H%M%S')
                if hsm <'060000':
                    d_date = int((_time +datetime.timedelta(days=-1)).strftime('%Y%m%d'))
                else:
                    d_date = int(_time.strftime('%Y%m%d'))
                _data['d_date'] = d_date
                ret = self.database.add('physical_buy',_data)
                if ret:
                    r.lrem(key,data,1)


def check_ch(database, table_name, ch_condition):
    data = database.select(table_name, ch_condition)
    if data:
        ch_id = data.get('id')
    else:
        ch_id = database.add(table_name, ch_condition)
    return ch_id



## 每日库存
class currency_gold_diamond(object):

    @catch_except('currency_gold_diamond')
    def __init__(self):
        self.database = connDB()
    @catch_except('currency_gold_diamond')
    def starting(self):
        while 1:
            gevent.sleep(0.1)
            key = Prefix + 'currency_daliy_beginning'
            datas = r.lrange(key, 0, 1000)
            if len(datas)==0:
                gevent.sleep(20)
            for data in datas:
                # gevent.sleep(0.01)
                _data = json.loads(data)
                _time = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                _data['time'] =  _time
                yes_time = _time + datetime.timedelta(days = -1)
                now_ymd = int(_time.strftime('%Y%m%d'))
                yes_ymd = int(yes_time.strftime('%Y%m%d'))
                ## 更新昨天的期末库存
                gold = _data.get('gold',0)
                diamond = _data.get('diamond',0)
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                ch  = _data.get('ch','')
                s_uid = _data.get('s_uid','')
                _condition = dict(
                            channel_name = ch,
                            s_uid = s_uid
                    )
                if gold:
                    __condition = copy.deepcopy(_condition)
                    __condition['c_type'] = 2
                    if yes_ymd:
                        __condition['d_date'] = yes_ymd
                        # __condition['create_time'] = yes_time
                        self.update_currency(gold,__condition)
                    if now_ymd:
                        __condition['d_date'] = now_ymd
                        ## 查找是否存在
                        now_data = self.database.select('currency_cond',__condition)
                        if now_data:
                            self.database.update('currency_cond',dict(start_inventory = gold),
                                    dict(id = now_data['id'])
                                )
                        else:
                            server_data = self.database.select('servers_data',dict(uid = _data.get('s_uid','')))
                            if server_data:
                                __condition['server_name'] = server_data.get('name','')
                            __condition['start_inventory'] = gold
                            __condition['create_time'] = _time
                            self.database.add('currency_cond',__condition)
                if diamond:
                    _condition['c_type'] = 1
                    if yes_ymd:
                        _condition['d_date'] = yes_ymd
                        self.update_currency(diamond, _condition)
                    if now_ymd:
                        _condition['d_date'] = now_ymd
                        now_data = self.database.select('currency_cond',_condition)
                        if now_data:
                            self.database.update('currency_cond',dict(start_inventory = diamond),dict(id = now_data['id']))
                        else:
                            server_data = self.database.select('servers_data',dict(uid = _data.get('s_uid','')))
                            if server_data:
                                _condition['server_name'] = server_data.get('name','')
                            _condition['start_inventory'] = diamond
                            _condition['create_time'] = _time
                            self.database.add('currency_cond',_condition)


                r.lrem(key, data,1)

    ## 条件更新期末
    def update_currency(self,param,condition):
       self.database.update('currency_cond',dict(end_inventory = param),condition)



## 古神遗迹和英雄试炼
class relic_hero(object):

    @catch_except('relic_hero')
    def __init__(self):
        self.database = connDB()

    @catch_except('relic_hero')
    def starting(self):
        while 1:
            gevent.sleep(0.1)
            key = Prefix + 'task_checkpoint'
            room_list = [2,3]
            datas = r.lrange(key,0,1000)
            if len(datas)==0:
                gevent.sleep(1)
            for data in datas:
                # gevent.sleep(0.01)
                _data = json.loads(data)
                # _data = eval(data)
                _time = int(_data.get('time'))
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','')+'_' + chsc
                __time = datetime.datetime.utcfromtimestamp(_time) + datetime.timedelta(hours=8)
                _data['time'] =  __time
                _data['d_date'] = int(__time.strftime('%Y%m%d'))
                name = _data.get('type','')
                if name.find('英雄试炼') > -1:
                    _data['b_type'] = 2
                elif name.find('组队') > -1:
                    _data['b_type'] = 3
                member_team = _data.pop('member_team','')
                _data['pid_time'] = str(_data.get('pid')) + str(_time)
                s_flag = _data.get('s_flag',0)
                if s_flag in room_list:
                    ret =self.database.add('room_info',_data)
                else:
                    ret = self.database.add('relic_hero',_data)
                    if member_team and _data.get('b_type',0) ==3:
                        for k,v in member_team.items():
                            _data['uid'] = k
                            _data['pid'] = v
                            self.database.add('relic_hero',_data)
                if ret:
                    r.lrem(key,data,1)


## 玩家精灵情况

class Spirit(object):

    @catch_except('Spirit')
    def __init__(self):

        self.database = connDB()

    @catch_except('Spirit')
    def starting(self):
        while 1:
            gevent.sleep(0.1)
            device_power_ster_cols = ('d_date','s_uid','time','ch','chmc','chsc','device_power','ster_stone','uid','pid')
            player_own_cols = ('d_date','s_uid','time','ch','chmc','chsc','name','level','uid','pid','type')
            key =Prefix +  'emblem_sprite_record'
            datas = r.lrange(key,0,1000)
            datas_len = len(datas)
            if datas_len==0:
                gevent.sleep(1)
            tables = ['player_own','device_power_ster']
            device_power_ster_values = []
            player_own_values = []
            for data in datas:
                # gevent.sleep(0.01)
                _data = json.loads(data)
                _time = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                _data['time'] =  _time
                _data['d_date'] = int(_time.strftime('%Y%m%d'))
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                device_power = _data.pop('emblem_power',0)
                ster_stone = _data.pop('star_stone',0)
                partner_device = _data.pop('partner_emblem',dict()) ## type = 2
                magic_stone = _data.pop('magic_stone',dict()) ## type = 3
                spirit_map = _data.pop('sprite_info',dict()) ## type = 1

                device_power_d = copy.deepcopy(_data) 
                
                delete_con = dict(d_date = _data.get('d_date'),
                                    uid =_data.get('uid'),
                                    pid = _data.get('pid'),
                                    ch  = _data.get('ch',''),
                                    s_uid = _data.get('s_uid')
                    )
                for _table in tables:
                    self.database.delete(_table,delete_con)
                device_power_d['device_power'] = device_power
                device_power_d['ster_stone'] = ster_stone
                device_power_ster_value = []
                for i in device_power_ster_cols:
                    _value = device_power_d.get(i)
                    if _value is not None:
                        device_power_ster_value.append(_value)
                    else :
                        print i,_value,key
                device_power_ster_values.append(tuple(device_power_ster_value))
                param_list = [spirit_map, partner_device, magic_stone]
                for i in range(len(param_list)):
                    _type = i+1
                    self.deal_data(_data, param_list[i], 'name', 'level', _type,player_own_values,player_own_cols)
            # print device_power_ster_values,'device_power_ster_values'
            # print player_own_values,'player_own_values'
            if datas_len:
                if device_power_ster_values:
                    de_sql = self.database.add2('device_power_ster',device_power_ster_cols)
                    self.database.addall(de_sql,device_power_ster_values)
                if player_own_values:
                    pl_sql = self.database.add2('player_own',player_own_cols)
                    self.database.addall(pl_sql,player_own_values)
                r.ltrim(key,datas_len,-1)

    def deal_data(self, data, params, field, field2, _type,player_own_values,player_own_cols):
        
        for k,v in params.items():
            data.update({field:k, field2:v, 'type':_type})
            # self.database.add('player_own', data)
            player_own_value= []
            for i in player_own_cols:
                _value = data.get(i)
                if _value is not None:
                    player_own_value.append(_value)
                else :
                    print i,_value,key
            player_own_values.append(tuple(player_own_value))
## 冒险团原始数据入库
class adgroup(object):

    @catch_except('adgroup')
    def __init__(self):

        self.database = connDB()

    @catch_except('adgroup')
    def starting(self):
        while 1:
            pass
            gevent.sleep(0.1)
            key =Prefix +  'guild_summary'
            datas =r.lrange(key,0,1000)
            if len(datas)==0:
                gevent.sleep(1)
            for data in datas:
                # gevent.sleep(0.01)
                _data = json.loads(data)
                _time = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                _data['time'] =  _time
                _data['d_date'] = int(_time.strftime('%Y%m%d'))
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','')+'_'+ chsc
                ret = self.database.add('guild_summary',_data)
                if ret:
                    r.lrem(key,data,1)


### 冒险团boss记录

class guildbossrecord(object):

    @catch_except('guildbossrecord')
    def __init__(self):
        self.database = connDB()


    @catch_except('guildbossrecord')
    def starting(self):
        while 1:
            cols = ('p_id','num','player_id','total_damage','bonus_grant','bonus_grant_time','bonus_id')
            gevent.sleep(0.1)
            key =Prefix +  'guild_boss'
            datas =r.lrange(key,0,1000)
            if len(datas)==0:
                gevent.sleep(1)
            for data in datas:
                # gevent.sleep(0.01)
                values = []
                _data = json.loads(data)
                # _data = eval(data)
                boss_starttime = datetime.datetime.utcfromtimestamp(int(_data.get('boss_starttime'))) + datetime.timedelta(hours=8)
                boss_endtime = datetime.datetime.utcfromtimestamp(int(_data.get('boss_endtime'))) + datetime.timedelta(hours=8)
                _data['boss_starttime'] =  boss_starttime
                _data['boss_endtime'] = boss_endtime
                rank = _data.pop('rank',[])
                _data.pop('server',None)
                # _data['d_date'] = int(_time.strftime('%Y%m%d'))
                ret = self.database.add('guild_boss_record',_data)
                if ret:
                    if rank:
                        for _rank in rank:
                            values.append([ret,_rank[0],_rank[1],_rank[2],_rank[3],time.localtime(_rank[4]),_rank[5]])
                        sql = self.database.add2('guild_boss_reward',cols)
                        self.database.addall(sql,values)
                    r.lrem(key,data,1)


## 冒险团消息
class GuildPperateMsg(object):

    @catch_except('GuildPperateMsg')
    def __init__(self):

        self.database = connDB()

    @catch_except('GuildPperateMsg')
    def starting(self):
        while 1:
            gevent.sleep(0.1)

            key =Prefix +  'guild_operate_msg'
            datas =r.lrange(key,0,500)
            if len(datas)==0:
                gevent.sleep(1)

            for data in datas:
                # gevent.sleep(100)
                _data = json.loads(data)
                if _data.get('create_time') is not None:
                    create_time = datetime.datetime.utcfromtimestamp(int(_data.get('create_time'))) + datetime.timedelta(hours=8)
                    d_date = int(create_time.strftime('%Y%m%d'))
                else:
                    d_date = int(datetime.datetime.now().strftime('%Y%m%d'))
                _data['d_date'] = d_date
                ret = self.database.add('guild_operate_msg',_data)
                if ret:
                    r.lrem(key,data,1)

## 冒险团产出消耗
class GuildOutputCon(object):

    @catch_except('GuildOutputCon')
    def __init__(self):

        self.database = connDB()

    @catch_except('GuildOutputCon')
    def starting(self):
        while 1:
            gevent.sleep(0.1)

            key =Prefix +  'guild_production_cost_msg'
            datas =r.lrange(key,0,500)
            if len(datas)==0:
                gevent.sleep(1)
            for data in datas:
                # gevent.sleep(100)
                _data = json.loads(data)
                s_uid = _data.get('s_uid','')
                r_type = _data.get('r_type','')
                way = _data.get('way','')
                r_type_id = deal_base.guild_way(r_type,0,self.database)
                wan_id = deal_base.guild_way(way,1,self.database)
                _data['r_type'] = r_type_id
                _data['way'] = wan_id
                operate_time = datetime.datetime.utcfromtimestamp(int(_data.get('operate_time'))) + datetime.timedelta(hours=8)
                server_name =check_server_name(s_uid,self.database)
                _data['server_name'] = server_name
                _data['d_date'] = int(operate_time.strftime('%Y%m%d'))
                ret = self.database.add('guild_production_cost_msg',_data)
                if ret:
                    r.lrem(key,data,1)


## 皮肤购买
class buy_skin(object):
    @catch_except('buy_skin')
    def __init__(self):
        self.database = connDB()

    @catch_except('buy_skin')
    def starting(self):
        while 1:
            cols = ('buytime','ch','chmc','chsc','cost','pid','uid','s_uid','type','d_date','skinid')
            key  = Prefix + 'buy_skin'
            gevent.sleep(1)
            skin_datas = r.lrange(key,0,1000)
            datas_len = len(skin_datas)
            if datas_len ==0:
                gevent.sleep(2)
            values = []
            for data in skin_datas:
                gevent.sleep(0.01)
                _data = json.loads(data)
                buy_time  = datetime.datetime.utcfromtimestamp(int(_data.get('buytime'))) + datetime.timedelta(hours=8)
                _data['d_date'] = int(buy_time.strftime('%Y%m%d'))
                value = []
                for i in cols:
                    _value = _data.get(i)
                    if _value is not None:
                        value.append(_value)
                    else :
                        print i,_value,key
                values.append(tuple(value))
            if values:
                sql = self.database.add2('buy_skin',cols)
                self.database.addall(sql,values)
                r.ltrim(key,datas_len,-1)


## 故事关卡
class cstory_adventure_clean_up(object):
    @catch_except('cstory_adventure_clean_up')
    def __init__(self):
        self.database = connDB()

    @catch_except('cstory_adventure_clean_up')
    def starting(self):
        while 1:
            cstory_adventure_cols = ('time','ch','chmc','chsc','pid','uid','s_uid','d_date','event_id','finish_time','medal','name',
                'onece','player_lv')
            cstory_adventure_clup_cols = ('time','ch','chmc','chsc','pid','uid','s_uid','d_date','event_id','player_lv','sweepnum')
            key  = Prefix + 'cstory_adventure'
            gevent.sleep(1)
            cstory = r.lrange(key,0,1000)
            datas_len = len(cstory)
            if datas_len ==0:
                gevent.sleep(2)
            values = []
            clean_up_values = []
            for data in cstory:
                gevent.sleep(0.01)
                _data = json.loads(data)
                time  = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                _data['d_date'] = int(time.strftime('%Y%m%d'))
                if not _data.get('sweepnum'):
                    self.deal_type(values,cstory_adventure_cols,_data,key)
                else:
                    self.deal_type(clean_up_values,cstory_adventure_clup_cols,_data,key)
            if values or clean_up_values:
                if values:
                    sql = self.database.add2('cstory_adventure',cstory_adventure_cols)
                    self.database.addall(sql,values)
                else:
                    clean_upsql = self.database.add2('cstory_adventure_clean_up',cstory_adventure_clup_cols)
                    self.database.addall(clean_upsql,clean_up_values)
                r.ltrim(key,datas_len,-1)
    @catch_except('')
    def deal_type(self,big_list,cols,_data,key):
        value = []
        for i in cols:
            _value = _data.get(i)
            if _value is not None:
                value.append(_value)
            else :
                print i,_value,key
        big_list.append(tuple(value))

## 添加附件
class annex_information(object):

    @catch_except('annex_information')
    def __init__(self):

        self.database = connDB()

    @catch_except('annex_information')
    def starting(self):
        while 1:
            annex_information = r.get(':cache:backend_attachments')
            if  isinstance(annex_information,str) and annex_information!='{}':
                annex_information =annex_information.replace("'",'"')
                if annex_information:
                    self.database.delete('origin_data',dict())
                self.database.add('origin_data',dict(data = annex_information))
            skin_list = []
            skin_data = r.get(':cache:backend_skins')
            if  isinstance(skin_data,str) and skin_data!='{}':
                _skin_data = dict()
                skin_data =  json.loads(skin_data)
                for k,v in skin_data.items():
                    for _k,_v in v.items():
                        if k not in skin_list:
                            _skin_data[str(_k)] = _v.encode('utf-8')
                self.database.delete('skin_data',dict())
                self.database.add('skin_data',dict(data = json.dumps(_skin_data,ensure_ascii=False)))
            ##更新间隔时间
            gevent.sleep(60*20)



## 
class dealPlayerGetREdis(object):

    @catch_except('dealPlayerGetREdis')
    def __init__(self):
        self.database = connDB()

    @catch_except('dealPlayerGetREdis')
    def starting(self,limit=500):
        ## 预计是量大的数据
        litem_cols = ('uid','pid', 'type','time','s_uid', 'd_date',
            'ch', 'chmc','chsc','Path','os','new','old','unique_id')
        while 1:
            gevent.sleep(0.1)
            key = Prefix + 'player_get'
            datas = r.lrange(key,0,limit)
            datas_len = len(datas)
            if datas_len==0:
                gevent.sleep(2)
            values = []
            for data in datas:
                # gevent.sleep(0.01)
                _data = json.loads(data)
                if _data.get('unique_id') is None:
                    _data['unique_id'] =''
                _time = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                _data['time'] =  _time
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                _data['d_date'] = int(_time.strftime('%Y%m%d'))
                _type = _data.get('type',1)
                if _type ==1:
                    p_name = '等级'
                    self.check(p_name)
                elif _type==2:
                    p_name ='角色'
                else:
                    p_name = '皮肤'
                if _type!=1:
                    _type = _data.pop('Path','')
                    _id =cons_mode(key,self.database,_type,p_name)
                    _data['Path'] = _id
                value = []
                for i in litem_cols:
                    _value = _data.get(i)
                    if _value is not None:
                        value.append(_value)
                values.append(tuple(value))
            if values:
                sql = self.database.add2('player_get',litem_cols)
                self.database.addall(sql,values)
                r.ltrim(key,datas_len,-1)

    def check(self,name):

        cache_data = caching.get(name)
        if cache_data is None:
            check_sql_d = self.database.select('cons_mode',dict(name = name))
            if not check_sql_d:
                self.database.add('cons_mode',dict(name = name))
            caching.set(name,int(time.time()))

class superstar(object):

    @catch_except('superstar')
    def __init__(self):
        self.database = connDB()

    @catch_except('superstar')
    def starting(self,limit = 1000):
        litem_cols = ('uid','pid', 'optype','time','s_uid', 'd_date',
            'ch', 'chmc','chsc','os','nodeid','pname','fid','fname','basediamond',
            'attr')
        while 1:
            gevent.sleep(2)
            key = Prefix + 'superstar'
            datas = r.lrange(key,0,limit)
            datas_len = len(datas)
            if datas_len==0:
                gevent.sleep(2)
            values = []
            for data in datas:
                _data = json.loads(data)
                _time = datetime.datetime.utcfromtimestamp(int(_data.get('time'))) + datetime.timedelta(hours=8)
                chsc = _data.get('chsc','')
                if chsc:
                    _data['ch'] = _data.get('ch','') +'_'+ chsc
                _data['d_date'] = int(_time.strftime('%Y%m%d'))
                optype = _data.pop('optype','')
                if not optype:
                    continue
                _optype = 0
                if optype =='active':
                    _optype =1
                    _data['basediamond'] = 0
                elif optype =='reset':
                    _optype = 2
                    _data['attr'] = _data.pop('lockattr',[])
                elif optype =='confirm':
                    _optype =3
                    _data['basediamond'] = 0
                _data['optype'] = _optype
                _data['attr'] = json.dumps(_data.get('attr',''))
                value = []
                for i in litem_cols:
                    _value = _data.get(i)
                    if _value is not None:
                        value.append(_value)
                values.append(tuple(value))
            if values:
                sql = self.database.add2('superstar',litem_cols)
                self.database.addall(sql,values)
                r.ltrim(key,datas_len,-1)