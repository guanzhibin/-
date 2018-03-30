#coding:utf8
from __future__ import division
import gevent
import random
from mg_config import mg_log
from public_class.base import catch_except
from mysql_data.sqlprocess import connDB
from public_class import caching
import datetime
import json
from decimal import Decimal
sleep_stime = 200
sleep_etime = 300
DIFFERENT_TIME = 60*(random.randint(2,3))
ACTION = '02'
class pay_distribution(object):
    @catch_except('pay_distribution')
    def __init__(self):
        self.init = True  ## 预留参数
        self.database = connDB() ## 数据库实例
        self.activation = False
        self.date = 20180304   ### 20180109
        self.identification = False
        self.ishour = False
        self.two_action = False
        self.days_ago  = '20180319'
        self.days_ago_action = True
    @catch_except('pay_distribution')
    def starting_deal(self):
        while 1:
            #  逻辑代码
            gevent.sleep(2)
            now = datetime.datetime.now()
            if now.strftime('%Y%m%d')!=self.days_ago and self.days_ago_action:
                d_date = self.days_ago
                now = datetime.datetime(int(self.days_ago[:4]),int(self.days_ago[4:6]),int(self.days_ago[6:]),2,2,59)
                self.days_ago = (now + datetime.timedelta(days = 1)).strftime('%Y%m%d')
            hour = now.strftime('%H')
            if hour == ACTION and self.two_action is False and self.ishour!=hour:
                self.two_action =True
            self.identification = int((now + datetime.timedelta(days = -1)).strftime('%Y%m%d')) == self.date
            if self.identification or self.two_action:
                now = now + datetime.timedelta(days = -1)
            ymd = int(now.strftime('%Y%m%d'))
            final_datas = []
            ##  统计玩家新登数
            new_login_datas = list(self.database.StatisticsNewLoginByDay(ymd))
            gevent.sleep(0.1)
            ##  统计当天登录玩家数  这个列表最大
            login_data = list(self.database.StatisticsLoginByDay(ymd))
            final_datas = login_data
            final_data_len = len(final_datas)
            if len(new_login_datas) :
                for i in range(final_data_len):
                    for _new in new_login_datas:
                        if final_datas[i].get('s_uid') == _new.get('s_uid') and final_datas[i].get('ch') == _new.get('ch'):
                            final_datas[i]['new_login_accont'] = _new.get('new_login_accont',0)
                            continue 

            ### 
            # atm_sisc = self.database.sisc_atm_num_bychsuid(ymd)
            # if atm_sisc:
            #     for _atm_sisc in atm_sisc:
            #         for i in range(final_data_len):
            #             if final_datas[i].get('s_uid') == _atm_sisc.get('s_uid') and final_datas[i].get('ch') == _atm_sisc.get('ch'):
            #                 final_datas[i]['atm_num'] = _new.get('atm_num',0)
            #                 continue
            # print final_datas
            ## 付费账号
            pay_account  = self.database.payAccountNum(ymd)
            ##  当天有付费账号时才有当天的收入
            if pay_account:
                ## 当天付费收入
                income_datas = self.database.IncomeByDay(ymd)
                for i in range(len(pay_account)):
                    for _income in income_datas:
                        if pay_account[i].get('s_uid') == _income.get('s_uid') and pay_account[i].get('ch') == _income.get('ch'):
                            # pay_account[i]['income']   = _income.get('income','')
                            s_uid = pay_account[i].get('s_uid','')
                            pay_account_num = pay_account[i].get('pay_account_num',0)
                            income = _income.get('income',0)
                            d_date = pay_account[i].get('d_date')
                            _check_condition = dict(
                                                d_date = d_date,
                                                channel_name = pay_account[i].get('ch',''),
                                                s_uid = s_uid 
                                            )
                            ret_check = self.database.select('mg_daily_newspaper',_check_condition)
                            _update_d = dict(
                                        pay_account_num = pay_account_num,
                                        income = income,
                                        pay_ARPU = float(income)/float(pay_account_num) if pay_account_num else 0.00,
                                        atm_num = _income.get('atm_num',0)
                                    )
                            if ret_check:
                                ## 更新
                                _update_d['DAU_ARPU'] = float(income)/float(ret_check.get('login_account')) if ret_check.get('login_account') else 0.00
                                self.database.update('mg_daily_newspaper',_update_d,dict(id = ret_check.get('id')))
                                gevent.sleep(0.1)
                            else:
                                ## 插入
                                _update_d.update(_check_condition)
                                gevent.sleep(0.1)
                                s_uid =pay_account[i].get('s_uid')
                                server_name = caching.get(s_uid)
                                if server_name is None:
                                    _ser_data = self.database.select('servers_data',dict(uid = pay_account[i].get('s_uid')))
                                    server_name = ''
                                    if _ser_data:
                                        server_name = _ser_data.get('name','')
                                        caching.set(s_uid,server_name)
                                _update_d['server_name'] = server_name
                                _update_d['create_time'] = now
                                last_id = self.database.add('mg_daily_newspaper',_update_d)
                            continue

            ##  首次付费账号
            once_pays = self.database.newPay_count(dict(d_date = ymd, new_pay_flag= 1))
            if len(once_pays):
                for i in range(final_data_len):
                    for _once in once_pays:
                        if final_datas[i].get('s_uid') == _once.get('s_uid') and final_datas[i].get('ch') == _once.get('ch'):
                            final_datas[i]['first_pay_account'] = _once.get('first_pay_account','')
                            final_datas[i]['first_pay_account_income'] = _once.get('first_pay_account_income') if _once.get('first_pay_account_income') else 0
                            continue
            ##  新注册且付费的账号数
            new_reg_pay_data = self.database.newRegPay(dict(d_date = ymd, new_reg_flag = 1))
            if len(new_reg_pay_data)>0:
                ##  当天有新注册且有付费的账号才有总的
                # new_reg_pay_datas = list(self.database.select_all('atm',dict(d_date = ymd, new_reg_flag =1,orderstate = 5
                #    ), field = ['ch','chmc','uid','chsc','s_uid','pid']))
                # for data in new_reg_pay_datas:
                #     ## 根据条件查出
                #     # gevent.sleep(0.1)
                #     data['d_date'] = ymd
                __data_l = self.database.check_by_message(dict(d_date = ymd))
                self.new_pay_income(new_reg_pay_data,__data_l)
                for i in range(final_data_len):
                    for _new_pay in new_reg_pay_data:
                        if final_datas[i].get('s_uid') == _new_pay.get('s_uid') and final_datas[i].get('ch') == _new_pay.get('ch'):
                            final_datas[i]['new_login_pay_num'] = _new_pay.get('new_login_pay_num',0)
                            final_datas[i]['new_login_pay_income'] = _new_pay.get('new_login_pay_income',0)

            for _final in final_datas:
                gevent.sleep(0.1)
                s_uid = _final.pop('s_uid','')
                channel_name = _final.pop('ch','')
                d_date = _final.pop('d_date')
                condition = dict(
                                d_date = d_date,
                                channel_name = channel_name,
                                s_uid = s_uid
                                )
                items = self.database.select('mg_daily_newspaper',condition)
                if items:
                    ## 存在更新
                    _final['DAU_ARPU'] = items.get('income',0)/_final.get('login_account') if _final.get('login_account') else 0.00
                    self.database.update('mg_daily_newspaper',_final,dict(id = items.get('id')))
                    gevent.sleep(0.1)
                else:
                    ## 不存在插入
                    _final.update(condition)
                    ##  插入数据根据s_uid获取服务对应的名称
                    _server_data  = self.database.select('servers_data',dict(uid = _final.get('s_uid','')))
                    if _server_data:
                        _final['server_name'] = _server_data.get('name','')
                    _final['create_time'] = now
                    self.database.add('mg_daily_newspaper',_final)

                ##  如果存在新增 new_login_accont 有值 new_login_pay_income 更新ltv价值表
                ## new_login_accont用户存留添加或者更新
                # if _final.get('new_login_accont'):
                self.deal_ltv_today(dict(s_uid =s_uid,
                                        channel_name = channel_name,
                                        d_date = d_date,
                                        new_login_accont = _final.get('new_login_accont',0),
                                        new_login_pay_income = _final.get('new_login_pay_income',0)
                                    ),now)
                self.deal_retain(_final,condition,now)

            ## 统计当天创建的角色
            

            #   获取当天最高和平均在线人数,由于接近与0点，可以考虑统计天昨天的
            # hms = datetime.datetime.now().strftime('%H%M%S')
            # if hms =='000000':
            #     self.activation = True
            # if hms =='002000':
            #     self.activation = False
            # if self.activation is False:
            #     ago_day = 0
            #     if self.identification:
            #         ago_day = -1
            #     yes_day = self.some_day_ago(ago_day)
            #     _yes_day = int(yes_day.strftime('%Y%m%d'))
            #     print _yes_day
            #     daily_online = self.database.select_all('daily_online',dict(d_date = _yes_day), in_condition =dict(os = (1,2)))
            #     print daily_online
            #     for _daily in daily_online:
            #         condition  =dict()
            #         if _daily.get('avg'):
            #             condition['average_number_online'] = _daily.get('avg')
            #         if _daily.get('num'):
            #             condition['highest_online'] = _daily.get('num')
            #         if condition:
            #             self.database.update('mg_daily_newspaper',condition,
            #                 dict(d_date = _yes_day,s_uid = _daily.get('s_uid','')))
            #             gevent.sleep(0.1)
                   
            ## 写入每天每条服务器的数据
            self.server_data(ymd)

            ###  算用户留存
            _one_retain_days = -1
            _three_retain_days = -2
            _seven_retain_days = -6
            if self.identification:
                _one_retain_days = -2
                _three_retain_days = -3
                _seven_retain_days = -7
            day_list = dict(one_retain_days = _one_retain_days, three_retain_days = _three_retain_days, seven_retain_days = _seven_retain_days)
            for k,v in day_list.items():
                gevent.sleep(0.1)
                _ymd = self.some_day_ago(v,now)
                two_retain = self.database.player_retain(dict(reg_ymd = int(_ymd.strftime('%Y%m%d')) , ymd = ymd))
                for _retain in two_retain:
                    ###  有的话更新
                    if _retain.get('count'):
                        retain_daily_data = self.database.select('mg_daily_newspaper',dict(
                                                                                d_date = _retain.get('d_date',0),
                                                                                s_uid = _retain.get('s_uid',''),
                                                                                channel_name = _retain.get('ch','')
                            ))
                        if retain_daily_data:
                            query =dict()
                            new_num = retain_daily_data.get('new_login_accont',0)
                            if new_num:
                                player_re_value = Decimal(str(_retain.get('count')*100/new_num)).quantize(Decimal('0.00'))
                                query[k] = player_re_value if player_re_value <=100.00 else 100.00
                                self.database.update('mg_daily_newspaper',query,dict(
                                                                            d_date = _retain.get('d_date',0),
                                                                            s_uid = _retain.get('s_uid',''),
                                                                            channel_name = _retain.get('ch','')
                                                        ))
                        gevent.sleep(0.1)
            ## 角色留存基础

            ### 统计某天的角色登录数
            role_login_acount = self.database.RoleLoginAccount(ymd)
            ## 当天新创建的角色
            new_roles = self.database.StatisticsNewRole(ymd)
            if new_roles:
                new_login_role = self.database.NewLoginRole(ymd)
                for new_role in new_roles:
                    for _new_login_role in new_login_role:
                        if new_role.get('channel_name') == _new_login_role.get('channel_name','') and new_role.get('s_uid')==_new_login_role.get('s_uid',''):
                            new_role.update(_new_login_role)
                            continue
            for _role_login_account in role_login_acount:
                for new_role in new_roles:
                    if _role_login_account.get('channel_name','') == new_role.get('channel_name','') and _role_login_account.get('s_uid','')==new_role.get('s_uid',''):
                        _role_login_account.update(new_role)
                        continue
            for role in role_login_acount:
                has_role = self.database.select('role_retain',dict(d_date = role.get('d_date',''), s_uid = role.get('s_uid',''), channel_name = role.get('channel_name','')))
                if has_role:
                    ## 存在更新
                    params = dict(
                            d_date = role.pop('d_date',None),
                            channel_name = role.pop('channel_name',None),
                            s_uid = role.pop('s_uid',None)
                        )
                    self.database.update('role_retain', role, params)
                else:
                    ##否则插入
                    _server_data  = self.database.select('servers_data',dict(uid = role.get('s_uid','')))
                    if _server_data:
                        role['server_name'] = _server_data.get('name','')
                    role['create_time'] = now
                    self.database.add('role_retain', role)


            ## 统计注册的用户数
            
            new_regist_data = self.database.new_regist_account(ymd)
            for _new_regist_d in new_regist_data:
                _new_regist_d['d_date'] = ymd
                rg_cond = dict(d_date = ymd, channel_name = _new_regist_d.get('channel_name',''), s_uid = _new_regist_d.get('s_uid'))
                check_pr = self.database.select('player_retain', rg_cond)
                if check_pr:
                    ## 更新
                    self.database.update('player_retain',dict(regist_account = _new_regist_d.get('regist_account')),dict(id = check_pr.get('id',0)))
                else:
                    rg_server_d = self.database.select('servers_data',dict(uid = _new_regist_d.get('s_uid','')))
                    _new_regist_d['server_name'] = rg_server_d.get('name','')
                    self.database.add('player_retain',_new_regist_d)

            self.date = int(datetime.datetime.now().strftime('%Y%m%d'))
            self.ishour = hour
            self.two_action = False
            gevent.sleep(sleep_stime)

    def server_data(self,d_date):
        server_data_dict = self.database.deal_servers(d_date)
        table_name = 'mg_daily_serverspaper'
        for __server in server_data_dict:
            d_date = __server.pop('d_date',0)
            check_daily_paper = dict(
                    d_date = d_date,
                    s_uid = __server.pop('s_uid','')
                )
            _daily_paper = self.database.select(table_name,check_daily_paper)
            if _daily_paper:
                self.database.update(table_name,__server,dict(id= _daily_paper['id']))
            else:
                if d_date>20180116:
                    __server.update(check_daily_paper)
                    self.database.add(table_name,__server)
    def some_day_ago(self,num,now):
        some_day_ago = now + datetime.timedelta(days = num)
        return some_day_ago
    def new_pay_income(self,new_reg_pay_data,l_data):
        for i in range(len(new_reg_pay_data)):
            for _l_data in l_data:
                if new_reg_pay_data[i].get('s_uid') == _l_data.get('s_uid') and new_reg_pay_data[i].get('ch') == _l_data.get('ch'):
                    new_reg_pay_data[i]['new_login_pay_income'] = new_reg_pay_data[i].get('new_login_pay_income',0)+(_l_data.get('new_login_pay_income') if _l_data.get('new_login_pay_income') else 0)
                    continue
    ##  处理当天新增的ltv价值
    def deal_ltv_today(self,ltv_data,create_time):
        _conditian = dict(
                        s_uid = ltv_data.get('s_uid',''),
                        channel_name = ltv_data.get('channel_name',''),
                        d_date = ltv_data.get('d_date',0)
                    )
        _ltv_data = self.database.select('ltv_value',_conditian)
        one_day_income = _ltv_data.get('one_day_income',0)
        ## 此值必定存在
        new_account_num = ltv_data.get('new_login_accont',0)
        _income = ltv_data.get('new_login_pay_income',0)
        _update_data = dict(
                       new_account_num =  new_account_num,
                       one_day_ltv = '%.2f' %(one_day_income/new_account_num if new_account_num else 0),
                )
        ### 存在更新，不存在则添加
        if _ltv_data:
            self.database.update('ltv_value',_update_data,_conditian)
        else:
            _update_data.update(_conditian)
            _server_data  = self.database.select('servers_data',dict(uid = _update_data.get('s_uid','')))
            if _server_data:
                _update_data['server_name'] = _server_data.get('name','')
            _update_data['create_time'] = create_time
            self.database.add('ltv_value',_update_data)

    ## 处理当日的用户retain
    def deal_retain(self,final,condition,create_time):
        account_retain = self.database.select('player_retain',condition)
        if account_retain:
            self.database.update('player_retain',dict(new_login_accont = final.get('new_login_accont',0),login_account = final.get('login_account',0)),condition)
        else:
            _account_retain =dict(new_login_accont = final.get('new_login_accont',0), login_account = final.get('login_account',0))
            _account_retain.update(condition)
            _server_data  = self.database.select('servers_data',dict(uid = _account_retain.get('s_uid','')))
            if _server_data:
                _account_retain['server_name'] = _server_data.get('name','')
            _account_retain['create_time'] = create_time
            self.database.add('player_retain',_account_retain)

    ##  
class deal_equipment(object):

    @catch_except('deal_equipment')
    def __init__(self):
        self.database = connDB()
        self.yes_actoin = False
        self.date = 20180307 
        self.ishour = False
        self.two_action = False
    @catch_except('deal_equipment')
    def starting(self):
        import copy
        ## 只统计当天的渠道名称可以作为key
        while 1:
            gevent.sleep(1)
            now = datetime.datetime.now()
            hour = now.strftime('%H')
            if hour == ACTION and self.two_action is False and self.ishour!=hour:
                self.two_action =True
            self.yes_actoin = int((now + datetime.timedelta(days = -1)).strftime('%Y%m%d'))==self.date
            if self.yes_actoin is True or self.two_action:
                now = now + datetime.timedelta(days = -1)
            d_date = int(now.strftime('%Y%m%d'))
            mg_daily_chs_dict = dict() 
            equipment_retain = dict()
            daily_ds =  self.database.deal_daily_np(dict(d_date = d_date))
            ##  算新增加的设备数
            new_fetch_ser_ds = self.database.deal_fetch_s(dict(d_date = d_date))
            _new_fetch_ser_ds = copy.deepcopy(new_fetch_ser_ds)
            ## 设备登录数
            equipment_login_d = self.database.LoginEquipment(d_date)
            ## 新登录设备数
            new_start_login_d = self.database.NewSatrtLogin(d_date)
            ### 各个渠道服务器的设备转化数
            valid_e_datas = self.database.deal_equipment_num(dict(d_date = d_date))
            self.deal_dict(mg_daily_chs_dict, daily_ds)
            self.deal_dict(mg_daily_chs_dict, new_fetch_ser_ds)
            self.deal_dict(mg_daily_chs_dict, valid_e_datas)
            
            self.deal_dict(equipment_retain, equipment_login_d)
            self.deal_dict(equipment_retain, _new_fetch_ser_ds)
            self.deal_dict(equipment_retain, new_start_login_d)
            for v in mg_daily_chs_dict.values():
                gevent.sleep(0.1)
                _check_condi = dict(channel_name = v.get('channel_name'),
                                    d_date = d_date)
                ## 查是否存在
                check_chs_d = self.database.select('mg_daily_chs',_check_condi)
                if check_chs_d:
                    ## 存在更新
                    self.database.update('mg_daily_chs',v, dict(id = check_chs_d['id']))
                else:
                    ## 不存在添加
                    v['d_date'] = d_date
                    v['create_time'] = now
                    self.database.add('mg_daily_chs',v)
                ## 

            ## 设备留存处理
            
            self.deal_eqp(d_date, equipment_retain,now)


            # if self.yes_actoin is False:
            self.overview(daily_ds, new_fetch_ser_ds,d_date,now)
            self.yes_actoin = False
            self.date = int((datetime.datetime.now()).strftime('%Y%m%d'))
            self.ishour = hour
            self.two_action = False
            gevent.sleep(sleep_stime)

    ### 

    def deal_eqp(self,d_date, equipment_retain,create_time):
        for v in equipment_retain.values():
            condition_eqp = dict(channel_name = v.get('channel_name',''), d_date = d_date)
            check_eqp_d = self.database.select('equipment_retain', condition_eqp)
            if check_eqp_d:
                self.database.update('equipment_retain', v ,dict(id = check_eqp_d.get('id',0)))
            else:
                v['d_date'] = d_date
                v['create_time'] = create_time
                self.database.add('equipment_retain',v)

    ##  处理概览表
    def overview(self,daily_data,new_fetch_ser_data,d_date,create_time):
        '''
          `create_time`       datetime NOT NULL DEFAULT NOW() COMMENT '创建时间',
          `d_date`            int(8) NOT NULL DEFAULT 0 COMMENT '年月日',
          `channel_name`      varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
          `new_equipment`     bigint NOT NULL DEFAULT 0 COMMENT '新增加去重设备',
          `new_login_account` bigint NOT NULL DEFAULT 0 COMMENT '新增加的去重注册账号',
          `login_account`     bigint NOT NULL DEFAULT 0 COMMENT '登录游戏的去重玩家数',
          `pay_account`       bigint NOT NULL DEFAULT 0 COMMENT '付费的玩家数',
          `pay_income`        decimal(10,2) NOT NULL DEFAULT 0.00 COMMENT '收入总额',
          `new_equip_login`   bigint NOT NULL DEFAULT 0 COMMENT '新增加的设备并且有登录游戏的设备数',
          `start_equip`       bigint NOT NULL DEFAULT 0 COMMENT '启动游戏的设备数',
        '''
        overview_dict = dict()
        for data in daily_data:
            key = data.get('channel_name')
            _data = dict(
                new_login_account =  int(data.get('new_login_accont',0)),
                login_account = int(data.get('login_account',0)),
                pay_account = int(data.get('pay_account_num',0)),
                pay_income = data.get('income',0.00)
            )
            if overview_dict.get(key):
                __data = overview_dict[key]
                __data.update(_data)
            else:
                _data['channel_name'] = key
                overview_dict[key] = _data
        ## 新增加的设备数,登录的账号数，付费玩家数，付费金额,新登账号数从分渠道上面的统计获得

        ## 获得新增加的设备并且有登录游戏的设备数
        new_equip_login_d = self.database.new_equip_login_d(dict(d_date = d_date))
        ## 获取启动游戏的设备数
        start_equip_d = self.database.start_equip_data(dict(d_date = d_date))
        self.deal_dict(overview_dict,new_fetch_ser_data)
        self.deal_dict(overview_dict, new_equip_login_d)
        self.deal_dict(overview_dict, start_equip_d)
        for _overview in overview_dict.values():
            _overview.pop('valid_e_num',None)
            gevent.sleep(0.1)
            _overview_check  = dict(channel_name = _overview.get('channel_name',''),
                                    d_date = d_date
                        )
            _overview_data = self.database.select('overview',_overview_check)
            if _overview_data:
                ## 存在更新
                self.database.update('overview',_overview,_overview_check)
            else:
                _overview['d_date'] = d_date
                _overview['create_time'] = create_time
                self.database.add('overview',_overview)
    def deal_dict(self,d_dict={},d_list=[]):
        for data in d_list:
            key = data.get('channel_name')
            if d_dict.get(key):
                _data = d_dict[key]
                _data.update(data)
            else:
                d_dict[key] = data


##  处理用留存,进用户留存表 
class deal_retains(object):

    @catch_except('deal_retain')
    def __init__(self):
        self.database = connDB()
        self.date = 20180307  
        self.ishour = False
        self.two_action = False
        self.days_ago = '20180307'
        self.days_ago_action = True
    @catch_except('deal_retain')
    def starting(self):
        while 1:
            gevent.sleep(2)
            now = datetime.datetime.now()
            if now.strftime('%Y%m%d')!=self.days_ago and self.days_ago_action:
                now = datetime.datetime(int(self.days_ago[:4]),int(self.days_ago[4:6]),int(self.days_ago[6:]),2,2,59)
                self.days_ago = (now + datetime.timedelta(days = 1)).strftime('%Y%m%d')
            hour = now.strftime('%H')
            if hour == ACTION and self.two_action is False and self.ishour!=hour:
                self.two_action =True
            if int((now + datetime.timedelta(days = -1)).strftime('%Y%m%d'))==self.date or self.two_action:
                now = now + datetime.timedelta(days = -1)
            ymd = int(now.strftime('%Y%m%d'))
            day_list = dict(once_retain = -1, 
                            three_retain = -2,
                            four_retain = -3,
                            five_retain = -4,
                            six_retain = -5, 
                            seven_retain = -6,
                            fifteen_retain = -14,
                            thirty_retain = -29,
                            forty_five_retain = -44,
                            sixty_retain = -59,
                            seventy_five_retain = -74,
                            ninety_retain =-89
                            )
            _daily_paper_server_dict =  dict()
            for k,v in day_list.items():
                gevent.sleep(0.1)
                _ymd = self.some_day_ago(v,now)
                _retains = self.database.player_retain(dict(reg_ymd = int(_ymd.strftime('%Y%m%d')) , ymd = ymd))
                _equipments = self.database.EquipmentRetain(dict(new_fetch_time = int(_ymd.strftime('%Y%m%d')), ymd = ymd))
                # _retain_by_server_datas = self.database._retain_by_server(dict(reg_ymd = int(_ymd.strftime('%Y%m%d')) , ymd = ymd))
                for _retain in _retains:
                    ###  有的话更新
                    s_uid = _retain.get('s_uid','')
                    d_date = _retain.get('d_date',0)
                    _check_data = dict(
                                    d_date = d_date,
                                    s_uid = s_uid,
                                    channel_name = _retain.get('ch',''))
                    if _retain.get('count'):
                        count = _retain.get('count')
                        key = s_uid +'_'+ str(d_date)
                        ## 
                        if v>=-89:
                            paper_value = _daily_paper_server_dict.get(key)
                            if paper_value:
                                paper_value[k] = paper_value.get(k,0) + count
                            else:
                                _daily_paper_server_dict[key] ={"d_date":d_date,'s_uid':s_uid,k:count}
                        retain_daily_data = self.database.select('player_retain',_check_data)
                        if retain_daily_data:
                            query =dict()
                            new_num = retain_daily_data.get('new_login_accont',0)
                            if new_num:
                                value = Decimal(str(_retain.get('count')*100/new_num)).quantize(Decimal('0.00'))
                                query[k] = value if value < 100.00 else 100.00
                                self.database.update('player_retain',query,_check_data
                                )

                        ## 角色留存

                        role_retain_data = self.database.select('role_retain',_check_data)
                        if role_retain_data:
                            query =dict()
                            new_login_accont = retain_daily_data.get('new_login_accont',0)
                            if new_login_accont:
                                role_value = Decimal(str(_retain.get('count')*100/new_login_accont)).quantize(Decimal('0.00'))
                                query[k] = role_value if role_value < 100.00 else 100.00
                                self.database.update('role_retain',query,_check_data
                                )
                ### 。。。。。
                for _equipment in _equipments:
                    if _equipment.get('count'):
                        equipment_retain_d = self.database.select('equipment_retain',dict(d_date = _equipment.get('d_date'),
                        channel_name=  _equipment.get('channel_name','')))
                        if equipment_retain_d:
                            query =dict()
                            new_equipment = equipment_retain_d.get('new_equipment')
                            if new_equipment:
                                eq_value = Decimal(str(_equipment.get('count')*100/new_equipment)).quantize(Decimal('0.00'))
                                query[k] = eq_value if eq_value < 100.00 else 100.00
                                
                                self.database.update('equipment_retain',query,dict(
                                                                            id  = equipment_retain_d.get('id',0)
                                                                            )
                                )
            for _value in  _daily_paper_server_dict.values():
                table_daily_server = 'mg_daily_serverspaper'
                d_date =_value.pop('d_date',0)
                check_daily_paper = dict(
                            s_uid = _value.pop('s_uid',''),
                            d_date = d_date

                    )
                check_daily_paper_data = self.database.select(table_daily_server,check_daily_paper)
                if check_daily_paper_data:
                    self.database.update(table_daily_server,_value,dict(id = check_daily_paper_data.get('id',0)))
                else:
                    if d_date>20180116:
                        _value.update(check_daily_paper)
                        self.database.add(table_daily_server,_value)
            self.date = int(datetime.datetime.now().strftime('%Y%m%d'))
            self.ishour = hour
            self.two_action = False
            gevent.sleep(sleep_stime)

    def some_day_ago(self,num,now):
        some_day_ago = now + datetime.timedelta(days = num)
        return some_day_ago

##  处理 ltv

'''
第十五点到底二十九天只更新
这做法估计数据库会死锁
'''
class deal_ltv(object):

    @catch_except('deal_ltv')
    def __init__(self):
        self.database = connDB()


    def deal_income(self,some_day_income,_some_day_income,income):
        if some_day_income==0:
            some_day_income = _some_day_income + income
        else:
            some_day_income += income
        return some_day_income      
    @catch_except('deal_ltv')
    def starting(self):
        while 1:
            gevent.sleep(1)
            # 统计昨天之前没被标记过的充值数据表
            # yestoday = datetime.datetime.now() + datetime.timedelta(days=-1)
            # yes_ymd = int(yestoday.strftime('%Y%m%d'))
            atm_datas = self.database.check_byFilter('atm',dict(ltv_flag = 0, limit = 1000))
            # if not atm_datas :
            #     gevent.sleep(random.randint(30,60))
            for _atm_data in atm_datas:
                ## 获取该用户的注册时间
                _check_condi = dict(
                                s_uid = _atm_data.get('s_uid',''),
                                ch = _atm_data.get('ch',''),
                                chmc = _atm_data.get('chmc',''),
                                chsc = _atm_data.get('chsc',''),
                                uid = _atm_data.get('uid')
                        )
                reg_data = self.database.select('player_regist',_check_condi)
                reg_time = reg_data.get('reg_time')
                if isinstance(reg_time, datetime.datetime):
                    atm_time = _atm_data.get('time')
                    ## 充值时间与注册时间的时间差
                    atm_ymd = datetime.datetime(atm_time.year, atm_time.month, atm_time.day)
                    reg_ymd = datetime.datetime(reg_time.year, reg_time.month, reg_time.day)
                    diff_day =  ( atm_ymd - reg_ymd ).days
                    if diff_day < 180:
                        _conditian = dict(
                                        d_date = reg_data.get('d_date'),
                                        s_uid = _atm_data.get('s_uid',''),
                                        channel_name = _atm_data.get('ch','')
                                    )
                        ltv_data = self.database.select('ltv_value',_conditian)

                        if not ltv_data:
                            continue
                        _income = _atm_data.get('ordermoney',0)
                        new_account_num = ltv_data.get('new_account_num')
                        one_day_income = ltv_data.get('one_day_income',0)
                        _update_data = dict()
                        if diff_day ==0:
                            one_day_income +=  _income
                            _update_data = dict(one_day_income = one_day_income,
                                                one_day_ltv = one_day_income/new_account_num
                                )
                        elif diff_day ==1:
                            two_day_income  = self.deal_income(ltv_data.get('two_day_income',0),one_day_income,_income)
                            _update_data = dict(
                                                two_day_income = two_day_income,
                                                two_day_ltv = two_day_income/new_account_num
                                            )   
                        elif diff_day ==2:
                            three_days_income = self.deal_income(ltv_data.get('three_days_income',0), ltv_data.get('two_day_income',0), _income)
                            _update_data = dict(
                                                three_days_income= three_days_income,
                                                three_days_ltv = three_days_income/new_account_num
                                            )
                        elif diff_day ==3:  
                            four_day_income =  self.deal_income(ltv_data.get('four_day_income',0), ltv_data.get('three_days_income',0), _income)
                            _update_data = dict(
                                                four_day_income= four_day_income,
                                                four_day_ltv = four_day_income/new_account_num
                                            )
                        elif diff_day ==4:
                            five_day_income = self.deal_income(ltv_data.get('five_day_income',0), ltv_data.get('four_day_income',0), _income)
                            _update_data = dict(
                                                five_day_income= five_day_income,
                                                five_day_ltv = five_day_income/new_account_num
                                            )
                        elif diff_day ==5:                    
                            six_day_income = self.deal_income(ltv_data.get('six_day_income',0), ltv_data.get('five_day_income',0), _income)
                            _update_data = dict(
                                                six_day_income= six_day_income,
                                                six_day_ltv = six_day_income/new_account_num
                                            )
                        elif diff_day == 6:
                            seven_days_income = self.deal_income(ltv_data.get('seven_days_income',0), ltv_data.get('six_day_income',0), _income)
                            _update_data = dict(
                                                seven_days_income = seven_days_income,
                                                seven_days_ltv = seven_days_income/new_account_num
                                            )
                        elif diff_day == 7:
                            eight_day_income = self.deal_income(ltv_data.get('eight_day_income',0), ltv_data.get('seven_days_income',0), _income)
                            _update_data = dict(
                                                eight_day_income = eight_day_income,
                                                eight_day_ltv = eight_day_income/new_account_num
                                            )
                        elif diff_day == 8:
                            nine_day_income = self.deal_income(ltv_data.get('nine_day_income',0), ltv_data.get('eight_day_income',0), _income)
                            _update_data = dict(
                                                nine_day_income = nine_day_income,
                                                nine_day_ltv = nine_day_income/new_account_num
                                            )
                        elif diff_day == 9:
                            ten_day_income = self.deal_income(ltv_data.get('ten_day_income',0), ltv_data.get('nine_day_income',0), _income)
                            _update_data = dict(
                                                ten_day_income = ten_day_income,
                                                ten_day_ltv = ten_day_income/new_account_num
                                            )
                        elif diff_day == 10:
                            eleven_day_income = self.deal_income(ltv_data.get('eleven_day_income',0), ltv_data.get('ten_day_income',0), _income)
                            _update_data = dict(
                                                eleven_day_income = eleven_day_income,
                                                eleven_day_ltv = eleven_day_income/new_account_num
                                            )
                        elif diff_day == 11:
                            twelve_day_income = self.deal_income(ltv_data.get('twelve_day_income',0), ltv_data.get('eleven_day_income',0), _income)
                            _update_data = dict(
                                                twelve_day_income = twelve_day_income,
                                                twelve_day_ltv =twelve_day_income/new_account_num
                                            )
                        elif diff_day == 12:
                            thirteen_day_income = self.deal_income(ltv_data.get('thirteen_day_income',0), ltv_data.get('twelve_day_income',0), _income)
                            _update_data = dict(
                                                thirteen_day_income = thirteen_day_income,
                                                thirteen_day_ltv = thirteen_day_income/new_account_num
                                            )
                        elif diff_day == 13:
                            fourteen_day_income = self.deal_income(ltv_data.get('fourteen_day_income',0), ltv_data.get('thirteen_day_income',0), _income)
                            _update_data = dict(
                                                fourteen_day_income = fourteen_day_income,
                                                fourteen_day_ltv = fourteen_day_income/new_account_num
                                            )
                        elif diff_day == 14:
                            half_moon_income = self.deal_income(ltv_data.get('half_moon_income',0), ltv_data.get('fourteen_day_income',0), _income)
                            _update_data = dict(
                                                half_moon_income = half_moon_income,
                                                half_moon_ltv = half_moon_income/new_account_num
                                            )
                        elif diff_day <=29:
                            one_month_income = self.deal_income(ltv_data.get('one_month_income',0), ltv_data.get('half_moon_income',0), _income)
                            _update_data = dict(
                                                one_month_income = one_month_income,
                                                one_month_ltv = one_month_income/new_account_num
                                            )
                        elif diff_day <=44:
                            forty_five_income = self.deal_income(ltv_data.get('forty_five_income',0), ltv_data.get('one_month_income',0), _income)
                            _update_data = dict(
                                            forty_five_income = forty_five_income,
                                            forty_five_ltv = forty_five_income/new_account_num
                                )
                        elif diff_day <=59:
                            sixty_income = self.deal_income(ltv_data.get('sixty_income',0), ltv_data.get('forty_five_income',0), _income)
                            _update_data = dict(
                                            sixty_income = sixty_income,
                                            sixty_ltv = sixty_income/new_account_num
                                )
                        elif diff_day <=74:
                            seventy_five_income = self.deal_income(ltv_data.get('seventy_five_income',0), ltv_data.get('sixty_income',0), _income)
                            _update_data = dict(
                                            seventy_five_income =seventy_five_income,
                                            seventy_five_ltv = seventy_five_income/new_account_num
                                )
                        elif diff_day <=89:
                            ninety_income = self.deal_income(ltv_data.get('ninety_income',0), ltv_data.get('seventy_five_income',0), _income)
                            _update_data = dict(
                                        ninety_income = ninety_income,
                                        ninety_ltv = ninety_income/new_account_num
                                )
                        elif diff_day <=119:
                            four_month_income = self.deal_income(ltv_data.get('four_month_income',0), ltv_data.get('ninety_income',0), _income)
                            _update_data = dict(
                                        four_month_income = four_month_income,
                                        four_month_ltv = four_month_income/new_account_num
                                )
                        elif diff_day <=149:
                            five_month_income = self.deal_income(ltv_data.get('five_month_income',0), ltv_data.get('four_month_income',0), _income)
                            _update_data = dict(
                                        five_month_income = five_month_income,
                                        five_month_ltv = five_month_income/new_account_num
                                )
                        elif diff_day <=179:
                            six_month_income = self.deal_income(ltv_data.get('six_month_income',0), ltv_data.get('five_month_income',0), _income)
                            _update_data = dict(
                                        six_month_income = six_month_income,
                                        six_month_ltv = six_month_income/new_account_num
                                )
                        if _update_data:
                            ret = self.database.update('ltv_value',_update_data,dict(id = ltv_data['id']))
                            # print ret
                            # if ret:
                            self.database.update('atm',dict(ltv_flag = 1),dict(id = _atm_data.get('id')))
                    else:
                        self.database.update('atm',dict(ltv_flag = 1), dict(id = _atm_data.get('id')))

                else:
                    ret =self.database.update('atm',dict(ltv_flag = 1),dict(id = _atm_data.get('id')))

            now = datetime.datetime.now()
            ## 三天前
            three_day = now + datetime.timedelta(days=-2)
            ninety = now + datetime.timedelta(days = -179)
            all_data = self.database.three_ninety(int(three_day.strftime('%Y%m%d')),int(ninety.strftime("%Y%m%d")))

            for data in all_data:
                create_time = data.get('create_time')
                __update_data = dict()
                diff_day2 =  (datetime.datetime(now.year, now.month, now.day) - datetime.datetime(create_time.year, create_time.month,create_time.day)).days
                one_day_income = data.get('one_day_income')
                one_day_ltv = data.get('one_day_ltv')

                two_day_income  = data.get('two_day_income')
                two_day_ltv = data.get('two_day_ltv')

                three_days_income = data.get('three_days_income')
                three_days_ltv = data.get('three_days_ltv')

                four_day_income = data.get('four_day_income')
                four_day_ltv = data.get('four_day_ltv')

                five_day_income = data.get('five_day_income')
                five_day_ltv = data.get('five_day_ltv')

                six_day_income = data.get('six_day_income')
                six_day_ltv = data.get('six_day_ltv')

                seven_days_income = data.get('seven_days_income')
                seven_days_ltv = data.get('seven_days_ltv')

                eight_day_income = data.get('eight_day_income')
                eight_day_ltv = data.get('eight_day_ltv')

                nine_day_income = data.get('nine_day_income')
                nine_day_ltv = data.get('nine_day_ltv')

                ten_day_income = data.get('ten_day_income')
                ten_day_ltv = data.get('ten_day_ltv')

                eleven_day_income = data.get('eleven_day_income')
                eleven_day_ltv = data.get('eleven_day_ltv')

                twelve_day_income = data.get('twelve_day_income')
                twelve_day_ltv = data.get('twelve_day_ltv')

                thirteen_day_income  = data.get('thirteen_day_income')
                thirteen_day_ltv = data.get('thirteen_day_ltv')

                fourteen_day_income = data.get('fourteen_day_income')
                fourteen_day_ltv = data.get('fourteen_day_ltv')

                half_moon_income = data.get('half_moon_income')
                half_moon_ltv = data.get('half_moon_ltv')

                one_month_income = data.get('one_month_income')
                one_month_ltv = data.get('one_month_ltv')

                forty_five_income = data.get('forty_five_income')
                forty_five_ltv = data.get('forty_five_ltv')

                sixty_income = data.get('sixty_income')
                sixty_ltv = data.get('sixty_ltv')

                seventy_five_income = data.get('seventy_five_income')
                seventy_five_ltv = data.get('seventy_five_ltv')

                ninety_income = data.get('ninety_income')
                ninety_ltv = data.get('ninety_ltv')


                four_month_income = data.get('four_month_income')
                four_month_ltv = data.get('four_month_ltv')

                five_month_income = data.get('five_month_income')
                five_month_ltv = data.get('five_month_ltv')

                six_month_income = data.get('six_month_income')
                six_month_ltv = data.get('six_month_ltv')

                if diff_day2==2:
                    if one_day_income and not two_day_income:
                        two_day_income = one_day_income
                        two_day_ltv = one_day_ltv
                        __update_data['two_day_income'] = two_day_income
                        __update_data['two_day_ltv'] = two_day_ltv
                if diff_day2==3:
                    if two_day_income and not three_days_income:
                        three_days_income = two_day_income
                        three_days_ltv = two_day_ltv
                        __update_data['three_days_income'] = three_days_income
                        __update_data['three_days_ltv'] = three_days_ltv

                if diff_day2==4:
                    if three_days_income and not four_day_income:
                        four_day_income = three_days_income
                        four_day_ltv = three_days_ltv
                        __update_data['four_day_income'] = four_day_income
                        __update_data['four_day_ltv'] = four_day_ltv
                if diff_day2==5:
                    if four_day_income and not five_day_income:
                        five_day_income = four_day_income
                        five_day_ltv= four_day_ltv
                        __update_data['five_day_income'] = five_day_income
                        __update_data['five_day_ltv'] = five_day_ltv
                if diff_day2 ==6:
                    if five_day_income and not six_day_income:
                        six_day_income = five_day_income
                        six_day_ltv = five_day_ltv
                        __update_data['six_day_income'] = six_day_income
                        __update_data['six_day_ltv'] = six_day_ltv


                if diff_day2 ==7:
                    if six_day_income and not seven_days_income:
                        seven_days_income = six_day_income
                        seven_days_ltv = six_day_ltv
                        __update_data['seven_days_income'] = seven_days_income
                        __update_data['seven_days_ltv'] = seven_days_ltv


                if diff_day2 ==8:
                    if seven_days_income and not eight_day_income:
                        eight_day_income = seven_days_income
                        eight_day_ltv = seven_days_ltv
                        __update_data['eight_day_income'] = eight_day_income
                        __update_data['eight_day_ltv'] = eight_day_ltv


                if diff_day2 ==9:
                    if eight_day_income and not nine_day_income:
                        nine_day_income = eight_day_income
                        nine_day_ltv = eight_day_ltv
                        __update_data['nine_day_income'] = nine_day_income
                        __update_data['nine_day_ltv'] = nine_day_ltv

                if diff_day2 ==10:
                    if nine_day_income and not ten_day_income:
                        ten_day_income = nine_day_income
                        ten_day_ltv = nine_day_ltv
                        __update_data['ten_day_income'] = ten_day_income
                        __update_data['ten_day_ltv'] = ten_day_ltv


                if diff_day2 ==11:
                    if ten_day_income and not eleven_day_income:
                        eleven_day_income = ten_day_income
                        eleven_day_ltv = ten_day_ltv
                        __update_data['eleven_day_income'] = eleven_day_income
                        __update_data['eleven_day_ltv'] = eleven_day_ltv
                if diff_day2 ==12:
                    if eleven_day_income and not twelve_day_income:
                        twelve_day_income = eleven_day_income
                        twelve_day_ltv = eleven_day_ltv
                        __update_data['twelve_day_income'] = twelve_day_income
                        __update_data['twelve_day_ltv'] = twelve_day_ltv

                if diff_day2 ==13:
                    if twelve_day_income and not thirteen_day_income:
                        thirteen_day_income = twelve_day_income
                        thirteen_day_ltv = twelve_day_ltv
                        __update_data['thirteen_day_income'] = thirteen_day_income
                        __update_data['thirteen_day_ltv'] = thirteen_day_ltv

                if diff_day2 ==14:
                    if thirteen_day_income and not fourteen_day_income:
                        fourteen_day_income = thirteen_day_income
                        fourteen_day_ltv = thirteen_day_ltv
                        __update_data['fourteen_day_income'] = fourteen_day_income
                        __update_data['fourteen_day_ltv'] = fourteen_day_ltv
                if diff_day2 ==15:
                    if fourteen_day_income and not half_moon_income:
                        half_moon_income = fourteen_day_income
                        half_moon_ltv = fourteen_day_ltv
                        __update_data['half_moon_income'] = half_moon_income
                        __update_data['half_moon_ltv'] = half_moon_ltv
                if diff_day2 >15:
                    if half_moon_income and not one_month_income:
                        one_month_income = half_moon_income
                        one_month_ltv = half_moon_ltv
                        __update_data['one_month_income'] = one_month_income
                        __update_data['one_month_ltv'] = one_month_ltv

                if diff_day2 >29:
                    if one_day_income and not forty_five_income:
                        forty_five_income = one_month_income
                        forty_five_ltv = one_month_ltv
                        __update_data['forty_five_income'] = forty_five_income
                        __update_data['forty_five_ltv'] = forty_five_ltv
                if diff_day2 >44:
                    if one_day_income and not sixty_income:
                        sixty_income = forty_five_income
                        sixty_ltv = forty_five_ltv
                        __update_data['sixty_income'] = sixty_income
                        __update_data['sixty_ltv'] = sixty_ltv

                if diff_day2 >59:
                    if one_day_income and not seventy_five_income:
                        seventy_five_income = sixty_income 
                        seventy_five_ltv = sixty_ltv
                        __update_data['seventy_five_income'] = seventy_five_income
                        __update_data['seventy_five_ltv'] = seventy_five_ltv
                if diff_day2 >74:
                    if one_day_income and not ninety_income:
                        ninety_income = seventy_five_income
                        ninety_ltv = seventy_five_ltv
                        __update_data['ninety_income'] = ninety_income
                        __update_data['ninety_ltv'] = ninety_ltv

                if diff_day2 >89:
                    if one_day_income and not four_month_income:
                        four_month_income = ninety_income
                        four_month_ltv = ninety_ltv
                        __update_data['four_month_income'] = four_month_income
                        __update_data['four_month_ltv'] = four_month_ltv
                if diff_day2 >119:
                    if one_day_income and not five_month_income:
                        five_month_income = four_month_income
                        five_month_ltv = four_month_ltv
                        __update_data['five_month_income'] = five_month_income
                        __update_data['five_month_ltv'] = five_month_ltv
                if 179>diff_day2 >149:
                    if one_day_income and not six_month_income:
                        six_month_income = one_day_income
                        six_month_ltv = five_month_ltv
                        __update_data['six_month_income'] = six_month_income
                        __update_data['six_month_ltv'] = six_month_ltv
                if __update_data:
                    self.database.update('ltv_value', __update_data, {"id":data.get('id')})
            # seven_day = now + datetime.timedelta(days=-6)
            # five_day  = now + datetime.timedelta(days=-14)
            # one_month = now + datetime.timedelta(days=-29)
            # forty_five = now + datetime.timedelta(days = -44)
            # sixty = now + datetime.timedelta(days = -59)
            # seventy_five = now + datetime.timedelta(days = -74)


            # three_ymd = int(three_day.strftime('%Y%m%d'))
            # seven_ymd = int(seven_day.strftime('%Y%m%d'))
            # five_ymd = int(five_day.strftime("%Y%m%d"))
            # one_month_ymd = int(one_month.strftime('%Y%m%d'))
            # forty_five_ymd = int(forty_five.strftime('%Y%m%s'))
            # sixty_ymd = int(sixty.strftime('%Y%m%d'))
            # seventy_five_ymd = int(seventy_five.strftime('%Y%m%d'))
            # ninety_ymd = int(ninety.strftime('%Y%m%d'))

            # three_datas = self.database.ltv(dict(ymd = ymd, _ymd = three_ymd))
            # seven_data = self.database.ltv(dict(ymd = ymd, _ymd = seven_ymd))
            # fif_data = self.database.ltv(dict(ymd =ymd, _ymd = five_ymd))
            # one_month_datas = self.database.ltv(dict(ymd = ymd, _ymd =one_month_ymd))
            # forty_five_datas = self.database.ltv(dict(ymd = ymd, _ymd = forty_five_ymd))
            # sixty_datas = self.database.ltv(dict(ymd = ymd, _ymd = sixty_ymd))
            # seventy_five_datas = self.database.ltv(dict(ymd =ymd, _ymd = seventy_five_ymd))
            # ninety_datas = self.database.ltv(dict(ymd = ymd , _ymd = ninety_ymd))
            # self.mysql_ltv(three_datas, three_ymd, 'three_days_income', 'three_days_ltv')
            # self.mysql_ltv(seven_data, seven_ymd, 'seven_days_income', 'seven_days_ltv')
            # self.mysql_ltv(fif_data, five_ymd, 'half_moon_income', 'half_moon_ltv')
            # self.mysql_ltv(one_month_datas, one_month_ymd, 'one_month_income', 'one_month_ltv')
            # self.mysql_ltv(forty_five_datas, forty_five_ymd, 'forty_five_income', 'forty_five_ltv')
            # self.mysql_ltv(sixty_datas, sixty_ymd ,'sixty_income', 'sixty_ltv')
            # self.mysql_ltv(seventy_five_datas, seventy_five_ymd, 'seventy_five_income', 'seventy_five_ltv')
            # self.mysql_ltv(ninety_datas, ninety_ymd, 'ninety_income', 'ninety_ltv')
            gevent.sleep(random.randint(sleep_stime,sleep_etime))

    def mysql_ltv(self, data, d_date, field_name, field_name2):
        for _data in data:
            gevent.sleep(0.1)
            _check = dict()
            _check['s_uid'] = _data.get('s_uid','')
            _check['channel_name'] = _data.get('ch','')
            _check['d_date'] = d_date
            check_data = self.database.select('ltv_value',_check)
            if check_data:
                new_account_num =  check_data.get('new_account_num')
                ordermoney =  _data.get('ordermoney',0)
                query = {field_name : ordermoney, field_name2: ordermoney/new_account_num}
                self.database.update('ltv_value',query , dict(id = check_data['id']))

##  处理活跃用户
class deal_ac_player(object):

    @catch_except('deal_ac_player')
    def __init__(self):
        self.database = connDB()
        self.date = 20180307
        self.now = datetime.datetime.now()
        self.ishour = False
        self.two_action = False
    @catch_except('deal_ac_player')
    def starting(self):
        while 1:
            pass
            gevent.sleep(10)
            now = datetime.datetime.now()
            hour = now.strftime('%H')
            if hour == ACTION and self.two_action is False and self.ishour!=hour:
                self.two_action =True
            if int((now+ datetime.timedelta(days = -1)).strftime('%Y%m%d'))==self.date or self.two_action:
                now = now + datetime.timedelta(days = -1)
            self.now = now
            now_ymd = int(now.strftime('%Y%m%d'))
            month_ago = now + datetime.timedelta(days = -29)
            month_ago_ymd = int(month_ago.strftime('%Y%m%d'))
            month_ago_d = self.database.action_player(dict(start_ago = month_ago_ymd,end_ago = now_ymd),'mth_ac_num')
            w_ymd = int((now + datetime.timedelta(days = -6)).strftime('%Y%m%d'))
            th_ymd = int((now + datetime.timedelta(days = -2)).strftime('%Y%m%d'))
            week_datas = self.database.action_player(dict(start_ago = w_ymd, end_ago = now_ymd),'w_ac_num')
            th_datas = self.database.action_player(dict( start_ago = th_ymd, end_ago = now_ymd),'th_ac_num')
            td_datas = self.database.action_player(dict(start_ago = now_ymd),'td_ac_num')
            self.deal_d(th_datas,td_datas)
            self.deal_d(week_datas, th_datas)
            self.deal_d(month_ago_d, week_datas, now_ymd = now_ymd)
            self.date = int(datetime.datetime.now().strftime('%Y%m%d'))
            self.ishour  = hour
            self.two_action = False
            gevent.sleep(random.randint(sleep_stime, sleep_etime))
    def deal_d(self,datas,items,now_ymd = None):
        data_len = len(datas)
        for i in range(data_len):
            for item in items:
                if datas[i].get('s_uid','') == item.get('s_uid') and datas[i].get('channel_name','')==item.get('channel_name'):
                    datas[i].update(item)
                    continue
        
        if now_ymd:
            for _data in datas:
                self.deal_ac_data(now_ymd,_data)
    def deal_ac_data(self, ymd,data = dict()):
        s_uid = data.pop('s_uid','')
        _check_condi = dict(
                        s_uid = s_uid,
                        channel_name = data.get('channel_name',''),
                        d_date = ymd
                )
        ac_data = self.database.select('action_player',_check_condi)
        if ac_data:
            ## 更新
            self.dua_wua(data)
            self.database.update('action_player',data,dict(id = ac_data.get('id')))
        else:
            ## 插入
            data.update(_check_condi)
            self.dua_wua(data)
            server_d = self.database.select('servers_data',dict(uid = s_uid))
            if server_d:
                data['server_name'] = server_d.get('name','')
            data['create_time'] =self.now
            self.database.add('action_player',data)

    def dua_wua(self,data):
        td_ac_num  = data.get('td_ac_num')
        if td_ac_num:
            data['dw_ac'] = '%.2f' % (td_ac_num/data.get('w_ac_num'))
            data['dm_ac'] = '%.2f' % (td_ac_num/data.get('mth_ac_num'))


## 每小时实时数据
class hours_statistics(object):

    @catch_except('hours_statistics')
    def __init__(self):
        self.database = connDB()
        self.last_hour = '00'
    @catch_except('hours_statistics')
    def starting(self):
        while 1:
            ## 算没消失新增加的设备
            ##  每小时统计一次

            gevent.sleep(1)
            now = datetime.datetime.now()
            if now.strftime('%M%S') == '0000':
                pass
                ## 当前时间所有处的小时时间点
            data_dicts =dict()
            _now = datetime.datetime(now.year, now.month, now.day, now.hour)
            __now  = _now + datetime.timedelta(hours=-1)
            hours = __now.strftime('%H')
            if hours != self.last_hour:
                _check_d =  dict(start_time = __now, end_time = _now)
                # new_fetch_ser_ds = self.database.deal_fetch_s_by_hour(_check_d)
                new_acc_datas = self.database.s_regist_login(_check_d)
                ac_u_datas  =self.database.s_acount_t(_check_d)
                pay_income_datas = self.database.pay_income(_check_d)
                ##  新注册账号数
                # new_regist_d = self.database.deal_equipment_by_hour(_check_d)
                # self.merge(data_dicts, new_fetch_ser_ds)
                self.merge(data_dicts, new_acc_datas)
                self.merge(data_dicts, ac_u_datas)
                self.merge(data_dicts, pay_income_datas)
                # self.merge(data_dicts, new_regist_d)
                ## 本统计只做插入操作
                d_date = int(__now.strftime('%Y%m%d'))
                for v in data_dicts.values():
                    gevent.sleep(0.1)
                    v['d_date'] = d_date
                    v['hours'] = hours
                    check_data = self.database.select('hours_statistics',dict(
                                                            d_date = d_date,
                                                            hours = hours,
                                                            channel_name = v.get('channel_name'),
                                                            s_uid = v.get('s_uid')
                                    ))
                    if not check_data:
                        if hours==23:
                            v['create_time'] = datetime.datetime(__now.year, __now.month, __now.day,23, 59, 00)
                        self.database.add('hours_statistics',v)

                self.last_hour = hours
            gevent.sleep(random.randint(sleep_stime,sleep_etime))
    def merge(self,d_dict,d_list):
        for data in d_list:
            key = data.get('channel_name','') + '_' + data.get('s_uid','')
            if d_dict.get(key):
                _dict = d_dict[key]
                _dict.update(data)
            else:
                d_dict[key] = data
##  处理金币产出消耗
class deal_lgold(object):

    @catch_except('deal_lgold')
    def __init__(self):
        self.database = connDB()


    @catch_except('deal_lgold')
    def starting(self):
        while 1:
            pass


### 处理钻石产出消耗
class deal_ldiamond(object):

    @catch_except('deal_ldiamond')
    def __init__(self):
        self.database = connDB()


    @catch_except('deal_ldiamond')
    def starting(self):
        while 1:
            pass

## 用户产出消耗物品
class deal_litem(object):

    @catch_except('deal_litem')
    def __init__(self):
        self.database = connDB()


    @catch_except('deal_litem')
    def starting(self):
        cols = ('uid', 'pid', 'time', 'type', 's_uid', 'd_date', 'ch', 'chmc', 'chsc', 'item_desc','status_flag','unique_id','goods_id')
        while 1:
            gevent.sleep(1)
            litem_data = self.database.select_all('litem',dict(deal_flag=0),limit=1000)
            if len(litem_data) ==0:
                gevent.sleep(2)

            values = []
            for resp in litem_data:
                s_uid = resp.get('s_uid')
                unique_id = resp.get('unique_id','')
                value = []
                if resp.get('created') != '{}':       
                    created = json.loads(resp['created'])
                    if isinstance(created,dict):
                        for k,v in created.items():
                            if isinstance(v,list):
                                item_desc=str(v[0])+','+str(v[1])
                            else:
                                item_desc = str(k)+','+str(v)
                            values.append((resp.get('uid'),
                                                resp.get('pid'),
                                                resp.get('time'),
                                                resp.get('type'),
                                                s_uid,
                                                resp.get('d_date'),
                                                resp.get('ch',''),
                                                resp.get('chmc',''),
                                                resp.get('chsc',''),
                                                item_desc,
                                                1,
                                                unique_id,
                                                k
                                                ))

                if resp.get('deleted') != '{}':
                    deleted = json.loads(resp['deleted'])
                    if isinstance(deleted,dict):
                        for k,v in deleted.items():
                            if isinstance(v,list):
                                item_desc=str(v[0])+','+str(v[1])
                            else:
                                item_desc = str(k)+','+str(v)
                            values.append((resp.get('uid'),
                                                resp.get('pid'),
                                                resp.get('time'),
                                                resp.get('type'),
                                                s_uid,
                                                resp.get('d_date'),
                                                resp.get('ch',''),
                                                resp.get('chmc',''),
                                                resp.get('chsc',''),
                                                item_desc,
                                                2,
                                                unique_id,
                                                k
                                                ))


                self.database.update('litem',dict(deal_flag=1),dict(id=resp.get('id')))
            if values:
                sql = self.database.add2('litems',cols)
                self.database.addall(sql,values)

## 用户产出消耗装备
class deal_lequipment(object):

    @catch_except('deal_litem')
    def __init__(self):
        self.database = connDB()


    @catch_except('deal_litem')
    def starting(self):
        cols = ('uid', 'pid', 'time', 'type', 's_uid', 'd_date', 'ch', 'chmc', 'chsc', 'item_desc','status_flag','unique_id','goods_id')
        while 1:
            gevent.sleep(1)
            litem_data = self.database.select_all('lequipment',dict(deal_flag=0),limit=1000)
            if len(litem_data) ==0:
                gevent.sleep(2)

            values = []
            for resp in litem_data:
                s_uid = resp.get('s_uid')
                unique_id = resp.get('unique_id','')
                value = []
                if resp.get('created') != '{}':       
                    created = json.loads(resp['created'])
                    if isinstance(created,dict):
                        for k,v in created.items():
                            if isinstance(v,list):
                                item_desc=str(v[0])+','+str(v[1])
                            else:
                                item_desc = str(v)
                            values.append((resp.get('uid'),
                                                resp.get('pid'),
                                                resp.get('time'),
                                                resp.get('type'),
                                                s_uid,
                                                resp.get('d_date'),
                                                resp.get('ch',''),
                                                resp.get('chmc',''),
                                                resp.get('chsc',''),
                                                item_desc,
                                                1,
                                                unique_id,
                                                k
                                                ))

                if resp.get('deleted') != '{}':
                    deleted = json.loads(resp['deleted'])
                    if isinstance(deleted,dict):
                        for k,v in deleted.items():
                            if isinstance(v,list):
                                item_desc=str(k)+','+str(v[0])+','+str(v[1])
                            else:
                                item_desc = str(k)+','+str(v)
                            values.append((resp.get('uid'),
                                                resp.get('pid'),
                                                resp.get('time'),
                                                resp.get('type'),
                                                s_uid,
                                                resp.get('d_date'),
                                                resp.get('ch',''),
                                                resp.get('chmc',''),
                                                resp.get('chsc',''),
                                                item_desc,
                                                2,
                                                unique_id,
                                                k
                                                ))


                self.database.update('lequipment',dict(deal_flag=1),dict(id=resp.get('id')))
            if values:
                sql = self.database.add2('lequipments',cols)
                self.database.addall(sql,values)


##  货币进毁存
class currencyGDS(object):

    @catch_except('currencyGDS')
    def __init__(self):
        self.database  = connDB()
        self.date = 20180307
        self.now = datetime.datetime.now()
        self.ishour = False
        self.two_action = False  
    @catch_except('currencyGDS')
    def starting(self):
        while 1:
            gevent.sleep(1)
            gold_dict = dict()
            diamonds_dict = dict()
            now = datetime.datetime.now()
            hour = now.strftime('%H')
            if hour == ACTION and self.two_action is False and self.ishour!=hour:
                self.two_action =True
            if int((now + datetime.timedelta(days = -1)).strftime('%Y%m%d'))==self.date or self.two_action:
                now = now + datetime.timedelta(days = -1)
            self.now = now
            ymd  = int(now.strftime('%Y%m%d'))
            ## c_type =1为钻石2为金币，status_flag=1为系统产出，2为消耗，3为充值所得
            ## 金币
            get_glold = self.database.select_currency(['system_output','lgold',1,ymd])
            output_glod = self.database.select_currency(['total_drain','lgold',2,ymd])
            ## 钻石
            ## 获取系统产出的钻石系统
            get_sys_diamonds = self.database.select_currency(['system_output','ldiamond',1,ymd])
            ## 获取消耗
            get_drain_diamonds  = self.database.select_currency(['total_drain','ldiamond',2,ymd])
            ## 获取充值所得
            get_atm_diamonds = self.database.diamdon(['atm_get',ymd,5])
            self.deal(gold_dict, get_glold)
            self.deal(gold_dict, output_glod)

            self.deal(diamonds_dict, get_sys_diamonds)
            self.deal(diamonds_dict, get_atm_diamonds)
            self.deal(diamonds_dict, get_drain_diamonds)
            ## 金币
            self.dealOfdict(gold_dict,2,ymd)
            ## 钻石
            self.dealOfdict(diamonds_dict, 1, ymd)
            self.date = int(datetime.datetime.now().strftime('%Y%m%d'))
            self.ishour = hour
            self.two_action = False
            gevent.sleep(random.randint(sleep_stime,sleep_etime))
    def dealOfdict(self,data,c_type,ymd):
        for v in data.values():
            gevent.sleep(0.1)
            s_uid = v.pop('s_uid','')
            _check = dict(
                    channel_name = v.pop('ch',''),
                    s_uid = s_uid,
                    d_date = ymd,
                    c_type = c_type
                )
            if not v.get('server_name'):
                server_data = self.database.select('servers_data',dict(uid =s_uid))
                if server_data:
                    v['server_name'] = server_data.get('name','')
            check_data  = self.database.select('currency_cond',_check)
            if check_data:
                ## 存在更新
                self.database.update('currency_cond',v,dict(id = check_data['id']))
            else:
                v.update(_check)
                v['create_time'] = self.now
                self.database.add('currency_cond',v)
    def deal(self,d_dict,d_list):
        for data in d_list:
            key = data.get('ch','') + ':' + data.get('s_uid')
            if d_dict.get(key):
                _data = d_dict[key]
                _data.update(data)
            else:
                d_dict[key] = data
                
## 产出消耗分布

class ODDistributoin(object):

    @catch_except('ODDistributoin')
    def __init__(self):
        self.database = connDB()
        self.date = 20180307
        self.ishour = False
        self.two_action=  False
    @catch_except('ODDistributoin')
    def starting(self):
        while 1:
            gevent.sleep(2)
            now = datetime.datetime.now()
            hour = now.strftime('%H')
            if hour == ACTION and self.two_action is False and self.ishour!=hour:
                self.two_action =True
            if int((now + datetime.timedelta(days = -1)).strftime('%Y%m%d')) == self.date or self.two_action:
                now = now + datetime.timedelta(days = -1)
            d_date = int(now.strftime('%Y%m%d'))
            ## 钻石系统产出
            sys_output_by_diamonds = list(self.database.outputdrain_distributoin(['ldiamond', 1, d_date]))
            ## 钻石消耗
            drain_diamond = list(self.database.outputdrain_distributoin(['ldiamond', 2, d_date]))
            ## 钻石充值所得
            atm_diamond = list(self.database.outputdrain_distributoin(['ldiamond', 3, d_date]))
            for i in range(len(atm_diamond)):
                atm_diamond[i]['status_flag'] =1
            ## 获得金币
            get_glold = list(self.database.outputdrain_distributoin(['lgold', 1, d_date]))
            ## 金币消耗
            drain_gold = list(self.database.outputdrain_distributoin(['lgold', 2, d_date]))

            all_ldiamond_list = sys_output_by_diamonds+drain_diamond +atm_diamond
            all_lgold_list = get_glold + drain_gold
            self.deal_ood(all_ldiamond_list,1,d_date)
            self.deal_ood(all_lgold_list,2,d_date)
            self.date = int(datetime.datetime.now().strftime('%Y%m%d'))
            self.ishour = hour
            self.two_action = False
            gevent.sleep(random.randint(sleep_stime, sleep_etime))
    def deal_ood(self,data_list,c_type,d_date):
        for data in data_list:
            gevent.sleep(0.1)
            _check_condition = dict(
                                    channel_name = data.get('channel_name',''),
                                    s_uid = data.get('s_uid',''),
                                    d_date = d_date,
                                    type = data.get('type'),
                                    status_flag = data.get('status_flag',1)
                                )
            ODD_data = self.database.select('OD_distributoin',_check_condition)
            if ODD_data:
                ## 更新
                ret = self.database.update('OD_distributoin',data,dict(id = ODD_data['id']))
            else:
                data['d_date'] = d_date
                data['c_type'] = c_type
                ret =self.database.add('OD_distributoin',data) 
class deal_task_check_point(object):
    @catch_except('deal_task_check_point')
    def __init__(self):

        self.database = connDB()
        self.date = 20180308
        self.ishour = False
        self.two_action = False
    @catch_except('deal_task_check_point')
    def starting(self):
        while 1:
            gevent.sleep(2)
            init_dict = dict()
            now = datetime.datetime.now()
            hour = now.strftime('%H')
            if hour == ACTION and self.two_action is False and self.ishour!=hour:
                self.two_action =True
            if int((now + datetime.timedelta(days = -1)).strftime('%Y%m%d'))==self.date or self.two_action:
                now = now + datetime.timedelta(days = -1)
            d_date = int(now.strftime('%Y%m%d'))
            ## 获取所有的挑战次数以及人数
            all_data = self.database.relic_hero(d_date)
            ### 
            s_data = self.database.suss_(d_date)

            room_info = self.database.relic_hero_room_info(d_date)
            self.deal(init_dict, all_data)
            self.deal(init_dict, s_data)
            self.deal(init_dict,room_info)
            for v in init_dict.values():
                v['avg_time'] = int(v.pop('avg_time',0))
                v['d_date'] = d_date
                v['challenge_num'] = v.pop('challenge_num',0)
                check_cond = dict(
                            ch = v.get('ch',''),
                            s_uid = v.get('s_uid',''),
                            b_type = v.get('b_type',0),
                            name = v.get('name',''),
                            d_date =  d_date
                    )
                check_data = self.database.select('task_checkpoint', check_cond)
                if check_data:
                    ## 更新
                    self.database.update('task_checkpoint', v, dict(id = check_data.get('id')))
                else:
                    v['time'] = now
                    self.database.add('task_checkpoint',v)
            self.date = int(datetime.datetime.now().strftime('%Y%m%d'))
            self.ishour = hour
            self.two_action = False
            gevent.sleep(sleep_stime)
    @catch_except('deal_task_check_point')
    def deal(self, init_dict, data):
        for _data in data:
            if _data.get('s_flag'):
                _data['type'] = 'GVE组队战'
                s_flag = _data.pop('s_flag',1)
                count = _data.pop('count',0)
                if s_flag==2:
                    _data['create_room_num'] = count
                elif s_flag==3:
                    _data['dis_room_num'] = count 
            if not _data.get('player_num'):
                _data['player_num'] = 0
            key = "_".join(map(str,(_data.get('ch',''),_data.get('s_uid',''),_data.get('name',''),_data.get('b_type'))))
            if init_dict.get(key):
                k_value = init_dict.get(key)
                k_value.update(_data)
            else:
                init_dict[key] = _data


class spirit_level(object):
    @catch_except('spirit_level')
    def __init__(self):

        self.database = connDB()
        self.date = 20180110
        self.ishour = False
        self.two_action = False
    @catch_except('spirit_level')
    def starting(self):
        while 1:
            pass
            gevent.sleep(2)
            delete_table_l = ['spirit_level','partner_device','magic_stone']
            now = datetime.datetime.now()
            hour = now.strftime('%H')
            if hour == ACTION and self.two_action is False and self.ishour!=hour:
                self.two_action =True
            if int((now + datetime.timedelta(days = -1)).strftime('%Y%m%d')) ==self.date or self.two_action:
                now = now + datetime.timedelta(days = -1)
            d_date = int(now.strftime('%Y%m%d'))
            all_data = self.database.spirit_level(d_date)
            panter_level  =  self.database.pa_level(d_date)
            mg_stones = self.database.mg_stone(d_date)
            for i in range(len(delete_table_l)):
                self.database.delete(delete_table_l[i], dict(d_date = d_date))
            self.deal_data(all_data ,delete_table_l[0],now)
            self.deal_data(panter_level ,delete_table_l[1],now)
            self.deal_data(mg_stones, delete_table_l[2],now)
            self.date = int(datetime.datetime.now().strftime('%Y%m%d'))
            self.ishour = hour
            self.two_action = False
            gevent.sleep(sleep_stime)



    def deal_data(self, data, table,create_time):
        for _data in data:
            if table =='spirit_level':
                _server_d = self.database.select('servers_data',dict(uid = _data.get('s_uid','')))
                _data['server_name'] = _server_d.get('name','')
                _data['create_time'] = create_time
            else:
                _data['time'] = create_time
            self.database.add(table,_data)
## 冒险团

class ad_groups(object):
    @catch_except('ad_groups')
    def __init__(self):

        self.database = connDB()

    @catch_except('ad_groups')
    def starting(self):
        while 1:
            pass
            gevent.sleep(2)
            init_dict = dict()
            now = datetime.datetime.now()
            ## 每小时30分钟时处理一次
            # if now.strftime('%M%S') == '3000':
            ### 当天凌晨6点时间
            now_six = datetime.datetime(now.year, now.month, now.day,6,0,0)
            ## 昨天凌晨6点
            yes_six = now_six +  datetime.timedelta(days = -1)
            d_date =int(yes_six.strftime('%Y%m%d'))

            ad_group_datas = self.database.onlineadgroup(yes_six, now_six)
            ad_dismiss_datas = self.database.ad_dismiss(yes_six, now_six)
            self.deal(init_dict, ad_group_datas)
            self.deal(init_dict, ad_dismiss_datas)
            for v in init_dict.values():
                s_uid = v.get('s_uid')
                v['d_date'] = d_date
                v['create_time'] = yes_six
                check_cond = dict(
                            d_date = v['d_date'],
                            s_uid = s_uid,
                            channel_name = v['channel_name']
                    )
                check_data = self.database.select('ad_datas',check_cond)
                if check_data:
                    ## 更新数据
                    self.database.update('ad_datas', v, dict(id = check_data['id']))
                else:
                    server_d = self.database.select('servers_data', dict(uid = s_uid))
                    v['server_name'] = server_d.get('name','')
                    self.database.add('ad_datas',v)

            ## 冒险团等级，仓库等级等
            self.ad_level(yes_six, now_six)
            gevent.sleep(1200)

    def deal(self, d_dict, d_list):
        for _d_list in d_list:
            key = _d_list.get('s_uid','')+ _d_list.get('channel_name','')
            if d_dict.get(key):
                _d_dict = d_dict[key]
                _d_dict.update(_d_list)
            else:
                d_dict[key] = _d_list

    def ad_level(self, yes_six, now_six):
        _d_date = int(yes_six.strftime('%Y%m%d'))
        check_list = ['guild_lv','shop_lv','room_lv','storehouse','prosperity_lv']
        for i in range(len(check_list)):
            _type = i+1
            _ad_datas = []
            _ad_datas = self.database.ad_level_dis(yes_six, now_six,check_list[i])
            for _ad_data in _ad_datas:
                _ad_data['d_date'] = _d_date
                _ad_data['create_time'] = yes_six
                s_uid = _ad_data.get('s_uid','')
                ad_check = dict(
                            d_date = _d_date,
                            s_uid = s_uid,
                            channel_name = _ad_data.get('channel_name',''),
                            type = _type,
                            level = _ad_data.get('level',1)
                    )
                check_data = self.database.select('ad_level',ad_check)
                if check_data:
                    ## 更新数据
                    self.database.update('ad_level', _ad_data, dict(id = check_data['id']))
                else:
                    server_d = self.database.select('servers_data', dict(uid = s_uid))
                    _ad_data['server_name'] = server_d.get('name','')
                    _ad_data['type'] = _type
                    self.database.add('ad_level',_ad_data)

## 故事关卡

class cstory_adventure_medal(object):
    @catch_except('ad_groups')
    def __init__(self):
        self.database = connDB()

    @catch_except('cstory_adventure_medal')
    def starting(self):
        while 1:
            gevent.sleep(1)
            now   = datetime.datetime.now()
            ymd = int(now.strftime('%Y%m%d'))
            medal_datas = self.database.medals(ymd)
            onece_medal_datas = self.database.onece_medals(ymd)
            playerlv_datas = self.database.player_lv(ymd)
            
            sweepnum_d = self.database.sweepnum(ymd)
            all_medal = self.deal_medal(medal_datas,dict())
            all_medal = self.deal_medal(sweepnum_d,all_medal,True)
            onece_medal = self.deal_medal(onece_medal_datas,dict())
            for _medal in all_medal.values():
                self.deal_to_sql(_medal)
            for __medal in onece_medal.values():
                self.deal_to_sql(__medal,onece=1)

            deal_p_d = self.deal_player_lv(playerlv_datas,dict())
            for _Pd in deal_p_d.values():
                pd_check = dict(
                            ch = _Pd.pop('ch',''),
                            s_uid = _Pd.pop('s_uid',''),
                            event_id = _Pd.pop('event_id',''),
                            d_date = _Pd.pop('d_date',0)
                    )
                pd_c_data = self.database.select('cstory_adventure_playerlv',pd_check)
                player_lvs = json.dumps(_Pd.get('player_lvs',dict()))
                if pd_c_data:
                    self.database.update('cstory_adventure_playerlv',dict(player_lvs = player_lvs),dict(id =pd_c_data.get('id',0) ))
                else:
                    _Pd['player_lvs'] = player_lvs
                    _Pd.update(pd_check)
                    self.database.add('cstory_adventure_playerlv',_Pd)
            gevent.sleep(sleep_stime)

    def deal_to_sql(self,__medal,onece = 0):
        __medal['onece'] = onece
        check_onece = dict(
                            d_date = __medal.pop('d_date',0),
                            ch = __medal.pop('ch',''),
                            s_uid = __medal.pop('s_uid',0),
                            event_id = __medal.pop('event_id',0),
                            onece = __medal.pop('onece',1)
                            )
        check_d = self.database.select('cstory_adventure_medal',check_onece)
        if check_d:
            __medal.pop('name','')
            self.database.update('cstory_adventure_medal',__medal,dict(id = check_d.get('id')))
        else:
            __medal.update(check_onece)
            self.database.add('cstory_adventure_medal',__medal)      
    @catch_except('cstory_adventure_medal')
    def deal_medal(self,data,ret_data,swee_flag = False):
        medal_dict = {0:'zero_medal',1:"one_medal",2:"two_medal",3:"three_medal"}
        for  _data in data:
            key = _data.get("ch","") +str(_data.get('s_uid',''))+str(_data.get('event_id',''))
            if key:
                if swee_flag is False:
                    count = _data.pop('count',0)
                    medal = _data.pop('medal',0)
                    __data = ret_data.get(key)
                    if __data:
                        __data[medal_dict[medal]] = count
                    else:
                        _data[medal_dict[medal]] = count
                        ret_data[key] = _data
                else:
                    __data = ret_data.get(key)
                    if __data:
                        __data.update(_data)
                    else:
                        ret_data[key] = _data                   
        return ret_data

    def deal_player_lv(self,data,ret_data):
        for _data in data:
            key = _data.get("ch","") +str(_data.get('s_uid',''))+str(_data.get('event_id',''))
            if key:
                player_lv =  _data.pop('player_lv',1)
                count = _data.pop('count',0)
                __data = ret_data.get(key)
                if __data:
                    player_lvs = __data.get('player_lvs',dict())
                    player_lvs[player_lv] = count
                else:
                    _data['player_lvs'] = {player_lv:count}
                    ret_data[key] = _data
        return ret_data



## 超觉醒节点

class superstars(object):
    @catch_except('superstars')
    def __init__(self):
        self.database = connDB()
        self.start = 0
        self._deal = False
    @catch_except('superstars')
    def starting(self):
        while 1:
            attr_type_d = {"hp":1,"atk":2,"reply":3,'defense':4,"crt":5}
            gevent.sleep(2)
            superstar_data = self.database.select_all('superstar',dict(deal_flag=0),limit=1000)
            now = datetime.datetime.now()
            year_week =  now.isocalendar()
            if len(superstar_data)==0:
                gevent.sleep(2)
            reset_d = dict()
            for data in superstar_data:
                uid = data.get('uid','')
                pid = data.get('pid','')
                s_uid = data.get('s_uid','')
                pname= data.get('pname','')
                nodeid = data.get('nodeid','')
                fid =data.get('fid','')
                d_date = data.get('d_date',0)
                fname = data.get('fname','')
                _time = data.get('time',0)
                base_data = dict(uid = uid,
                                pid = pid,
                                s_uid = s_uid,
                                nodeid = nodeid,
                                fid = fid,
                                fname  = fname
                    )


                optype = data.get('optype',1)
                attr = data.get('attr','[]')
                attr=json.loads(attr)
                _basedata = dict(
                                s_uid =int(s_uid),
                                uid = uid,
                                pid = pid,
                                pname = pname,
                                fid = fid,
                                fname = fname,
                                nodeid = nodeid,
                                op_type = optype,
                                time = _time
                            )   
                ## 
                # check_pl = dict(s_uid = s_uid,pid = pid,uid= uid)
                # ck_data = self.database.select('superstartoplayer',check_pl)
                attr_len  =len(attr)
                if optype==1:
                    king_num = 0
                    for i in range(attr_len):
                        quality = attr[i][0]
                        __attr_type = attr_type_d.get(attr[i][1],1)
                        if quality==6:
                            king_num +=1
                        base_data['quality'] = quality
                        base_data['attr_type'] = __attr_type
                        base_data['nodeseat'] = i+1
                        base_data['d_date'] = d_date
                        base_data['year'] = year_week[0]
                        base_data['month'] = str(d_date)[4:6]
                        base_data['week'] = year_week[1]
                        ret =self.database.add('superstars',base_data)
                        __basedata = dict(quality  = quality, attr_type = __attr_type,nodeseat = i+1)
                        __basedata.update(_basedata)
                        self.database.add("superstardetail",__basedata)
                    # if ck_data:
                    #     ## 
                    #     update_d = dict(ak_num = ck_data.get('ak_num',0) +1,
                    #                                                   ak_attr_num = ck_data.get('ak_attr_num',0)+attr_len  
                    #         )
                    #     if king_num:
                    #         update_d['king_num'] = king_num + ck_data.get('king_num',0)
                    #     self.database.update('superstartoplayer',update_d,dict(id = ck_data.get('id',0)))
                    # else:
                    #     add_d = dict(s_uid = s_uid,
                    #                 uid = uid,
                    #                 pid = pid,
                    #                 ak_num =1,
                    #                 ak_attr_num = attr_len,
                    #                 pname = pname
                    #         )
                    #     if king_num:
                    #         add_d['king_num'] = king_num
                    #     self.database.add('superstartoplayer',add_d)

                    self.deal_reste(reset_d,dict(s_uid  = s_uid, d_date = d_date, ak_num = 1))
                    
                elif optype ==2:
                    __basedata = dict()
                    reset_num = 1
                    oneattr_reset = 0
                    twoattr_reste = 0
                    ldiamond = data.get('basediamond',0)
                    basediamond = ldiamond
                    if attr_len>=1:
                        ldiamond += attr[0][1]
                    if attr_len ==2:
                        ldiamond += attr[1][1]
                        twoattr_reste = 1
                    if attr_len==1:
                        oneattr_reset = 1
                    # if ck_data:
                    #     update_d = dict(reset_num = ck_data.get('reset_num',0) + reset_num)
                    #     if ldiamond:
                    #         update_d['ldiamond'] = ck_data.get('ldiamond',0) + ldiamond
                    #         update_d['basediamond'] = ck_data.get('basediamond',0) + basediamond
                    #     if oneattr_reset:
                    #         update_d['oneattr_reset'] = ck_data.get('oneattr_reset',0) + oneattr_reset
                    #         update_d['oneuserld'] = ck_data.get('oneuserld',0) + (ldiamond - basediamond)
                    #     if twoattr_reste:
                    #         update_d['twoattr_reste'] = ck_data.get('twoattr_reste',0) + twoattr_reste
                    #         update_d['twouser_ld'] = ck_data.get('twouser_ld',0) + (ldiamond - basediamond)
                    #     self.database.update('superstartoplayer',update_d,dict(id = ck_data.get('id',0)))
                    # else:
                    #     add_d = dict(s_uid = s_uid ,
                    #                 uid = uid,
                    #                 pid = pid,
                    #                 reset_num = reset_num,
                    #                 pname = pname
                    #         )
                    #     if ldiamond:
                    #         add_d['ldiamond'] = ldiamond
                    #         add_d['basediamond'] = basediamond
                    #     if oneattr_reset:
                    #         add_d['oneattr_reset'] = oneattr_reset
                    #         add_d['oneuserld'] = ldiamond - basediamond
                    #     if twoattr_reste:
                    #         add_d['twoattr_reste'] = twoattr_reste
                    #         add_d['twouser_ld'] = ldiamond - basediamond
                    #     self.database.add('superstartoplayer',add_d)
                    data_g = dict(s_uid = s_uid,
                                d_date = d_date,
                                reset_num = 1,
                                ldiamond = ldiamond
                        )
                    oneuserld = 0
                    twouser_ld = 0
                    diff = ldiamond - data.get('basediamond',0)
                    if oneattr_reset:
                        oneuserld = diff
                        data_g['oneuserld'] = oneuserld
                        data_g['oneattr_reset']  =1
                    elif twoattr_reste:
                        twouser_ld = diff
                        data_g['twouser_ld'] = twouser_ld
                        data_g['twoattr_reste'] = 1
                    self.deal_reste(reset_d,data_g)
                    __basedata['basediamond'] = basediamond
                    __basedata['lock_user_diamond'] = diff
                    __basedata['lock_type'] = attr_len
                    __basedata.update(_basedata)
                    self.database.add("superstardetail",__basedata)
                elif optype ==3:
                    ck_sup = dict(s_uid = str(s_uid),
                                    fid = fid,
                                    nodeid  = nodeid,
                                    pid = pid
                        )
                    ck_supers = self.database.select_all('superstars',ck_sup)
                    # mod_king_num = 0
                    for i in range(attr_len):
                        su_up_d = dict()
                        quality = 0
                        attr_type = 0
                        if ck_supers:
                            quality = ck_supers[i].get('quality',1)
                            attr_type = ck_supers[i].get('attr_type',1)
                        now_quality = attr[i][0]
                        mof_attr_type = attr_type_d.get(attr[i][1])
                        if quality !=now_quality:
                            su_up_d['quality'] = now_quality
                        if attr_type!=mof_attr_type:
                            su_up_d['attr_type'] = mof_attr_type
                        if su_up_d and ck_supers:
                            self.database.update('superstars',su_up_d,dict(id = ck_supers[i].get('id')))
                        elif su_up_d:
                            ck_sup['quality'] = now_quality
                            ck_sup['attr_type'] = mof_attr_type
                            ck_sup['fname'] = fname
                            ck_sup['d_date'] = d_date
                            ck_sup['year'] = year_week[0]
                            ck_sup['month'] = str(d_date)[4:6]
                            ck_sup['week'] = year_week[1]
                            ck_sup['uid'] = uid
                            self.database.add('superstars',ck_sup)
                        __basedata = dict(quality  = now_quality, attr_type = mof_attr_type,nodeseat   = i+1)
                        # _basedata['op_type'] =1
                        __basedata.update(_basedata)
                        self.database.add("superstardetail",__basedata)
                        # if quality==6 and now_quality!=6:
                        #     mod_king_num +=-1
                        # elif quality!=6 and now_quality==6:
                        #     mod_king_num +=1
                    # if mod_king_num:
                    #     self.database.update('superstartoplayer',dict(king_num = ck_data.get('king_num',0) + mod_king_num),dict(id = ck_data.get('id')))
                self.database.update('superstar',dict(deal_flag =1),dict(id = data.get('id',1)))
            for k,v in reset_d.items():
                s_uid_d_l = k.split('_')
                s_uid = s_uid_d_l[0]
                d_date = s_uid_d_l[1]
                select_d = self.database.select('superstardatagram',dict(s_uid = s_uid,d_date = d_date))
                update_d = dict()
                for _k,_v in v.items():
                    if _v:
                        update_d[_k] = _v
                if select_d:
                    for __k,__v in update_d.items():
                        update_d[__k] = update_d[__k] + select_d.get(__k,0)
                    self.database.update('superstardatagram',update_d,dict(id = select_d.get('id',0)))
                else:
                    update_d['s_uid'] =s_uid
                    update_d['d_date'] = d_date
                    update_d['year'] = year_week[0]
                    update_d['week'] = year_week[1]
                    update_d['month'] = str(d_date)[4:6]
                    self.database.add('superstardatagram',update_d)
            h_m =  now.strftime('%H%M')
            if h_m>='0030' and h_m <='0040' and self.start==0:
                self.start = 1
                self._deal = True
            if self._deal:
                self._superstardatagram()
                self._deal = False
            if h_m>'0050':
                self.start = 0
    @catch_except('superstars')
    def _superstardatagram(self):
        ## 00
        now = datetime.datetime.now()
        h_m =  now.strftime('%H%M')
        _day = now.strftime('%d')
        yestoday = now + datetime.timedelta(days = -1)
        now_month = now.month
        yes_month = yestoday.month
        y_week = now.isocalendar()
        week_n = y_week[2]
        d_date = int(yestoday.strftime('%Y%m%d'))
        yes_y_week = yestoday.isocalendar()
        d_datas = self.database.sts_superstars(['d_date'],[d_date])
        for d_data in d_datas:
            s_uid = d_data.pop('s_uid','')
            d_to_p = self.database.select('superstardatagram',dict(s_uid = str(s_uid),d_date =d_date))
            if d_to_p:
                self.database.update('superstardatagram',d_data,dict(id = d_to_p.get('id',0)))
            else:
                d_data['s_uid'] = s_uid
                d_data['d_date']  = d_date
                d_data['year'] = yes_y_week[0]
                d_data['week'] = yes_y_week[1]
                d_data['month'] = yes_month
                self.database.add('superstardatagram',d_data)
        if week_n ==1:
            _year = yes_y_week[0]
            last_week = yes_y_week[1]
            week_start_day = yestoday+datetime.timedelta(days = -6)
            last_m =  int(yestoday.strftime("%m"))
            last_d = int(yestoday.strftime("%d"))
            start_m = int(week_start_day.strftime("%m"))
            start_d = int(week_start_day.strftime("%d"))
            gram_datas = self.database.sts_superstardatagram(['year','week'],[_year,last_week])
            w_datas = self.database.sts_superstars(['year','week'],[_year,last_week])
            ret_d_w = dict()
            self.deal_w_m(ret_d_w,w_datas)
            self.deal_w_m(ret_d_w,gram_datas)

            check_last_week = int(str(_year) + str(last_week))
            for k,v in ret_d_w.items():
                w_d_w = self.database.select('superstarweek',dict(s_uid = str(k), year = _year, week = check_last_week))
                if w_d_w:
                    self.database.update('superstarweek',v,dict(id = w_d_w.get('id',0)))
                else:
                    v['s_uid'] = k
                    v['week'] = check_last_week
                    v['year'] = _year
                    v['week_date'] = str(start_m) + '.' + str(start_d) + '-' +str(last_m)+'.'+str(last_d)
                    self.database.add('superstarweek',v)
        if now_month != yes_month:
            _year = yestoday.year
            m_gram_datas = self.database.sts_superstardatagram(['year','month'],[_year,yes_month])
            m_datas = self.database.sts_superstars(['year','month'],[_year,yes_month])
            m_ret_d = dict()

            self.deal_w_m(m_ret_d, m_datas)
            self.deal_w_m(m_ret_d, m_gram_datas)
            last_month = int(str(_year) + str(yes_month))
            for k,v in m_ret_d.items():
                m_data_to_ym = self.database.select('superstarym',dict(s_uid = str(k), year = _year,month = last_month))
                if m_data_to_ym:
                    self.database.update('superstarym',v,dict(id = m_data_to_ym.get('id',0)))
                else:
                    v['s_uid'] = k
                    v['month'] = last_month
                    v['year'] = _year

                    self.database.add('superstarym',v)




    @catch_except('superstars')
    def deal_reste(self,reset_d,data):
        s_uid = data.pop('s_uid','')
        d_date = data.pop('d_date','')
        key = str(s_uid) +'_'+ str(d_date)
        get_data = reset_d.get(key)
        if get_data:
            for k,v in data.items():
                get_data[k] = get_data.get(k,0) + v
        else:
            reset_d[key] = data

    @catch_except('superstars')
    def deal_w_m(self,ret_data,data):
        for _data in data:
            s_uid = _data.pop('s_uid')
            if ret_data.get(s_uid):
                _ret_d = ret_data.get(s_uid)
                _ret_d.update(_data)
            else:
                ret_data[s_uid] = _data
