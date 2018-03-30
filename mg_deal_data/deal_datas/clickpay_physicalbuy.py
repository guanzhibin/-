#coding:utf8
import gevent
import random
from mg_config import mg_log
from public_class.base import catch_except
from mysql_data.sqlprocess import connDB
import time
import datetime


class physical_buy(object):
    @catch_except('physical_buy')
    def __init__(self):
        self.init = True  ## 预留参数
        self.database = connDB() ## 数据库实例
        self.activation = False

    @catch_except('physical_buy')
    def starting_deal(self):
        while 1:

            gevent.sleep(30)
            now = datetime.datetime.now()
            if now.strftime('%H%M%S') <='060000':
                _now = now - datetime.timedelta(days= -1)
                today = int(_now.strftime('%Y%m%d'))
                create_time = datetime.datetime(_now.year, _now.month, _now.day,23,23,23)
            else:
                today = int(time.strftime("%Y%m%d"))
                create_time = now
            physical_buy_data = self.database.physical_buy(dict(d_date=today))
            # today_vip_login_data = self.database.today_vip_login(dict(d_date=today))

            # 删除历史数据
            delete_history = self.database.delete_num_of_phy_pur(dict(d_date=today))

            # 购买体力次数字段的dict
            buy_times_dict = {1:'once_pay_num',2:'twice_pay_num',3:'three_pay_num',4:'four_pay_num',5:'five_pay_num',
            6:'six_pay_num',7:'seven_pay_num',8:'eight_pay_num',9:'nine_pay_num',10:'ten_pay_num',11:'eleven_pay_num',
            12:'twelve_pay_num',13:'thirt_pay_num',14:'fourt_pay_num',15:'fift_pay_num',16:'sixt_pay_num'}

            buy_dict = dict()

            # 取各条件下的体力购买情况

            for resp in physical_buy_data:
                resp_key = resp['vip']+resp['s_uid']+resp['ch']
                buy_times = resp['buy_times']
                resp[buy_times_dict[buy_times]] = resp['count']
                resp.pop('buy_times')
                resp.pop('count')

                if buy_dict.get(resp_key):
                    dict_resp = buy_dict.get(resp_key)
                    resp['total_vip_num'] = int(resp['total_vip_num']) + int(dict_resp['total_vip_num'])
                    dict_resp.update(resp)
                else:
                    buy_dict[resp_key] = resp

            # 取各条件下的当天登录人数
            # for resp in today_vip_login_data:
            #     resp['vip'] = str(resp.pop('vip',''))
            #     resp_key = resp['vip']+resp['s_uid']+resp['ch']

            #     if buy_dict.get(resp_key):
            #         dict_resp = buy_dict.get(resp_key)
            #         dict_resp['total_vip_num'] = resp['count']

            for d,v in buy_dict.items():
                v['channel_name'] = v.pop('ch','')
                v['vip_level'] ='VIP' + str(v.pop('vip',0))
                v['create_time'] = create_time
                self.database.add('num_of_phy_pur', v)                             
                
            '''
            for resp in physical_buy_data:

                if self.database.select('num_of_phy_pur', {'s_uid':resp['s_uid'], 
                    'channel_name':resp['ch'], 'vip_level':resp['vip'], 'd_date':resp['d_date']} ):
                    self.database.update('num_of_phy_pur',
                        {buy_times_dict[resp['buy_times']]:int(resp['count'])},
                        {'s_uid':resp['s_uid'], 'channel_name':resp['ch'], 'vip_level':resp['vip'], 'd_date':resp['d_date']})


                else:              
                    self.database.add('num_of_phy_pur', 
                            {'s_uid':resp['s_uid'],
                                'server_name':resp['server_name'], 
                                'channel_name':resp['ch'], 
                                'vip_level':resp['vip'], 
                                'd_date':resp['d_date'], 
                                buy_times_dict[resp['buy_times']]:int(resp['count'])
                            }
                        )
            
            for resp in today_vip_login_data:
                if self.database.select('num_of_phy_pur', 
                                        {'s_uid':resp['s_uid'], 
                                            'channel_name':resp['ch'], 
                                            'vip_level':resp['vip'], 
                                            'd_date':int(resp['d_date'])
                                        }):
                    self.database.update('num_of_phy_pur',
                        {'total_vip_num':int(resp['count'])},
                        {'s_uid':resp['s_uid'], 'channel_name':resp['ch'], 'vip_level':resp['vip'], 'd_date':resp['d_date']})
            '''

class click_pay:
    @catch_except('click_pay')
    def __init__(self):
        self.init = True  ## 预留参数
        self.database = connDB() ## 数据库实例
        self.activation = False

    @catch_except('click_pay')
    def starting_deal(self):
        while 1:
            gevent.sleep(10)
            today = int(time.strftime("%Y%m%d"))
            click_pay_data = self.database.today_click_pay(dict(d_date=today))

            for resp in click_pay_data:
                if self.database.select('pay_points',
                                        dict(s_uid=resp['s_uid'],
                                            ch=resp['ch'],
                                            d_date=int(resp['d_date']),
                                            pay_points=resp['name'])):
                    self.database.update('pay_points',
                                        dict(pay_num=resp['num'],amount_of_recharge=resp['money']),
                                        dict(s_uid=resp['s_uid'],
                                            ch=resp['ch'],
                                            d_date=int(resp['d_date']),
                                            pay_points=resp['name']))

                else:
                    self.database.add('pay_points',
                                        dict(s_uid=resp['s_uid'],
                                            ch=resp['ch'],
                                            d_date=int(resp['d_date']),
                                            pay_points=resp['name'],
                                            pay_num=int(resp['num']),
                                            amount_of_recharge=resp['money']
                                        )
                                    )

