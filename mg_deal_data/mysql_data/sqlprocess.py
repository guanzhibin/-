#coding:utf8
import mysql_base
from sql_deal import deal_sql
from mg_config import config
import redis
class connDB(deal_sql,object):
    def __init__(self):
        self.client = mysql_base.mysqlDB(config['m_ip'],config['m_port'],config['m_user'],config['m_pwd'],config['m_db'])
        if config.get("r_password"):
            pool = redis.ConnectionPool(host = config['r_ip'],port =config['r_port'],db=config['r_db'],password =config['r_password'])
        else:
            pool = redis.ConnectionPool(host = config['r_ip'],port =config['r_port'],db=config['r_db'])
        self.r = redis.Redis(connection_pool=pool)

    def select(self,table_name,condition):
        sql,values = self.select_sql(table_name,condition,limit=1)
        items = self.client.mselect(sql,values)
        if  items:
            return items[0]
        return dict()
    ## 获取mysql游标
    def mysqlcur(self):
        return self.client.base_connect()

    def select_all(self,table_name,condition, field = None, limit=None, in_condition =None):
        sql,values = self.select_sql(table_name,condition, field = field, limit=limit, in_condition = in_condition)
        return self.client.mselect(sql,values)

    def check_byFilter(self,table_name,condition):
        sql = '''select id, ch, s_uid , d_date, chmc, chsc, uid, time,ordermoney from atm where orderstate=5 and ltv_flag= %s  limit %s'''
        values = (condition.get('ltv_flag',"0"),condition.get('limit',1))
        return self.client.mselect(sql,values)
    def add(self,table_name,query):
        sql,values  =self.inert_sql(table_name,query)
        ret = self.client.madd(sql,values)
        return ret
    def add2(self,table_name,query):
        return self.inert_sql2(table_name,query)
    def new_add(self,conn,cur,table_name,query):
        sql,values  =self.inert_sql(table_name,query)
        ret = cur.execute(sql % values)
        last_id = 0
        if ret==1:
            last_id = int(cur.lastrowid)
        conn.commit()
        return last_id
    def addall(self,sql,values):
        self.client.malladd(sql, values)
    def delete(self, table_name,params):
        sql,values = self.delete_sql(table_name,params)
        return self.client.mselect(sql,values)

    def update(self,table_name,query,condition):
        sql,values = self.update_sql(table_name,query,condition)
        return self.client.mupdate(sql,values)
    def get_byFilter(self,table_name,params):
        text_array = []
        val_array = []
        if params.get('s_uid'):
            text_array.append("`s_uid` = %s")
            val_array.append(params.get('s_uid'))
        if params.get('uid'):
            text_array.append("`uid` = %s")
            val_array.append(params.get('uid'))
        if params.get('ch'):
            text_array.append("`ch` = %s")
            val_array.append(params.get('ch'))
        if params.get('chmc'):
            text_array.append("`chmc` = %s")
            val_array.append(params.get('chmc'))
        if params.get('chsc'):
            text_array.append("`chsc` = %s")
            val_array.append(params.get('chsc'))
        if params.get('start_time'):
            text_array.append("`d_date` >= %s")
            val_array.append(datetime.datetime.strptime(params.get('start_time'), "%Y-%m-%d %H:%M:%S"))
        if params.get('end_time'):
            text_array.append("`d_date` <= %s")
            val_array.append(datetime.datetime.strptime(params.get('end_time'), "%Y-%m-%d %H:%M:%S"))
        
        sql = 'select * from %s where %s' % (table_name,' and '.join(text_array))

        items = self.client.mselect(sql,tuple(val_array))
        if items:
            return items[0]
        return dict()

    
    def StatisticsLoginByDay(self,d_date):
        sql  = "SELECT count(distinct uid,ch,s_uid) as login_account,d_date,s_uid,ch FROM player_login where d_date = %s group by s_uid,ch"
        return self.client.mselect(sql,(d_date))

    ## 查询当天注册的玩家
    def player_regist(self,d_date):
        sql ='''SELECT ch,chmc, uid,s_uid FROM player_regist where d_date = %s group by ch,s_uid,uid '''
        return self.client.mselect(sql,(d_date))


    ### 查询当天玩家登录
    def player_login(self, d_date):
        sql = ''' SELECT ch, s_uid, uid FROM player_login where d_date = %s group by ch,s_uid,uid '''
        return self.client.mselect(sql,(d_date))

    ### 
    def StatisticsNewLoginByDay(self,d_date):
        sql  = '''select count(distinct pr.uid) as new_login_accont,pr.ch,pr.s_uid,pr.d_date from
            (select reg.uid, reg.ch, reg.s_uid,reg.d_date from player_regist as reg where reg.d_date=%s group by reg.uid,reg.ch,reg.s_uid ) as pr
            join (select log.uid, log.ch, log.s_uid from player_login as log where log.d_date=%s
            group by log.uid,log.ch,log.s_uid) as pl on pr.uid = pl.uid and pr.ch=pl.ch and pr.s_uid=pl.s_uid group by pr.ch,pr.s_uid'''
        return self.client.mselect(sql,(d_date, d_date))

    ## 统计某天新创建的角色数
    def StatisticsNewRole(self,d_date):
        sql = '''
        SELECT ch as channel_name, s_uid,count(distinct pid) as create_role_accont FROM player_create where d_date = %s group by ch,s_uid

        '''
        return self.client.mselect(sql,(d_date))



    ## 统计某天新登角色数
    def NewLoginRole(self,d_date):

        sql = '''
            select count(distinct pc.pid) as new_login_accont,pc.ch  as channel_name,pc.s_uid,pc.d_date from
            (select pc.pid, pc.ch, pc.s_uid,pc.d_date from player_create as pc where pc.d_date=%s group by pc.pid,pc.ch,pc.s_uid ) as pc
            join (select log.pid, log.ch, log.s_uid from player_login as log where log.d_date=%s 
            group by log.uid,log.ch,log.s_uid) as pl on pc.pid = pl.pid and pc.ch=pl.ch and pc.s_uid=pl.s_uid group by pc.ch,pc.s_uid

        '''
        return self.client.mselect(sql,(d_date, d_date))


    def RoleLoginAccount(self, d_date):
        sql = '''
        SELECT count(distinct pid)as role_login_accont,ch as channel_name,s_uid,d_date FROM player_login where d_date = %s group by ch,s_uid;

        '''
        return self.client.mselect(sql,(d_date))
    def payAccountNum(self,d_date):
        sql  = '''select count(1) as pay_account_num,a.s_uid, a.ch, a.d_date from 
        (SELECT distinct uid,d_date,s_uid,ch FROM atm where orderstate=5 and d_date = %s) as a 
        group by a.ch, a.s_uid'''
        return self.client.mselect(sql,(d_date))

    def IncomeByDay(self,d_date):
        sql = '''SELECT sum(ordermoney) as income,s_uid,ch,d_date,count(1)as atm_num FROM atm where orderstate=5 and d_date=%s group by s_uid,ch'''
        return self.client.mselect(sql,(d_date))

    ## 新增付费账号数
    def newPay_count(self,data):
        sql  = '''select count(1) as first_pay_account,sum(ordermoney) as first_pay_account_income, s_uid,ch, 
        d_date from atm where orderstate=5 and d_date = %s and new_pay_flag = %s group by ch, s_uid'''
        return self.client.mselect(sql,(data.get('d_date'),data.get('new_pay_flag')))

    ## 新注册且付费的账号数
    def newRegPay(self,data):
        sql = '''select count(1) as new_login_pay_num,atm.ch,atm.s_uid, atm.d_date from 
        (SELECT distinct pid,s_uid,ch,d_date FROM atm where orderstate=5 and d_date = %s and new_reg_flag = %s) as atm
        group by atm.ch,atm.s_uid'''

        # sql = '''
        # select count(distinct b.uid) as new_login_pay_num,b.ch,b.s_uid, b.d_date from (select * from player_regist where d_date = %s) as a right join 
        # ( SELECT distinct uid,s_uid,ch,d_date FROM atm where orderstate=5 and d_date = %s and new_pay_flag = 1) as b on a.uid = b.uid and a.s_uid = b.s_uid and a.ch = b.ch group by b.s_uid,b.ch

        # '''
        return self.client.mselect(sql,(data.get('d_date'),data.get('new_reg_flag')))

    def check_by_message(self,data):
        # sql = '''select sum(ordermoney) as new_login_pay_income,ch,s_uid,chmc,chsc, d_date from atm where orderstate=5 and
        # ch = %s and d_date= %s and chmc = %s and chsc = %s and uid = %s and pid=%s group by ch ,s_uid'''
        sql = '''
        select sum(b.ordermoney) as new_login_pay_income,b.ch,b.s_uid,b.d_date from (SELECT * FROM atm where d_date = %s and  new_reg_flag=1 and orderstate = 5)a left join 
        (select * from atm where orderstate=5 and d_date= %s )b on a.pid=b.pid group by b.ch,b.s_uid;
        '''
        return self.client.mselect(sql,(data.get('d_date',0),data.get('d_date',0)))

    def player_retain(self,data):
        sql = '''select count(distinct pr.uid) as count,pr.ch,pr.s_uid,pr.d_date from
                (select reg.uid, reg.ch, reg.s_uid,reg.d_date from player_regist as reg where reg.d_date=%s group by reg.uid,reg.ch,reg.s_uid ) as pr
                join (select log.uid, log.ch, log.s_uid from player_login as log where log.d_date=%s group by log.uid,log.ch,log.s_uid) as pl on 
                pr.uid = pl.uid and pr.ch=pl.ch and pr.s_uid=pl.s_uid group by pr.ch,pr.s_uid'''
        return self.client.mselect(sql,(data.get('reg_ymd'),data.get('ymd')))

    def _retain_by_server(self,data):
        sql = '''select count(distinct pr.uid) as count,pr.ch,pr.s_uid,pr.d_date from
                (select reg.uid, reg.ch, reg.s_uid,reg.d_date from player_regist as reg where reg.d_date=%s group by reg.uid,reg.ch,reg.s_uid ) as pr
                join (select log.uid, log.ch, log.s_uid from player_login as log where log.d_date=%s group by log.uid,log.ch,log.s_uid) as pl on 
                pr.uid = pl.uid and pr.ch=pl.ch and pr.s_uid=pl.s_uid group by pr.s_uid'''
        return self.client.mselect(sql,(data.get('reg_ymd'),data.get('ymd')))

    ## 新增加的注册账号数
    def new_regist_account(self,d_date):
        sql = '''SELECT count(distinct uid) regist_account, ch as channel_name, s_uid 
        FROM player_regist where d_date= %s group by ch,s_uid;'''

        return self.client.mselect(sql,(d_date))

    ## 设备留存 
    def EquipmentRetain(self,data):

        sql = '''
        select count(distinct tt.uuid) as count, tt.ch as channel_name, tt.d_date from player_login as pl 
        right join (SELECT * FROM  fetch_server where new_flag = 1 and  d_date = %s) as tt on pl.ch = tt.ch 
        and pl.uuid = tt.uuid where pl.d_date = %s group by tt.ch;
        '''
        return self.client.mselect(sql, (data.get('new_fetch_time'),data.get('ymd')))

    # 当日体力购买情况
    def physical_buy(self,data):
        sql = '''select count(distinct a.pid) total_vip_num,b.vip,c.name as server_name, a.s_uid,a.ch,a.d_date,a.ct as buy_times,count(1) as count from 
        (select pid,s_uid,ch,d_date,sum(count) as ct from physical_buy where d_date = %s  group by pid,s_uid,d_date, ch) a
        inner join (select pid,max(vip) as vip from physical_buy where d_date = %s group by pid) b on a.pid = b.pid
        inner join servers_data c on c.uid = a.s_uid
        group by b.vip,a.s_uid,a.ch,a.d_date,a.ct'''
        return self.client.mselect(sql,(data.get('d_date'),data.get('d_date')))

    # 当日VIP登录
    def today_vip_login(self,data):
        sql = '''select distinct vip,d_date,ch, s_uid, count(1) as count from player_login 
        where d_date = %s group by s_uid, ch, d_date, vip '''
        return self.client.mselect(sql,(data.get('d_date')))

    def today_click_pay(self,data):
        sql = '''select name,s_uid,ch,d_date,sum(count) as num, sum(money) as money
        from click_pay where d_date = %s group by ch, s_uid, d_date, name'''
        return self.client.mselect(sql,(data.get('d_date')))


    def delete_num_of_phy_pur(self,data):
        sql = ' delete from num_of_phy_pur where d_date = %s '
        return self.client.mselect(sql,(data.get('d_date')))



    ### 统计设备登录数
    def LoginEquipment(self, d_date):
        sql = '''SELECT ch as channel_name,count(distinct uuid) as equipment_login_accont FROM player_login where d_date = %s group by ch;'''
        return self.client.mselect(sql, (d_date))

    ##统计新增设备并注册了账号的设备数
    def deal_equipment_num(self,data):
        sql = '''select fs.ch as channel_name , count(distinct pr.uuid) as valid_e_num from fetch_server as fs left join 
            (select * from player_regist where d_date = %s group by uuid) as pr on fs.uuid = pr.uuid where fs.d_date=%s and fs.new_flag=1 group by fs.ch'''
        return self.client.mselect(sql,(data.get('d_date'), data.get('d_date')))
    ## 
    def deal_daily_np(self,data):
        sql = '''SELECT sum(new_login_accont) as new_login_accont, sum(login_account)as login_account 
                , sum(pay_account_num) as pay_account_num, sum(income) as income,sum(first_pay_account) as first_pay_account
                , sum(first_pay_account_income) as first_pay_account_income, sum(new_login_pay_num) as new_login_pay_num
                , sum(new_login_pay_income) as new_login_pay_income, sum(one_retain_days) as one_retain_days,
                sum(three_retain_days) as three_retain_days, sum(seven_retain_days) as seven_retain_days,
                sum(average_number_online) as average_number_online,sum(highest_online) as highest_online,
                channel_name,sum(atm_num) as atm_num  FROM mg_daily_newspaper where d_date = %s group by channel_name
                '''
        return self.client.mselect(sql, (data.get('d_date')))
    ## 统计新增设备数按渠道
    def deal_fetch_s(self,data):
        sql  ='''select ch as channel_name,count(distinct uuid) as new_equipment from  fetch_server where d_date = %s and new_flag=1 group by ch'''
        return self.client.mselect(sql, (data.get('d_date')))

    ## 新增加的设备并且有登录游戏的设备数
    def new_equip_login_d(self,data):
        sql =''' select count(distinct fserver.uuid) as new_equip_login,fserver.ch as channel_name from
                (select fs.uuid, fs.ch,fs.d_date from fetch_server as fs where fs.new_flag=1 and fs.d_date=%s group by fs.uuid,fs.ch ) as fserver
                join (select log.uuid, log.ch from player_login as log where log.d_date=%s group by log.uuid,log.ch) as pl on 
                fserver.uuid = pl.uuid and fserver.ch=pl.ch group by fserver.ch;'''
        return self.client.mselect(sql,(data.get('d_date'), data.get('d_date')))


    ## 启动游戏的设备数
    def start_equip_data(self,data):
        sql = '''select ch as channel_name , count(distinct uuid) as start_equip from fetch_server where d_date = %s group by ch'''
        return self.client.mselect(sql, (data.get('d_date')))

    ## 新启动并且登录游戏的设备数
    def NewSatrtLogin(self, d_date):
        sql = '''

         select count(distinct fserver.uuid) as new_start_login_accont,fserver.ch as channel_name from
        (select fs.uuid, fs.ch,fs.d_date from fetch_server as fs where fs.new_flag=1 and fs.d_date=%s group by fs.uuid,fs.ch ) as fserver
        join (select log.uuid, log.ch from player_login as log where log.d_date=%s group by log.uuid,log.ch) as pl on 
        fserver.uuid = pl.uuid and fserver.ch=pl.ch group by fserver.ch;

        '''

        return self.client.mselect(sql , (d_date, d_date))

    ## 统计新增设备数按渠道按小时取
    def deal_fetch_s_by_hour(self,data):
        sql  ='''select ch as channel_name,count(distinct uuid) as new_equip,s_uid from 
        (select * from fetch_server where fetch_time between %s and %s)a group by ch,s_uid'''
        return self.client.mselect(sql, (data.get('start_time'),data.get('end_time')))
    ## 每小时实时数据新注册账号数
    def deal_equipment_by_hour(self,data):
        sql = '''select ch as channel_name, count(distinct uuid) as new_account,s_uid from (select * from player_regist where reg_time between %s and %s )a group by ch,s_uid'''
        return self.client.mselect(sql,(data.get('start_time'),data.get('end_time')))

    ## 统计注册并且登录的玩家数
    def s_regist_login(self,data):
        sql  = '''select count(distinct pr.uid) as new_account,pr.ch as channel_name,pr.s_uid from
            (select reg.uid, reg.ch, reg.s_uid,reg.d_date from player_regist as reg where reg.reg_time between %s and %s group by reg.uid,reg.ch,reg.s_uid ) as pr
            join (select log.uid, log.ch, log.s_uid from player_login as log where log.login_time between %s and %s group by log.uid,log.ch,log.s_uid) as pl 
            on pr.uid = pl.uid and pr.ch=pl.ch and pr.s_uid=pl.s_uid group by pr.ch,pr.s_uid;'''
        return self.client.mselect(sql, (data.get('start_time'),data.get('end_time'),data.get('start_time'),data.get('end_time')))
    ## 每小时实时数据统计活跃人数
    def s_acount_t(self,data):
        sql = '''SELECT  count(distinct uid) as actoin, ch as channel_name, s_uid FROM player_login where login_time between %s and  %s group by s_uid, ch'''
        return self.client.mselect(sql, (data.get('start_time'),data.get('end_time')))

    def pay_income(self,data):
        sql = '''SELECT sum(ordermoney)pay_income,count(distinct(uid))pay_account,ch channel_name,s_uid 
        FROM atm where orderstate=5 and time between %s and  %s group by ch, s_uid'''
        return self.client.mselect(sql, (data.get('start_time'),data.get('end_time')))
    ## 活跃用户
    def action_player(self,data,some_d_ago):
        start_ago = data.get('start_ago')
        end_ago = data.get('end_ago')
        if start_ago and end_ago:
            sql = ''' select ch as channel_name, s_uid, count(1) as %s from (select distinct uid,ch,s_uid from player_login where d_date between %s and %s)a group by s_uid, ch '''
            return self.client.mselect(sql, (some_d_ago, start_ago, end_ago))
        else:
            sql = ''' select ch as channel_name, s_uid, count(1) as %s from (select distinct uid,ch,s_uid from player_login where d_date=%s)a group by s_uid, ch '''
            return self.client.mselect(sql, (some_d_ago, start_ago))

    def select_currency(self,data=[]):

        sql = '''SELECT ch, s_uid, sum(diff) as %s FROM %s where status_flag=%s and d_date = %s group by s_uid,ch'''
        sql = sql % tuple(data)
        return self.client.mselect(sql, tuple())


    ## 产出消耗分布
    def outputdrain_distributoin(self,data=[]):
        sql ='''select type,sum(diff) as diff ,count(distinct pid) as p_num,count(*) as num,ch as channel_name,s_uid,status_flag,
        time as create_time from %s where status_flag = %s and d_date = %s group by s_uid,ch,type'''
        sql =  sql % tuple(data)
        return self.client.mselect(sql,tuple())



    ## ltv价值
    def ltv(self,data):
        sql = '''select sum(atm.ordermoney) as ordermoney, atm.ch, atm.s_uid from 
        player_regist as pr left join atm on pr.ch = atm.ch and pr.s_uid=atm.s_uid 
        where pr.d_date = %s and orderstate=5 and atm.d_date 
        between %s and %s group by atm.ch, atm.s_uid'''

        return self.client.mselect(sql, (data.get('_ymd'), data.get('_ymd'), data.get('ymd')))

    def diamdon(self,data =[]):
        sql ='''SELECT sum(cornobtain) as %s,ch, s_uid FROM atm where d_date = %s and orderstate=%s group by ch, s_uid'''
        return self.client.mselect(sql, tuple(data))

    ## 古神遗迹和英雄试炼
    def relic_hero(self,d_date):
        sql = '''SELECT count(DISTINCT pid_time)challenge_num, count(distinct pid)player_num, name, type, b_type,s_uid, ch FROM 
        relic_hero where d_date = %s group by s_uid, ch, name'''

        return self.client.mselect(sql, (d_date,))
    def suss_(self,d_date):
        sql = '''SELECT count(DISTINCT pid_time)success, name, type, b_type, avg(finish_time)avg_time,s_uid, ch FROM 
        (SELECT * FROM relic_hero where d_date = %s and s_flag=0 group by pid_time)a  group by s_uid, ch, type, name'''

        return self.client.mselect(sql, (d_date,))
    ## 
    def relic_hero_room_info(self,d_date):
        sql = '''SELECT count(1)count, name, b_type,s_uid, ch,s_flag FROM 
        room_info where d_date = %s group by s_uid, ch, name, s_flag '''

        return self.client.mselect(sql, (d_date,))
    ## spirit_level
    def spirit_level(self,d_date):
        sql = '''SELECT d_date, s_uid, ch as channel_name, name, level,count(distinct pid)num_owner FROM player_own 
        where d_date = %s and type=1 group by ch, s_uid, type, name, level'''
        return self.client.mselect(sql, (d_date,))

    ## 伙伴
    def pa_level(self,d_date):
        sql = '''SELECT d_date,s_uid, ch, name as partner_name, level as device_level, count(distinct pid)own_num,chmc,chsc 
        FROM player_own where type =2 and d_date = %s group by ch, s_uid , name, level'''
        return self.client.mselect(sql, (d_date,))

    ## 魔石
    def mg_stone(self,d_date):
        sql = '''select d_date,s_uid, ch,name as magic_stone_name, sum(level)magic_stone_num,chmc, chsc 
        from player_own where type =3 and d_date = %s group by ch, s_uid, name'''
        return self.client.mselect(sql, (d_date,))


    ### 冒险团
    def  onlineadgroup(self,yes_date, now_date):
        pass
        sql ='''SELECT 
        COUNT(DISTINCT guild_id) ad_num,
        s_uid,
        ch as channel_name,
        SUM(guild_member_num) ad_player_num,
        SUM(diamond_donate) diam_donation,
        SUM(prestige) residual_con,
        SUM(camp_wood_num) logging_camp,
        SUM(camp_stone_num) mine_num,
        SUM(open_boss) open_boss,
        SUM(kill_boss) kill_boss,
        SUM(money) surplus_funds,
        SUM(wood) surplus_woods,
        SUM(stone) surplus_stones,
        SUM(marnastone) sur_ori_stone
        FROM
            (SELECT * FROM guild_summary where time between %s and  %s and disband_flag = 0 group by guild_id order by time desc)a
        GROUP BY s_uid , ch;'''
        return self.client.mselect(sql, (yes_date,now_date))


    ## 解散的冒险团
    def ad_dismiss(self,yes_date, now_date):
        sql ='''SELECT count(distinct guild_id)ad_dis_num, s_uid, ch as channel_name FROM guild_summary where time between %s and  %s and disband_flag = 1 group by s_uid, ch'''
        return self.client.mselect(sql, (yes_date,now_date))

    def ad_level_dis(self, yes_date, now_date, field):
        sql = '''SELECT s_uid, ch as channel_name, count(distinct guild_id)num, %s as level FROM guild_summary ''' % field
        whereString = '''where time between %s and  %s and disband_flag = 0 group by s_uid, ch, ''' + field
        sql +=whereString
        return self.client.mselect(sql, (yes_date,now_date))

    ### 找出ltv表中3-90天内的数据
    def three_ninety(self,three_day, ninety):
        sql = '''SELECT * FROM ltv_value where d_date between %s and %s '''
        return self.client.mselect(sql,(ninety, three_day))

    def deal_servers(self,d_date):
        sql = '''
            SELECT s_uid,create_time,d_date,sum(new_login_accont) as new_login_accont,sum(login_account) as login_account,sum(pay_account_num)as pay_account_num
            ,sum(income)income,sum(first_pay_account)first_pay_account,sum(first_pay_account_income)first_pay_account_income,sum(new_login_pay_num)new_login_pay_num
            ,sum(new_login_pay_income) as new_login_pay_income,sum(atm_num) as atm_num   FROM mg_daily_newspaper where d_date = %s  group by s_uid;
        '''
        return self.client.mselect(sql,(d_date))

    def medals(self,d_date):

        sql = '''
        select count(distinct pid)count,ch,s_uid,event_id,name,medal,d_date
        from (
         SELECT uid,pid,ch,s_uid,event_id,max(medal) medal,name,d_date FROM cstory_adventure where  d_date = %s
        group by ch,s_uid,event_id, pid)a group by ch,s_uid,event_id,medal

        '''
        return self.client.mselect(sql, (d_date))

    def onece_medals(self,d_date):

        sql = '''
        select count(distinct pid)count,ch,s_uid,event_id,name,medal,d_date,onece
        from (
         SELECT uid,pid,ch,s_uid,event_id,max(medal) medal,name,d_date,onece FROM cstory_adventure where onece = 1 and d_date = %s
        group by ch,s_uid,event_id, pid)a group by ch,s_uid,event_id,medal

        '''
        return self.client.mselect(sql, (d_date))


    def player_lv(self,d_date):
        sql = '''
        select count(distinct pid)count,ch,s_uid,event_id,name,player_lv,d_date
        from (
        SELECT uid,pid,ch,s_uid,event_id,player_lv,name,d_date FROM cstory_adventure where onece =1 and d_date = %s 
        group by ch,s_uid,event_id, pid)a group by ch,s_uid,event_id,player_lv

        '''
        return self.client.mselect(sql,(d_date))

    def sweepnum(self,d_date):
        sql = '''
        SELECT ch,s_uid,d_date,sum(sweepnum)sweepnum,event_id FROM cstory_adventure_clean_up where d_date = %s group by ch,s_uid,event_id

        '''
        return self.client.mselect(sql,(d_date))

    def sts_superstars(self,key,value):
        sql = '''
            SELECT count(distinct pid)st_p_num,s_uid,count(distinct pid,fid,nodeid)ak_num FROM superstars 
            %s group by s_uid

        '''
        where = ' where '
        now_list = []
        for _key in key:
            now_list.append(_key+'=%s')
        where+=' and '.join(now_list)
        sql = sql % where
        return self.client.mselect(sql,tuple(value))

    def sts_superstardatagram(self,key,value):
        sql = '''
            SELECT s_uid,sum(reset_num)reset_num,sum(ldiamond)ldiamond,sum(oneattr_reset)oneattr_reset,
            sum(oneuserld)oneuserld,sum(twoattr_reste)twoattr_reste
            ,sum(twouser_ld)twouser_ld FROM superstardatagram  %s group by s_uid;

        '''
        where = ' where '
        now_list  = []
        for _key in key:
            now_list.append(_key+'=%s')
        where+=' and '.join(now_list)
        sql = sql % where
        return self.client.mselect(sql,tuple(value))