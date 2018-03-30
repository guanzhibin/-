#coding:utf-8
from mysql_data.sqlprocess import connDB

## 活跃用户表
def action_player():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'action_player' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `action_player` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `server_name` varchar(30) NOT NULL DEFAULT '' COMMENT '服务器名称',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器ID',
			  `channel_name` varchar(30) NOT NULL DEFAULT '' COMMENT '渠道名称',
			  `td_ac_num` int(11) NOT NULL DEFAULT '0' COMMENT '当天登录游戏的去重玩家数',
			  `th_ac_num` int(11) NOT NULL DEFAULT '0' COMMENT '当天以及倒推三天内的登录游戏的去重玩家数',
			  `w_ac_num` int(11) NOT NULL DEFAULT '0' COMMENT '当天倒推一周内登录游戏的去重玩家数',
			  `mth_ac_num` int(11) NOT NULL DEFAULT '0' COMMENT '当天倒推30天登录游戏的去重玩家数',
			  `dw_ac` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '日活跃除以周活跃',
			  `dm_ac` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '日活跃除以周活跃',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'action_player table %s' % msg

def ad_datas():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'ad_datas' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `ad_datas` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
				  `server_name` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器名称',
				  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `ad_num` int(11) NOT NULL DEFAULT '0' COMMENT '冒险团数量',
				  `ad_dis_num` int(11) NOT NULL DEFAULT '0' COMMENT '解散的冒险团数量',
				  `ad_player_num` int(11) NOT NULL DEFAULT '0' COMMENT '在团人数',
				  `residual_con` bigint(20) NOT NULL DEFAULT '0' COMMENT '剩余贡献',
				  `diam_donation` int(11) NOT NULL DEFAULT '0' COMMENT '钻石捐献次数',
				  `logging_camp` bigint(20) NOT NULL DEFAULT '0' COMMENT '伐木场总次数',
				  `mine_num` bigint(20) NOT NULL DEFAULT '0' COMMENT '矿场总次数',
				  `open_boss` int(11) NOT NULL DEFAULT '0' COMMENT 'boss开启总次数',
				  `kill_boss` int(11) NOT NULL DEFAULT '0' COMMENT '成功击杀boss总次数',
				  `surplus_funds` bigint(20) NOT NULL DEFAULT '0' COMMENT '所有冒险团剩余资金总量',
				  `surplus_woods` bigint(20) NOT NULL DEFAULT '0' COMMENT '所有冒险团剩余木材总量',
				  `surplus_stones` bigint(20) NOT NULL DEFAULT '0' COMMENT '所有冒险团剩余石头总量',
				  `sur_ori_stone` bigint(20) NOT NULL DEFAULT '0' COMMENT '所有冒险团剩余原石总量',
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'ad_datass table %s' % msg

def ad_group():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'ad_group' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `ad_group` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
			  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据发送的时间',
			  `chmc` varchar(45) NOT NULL DEFAULT '' COMMENT '主渠道号',
			  `chsc` varchar(45) NOT NULL DEFAULT '' COMMENT '子渠道号',
			  `ad_id` varchar(45) NOT NULL DEFAULT '' COMMENT '冒险团id',
			  `flag` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0冒险团健在，1冒险团已被解散',
			  `ad_player_num` int(11) NOT NULL DEFAULT '0' COMMENT '在团玩家数',
			  `diam_donation` int(11) NOT NULL DEFAULT '0' COMMENT '钻石捐献次数',
			  `residual_con` bigint(20) NOT NULL DEFAULT '0' COMMENT '剩余总贡献',
			  `logging_camp` int(11) NOT NULL DEFAULT '0' COMMENT '伐木场总次数',
			  `mine_num` int(11) NOT NULL DEFAULT '0' COMMENT '矿场总次数',
			  `open_boss` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'boss开启次数',
			  `kill_boss` tinyint(4) NOT NULL DEFAULT '0' COMMENT '击杀boss次数',
			  `surplus_funds` bigint(20) NOT NULL DEFAULT '0' COMMENT '冒险团剩余资金总量',
			  `surplus_woods` bigint(20) NOT NULL DEFAULT '0' COMMENT '冒险团剩余木材的总量',
			  `surplus_stones` bigint(20) NOT NULL DEFAULT '0' COMMENT '冒险团剩余石头的总量',
			  `sur_ori_stones` bigint(20) NOT NULL DEFAULT '0' COMMENT '冒险团剩余原石的总量',
			  `ad_level` tinyint(4) NOT NULL DEFAULT '0' COMMENT '冒险团等级',
			  `ad_shop` tinyint(4) NOT NULL DEFAULT '0' COMMENT '冒险团商店等级',
			  `m_room` tinyint(4) NOT NULL DEFAULT '0' COMMENT '冒险团冥想室等级',
			  `storehouse` tinyint(4) NOT NULL DEFAULT '0' COMMENT '冒险团仓库等级',
			  `frequency` tinyint(4) NOT NULL DEFAULT '0' COMMENT '冒险团繁荣度等级',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'ad_group table %s' % msg


def ad_level():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'ad_level' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `ad_level` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
				  `server_name` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器名称',
				  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
				  `type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '数据类型',
				  `level` tinyint(4) NOT NULL DEFAULT '0' COMMENT '数据类型所处的等级',
				  `num` int(11) NOT NULL DEFAULT '0' COMMENT '有多少个处于该等级的',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'ad_level table %s' % msg

def atm():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'atm' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `atm` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `uuid` varchar(50) NOT NULL DEFAULT '' COMMENT '设备ID',
				  `uid` bigint(20) NOT NULL DEFAULT '0' COMMENT '账号ID',
				  `pid` bigint(20) NOT NULL DEFAULT '0' COMMENT '角色ID',
				  `account` varchar(50) NOT NULL DEFAULT '' COMMENT '账号',
				  `player` varchar(30) NOT NULL DEFAULT '' COMMENT '玩家昵称',
				  `ip` varchar(30) NOT NULL DEFAULT '' COMMENT '消费时IP',
				  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
				  `chmc` varchar(50) NOT NULL DEFAULT '' COMMENT '主渠道号',
				  `chsc` varchar(50) NOT NULL DEFAULT '' COMMENT '子渠道号',
				  `os` tinyint(4) NOT NULL DEFAULT '0' COMMENT '平台',
				  `a_ct` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '玩家创建此记录的时间',
				  `mainchannel` varchar(45) NOT NULL DEFAULT '' COMMENT '主渠道',
				  `gameorderid` varchar(50) NOT NULL DEFAULT '' COMMENT '游戏识别订单号',
				  `channelorderid` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道订单号',
				  `ordermoney` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '订单金额',
				  `rechargemoney` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '游戏充值金额',
				  `rechargelevel` int(6) NOT NULL DEFAULT '0' COMMENT '玩家所处充值等级',
				  `rechargestate` tinyint(4) NOT NULL DEFAULT '0' COMMENT '充值状态',
				  `orderstate` tinyint(4) NOT NULL DEFAULT '0' COMMENT '订单状态',
				  `cornown` int(10) NOT NULL DEFAULT '0' COMMENT '充值后获得虚拟币总额',
				  `cornused` int(10) NOT NULL DEFAULT '0' COMMENT '玩家已经使用的虚拟币的总额',
				  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '充值时间',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `new_pay_flag` tinyint(3) NOT NULL DEFAULT '0' COMMENT '判断是否是新增付费玩家的标志',
				  `new_reg_flag` tinyint(3) NOT NULL DEFAULT '0',
				  `ltv_flag` tinyint(3) NOT NULL DEFAULT '0',
				  `cornobtain` int(11) NOT NULL DEFAULT '0',
				  `unique_id` varchar(60) DEFAULT '',
				  PRIMARY KEY (`id`),
				  KEY `orderstate_date_ch_s_uid` (`orderstate`,`d_date`,`ch`,`s_uid`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	if not cur.execute('Describe `atm` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `atm` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `cornobtain`;
		'''
		cur.execute(add_guild_id_sql)
	if not cur.execute(checkIndex('atm','orderstate_date_ch_s_uid')):
		add_orderstate_date_ch_s_uid = '''ALTER TABLE `atm` 
				ADD INDEX `orderstate_date_ch_s_uid` (`orderstate` ASC, `d_date` ASC, `ch` ASC, `s_uid` ASC)

		'''
		cur.execute(add_orderstate_date_ch_s_uid)
	ret = cur.execute('desc atm')
	for i in cur.fetchall():
		if i.get('Field','')=='ordermoney':
			Type = i.get('Type')
			if  (Type=='decimal(12,2)')==False:
				m_sql = '''
				ALTER TABLE `atm` 
					CHANGE COLUMN `ordermoney` `ordermoney` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '订单金额' ;
				'''
				cur.execute(m_sql)
		elif i.get('Field','')=='rechargemoney':
			Type = i.get('Type')
			if  (Type=='decimal(12,2)')==False:
				m_sql = '''
				ALTER TABLE `atm` 
					CHANGE COLUMN `rechargemoney` `rechargemoney` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '游戏充值金额' ;
				'''
				cur.execute(m_sql)


	conn.commit()
	conn.close()
	cur.close()
	print 'atm table %s' % msg

def channel_server():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'channel_server' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `channel_server` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `ch_id` int(10) NOT NULL DEFAULT '0' COMMENT '父级id',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器s_uid',
				  PRIMARY KEY (`id`),
				  UNIQUE KEY `ch_id_s_uid` (`ch_id`,`s_uid`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'channel_server table %s' % msg
def channels():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'channels' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `channels` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
			  PRIMARY KEY (`id`),
			  UNIQUE KEY `name` (`name`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'channels table %s' % msg

def click_pay():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'click_pay' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `click_pay` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `pid` bigint(20) NOT NULL DEFAULT '0' COMMENT '玩家ID',
			  `name` varchar(50) NOT NULL DEFAULT '' COMMENT '付费点名称',
			  `money` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '付费金额',
			  `count` int(6) NOT NULL DEFAULT '0' COMMENT '付费次数',
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '付费时间',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc` varchar(50) NOT NULL DEFAULT '' COMMENT '主渠道号',
			  `chsc` varchar(50) NOT NULL DEFAULT '' COMMENT '子渠道号',
			  `deal_flag` bit(1) NOT NULL DEFAULT b'0',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `date_ch_suid_name` (`d_date`,`ch`,`s_uid`,`name`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `click_pay` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `click_pay` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `deal_flag`;
		'''
		cur.execute(add_guild_id_sql)
	if not cur.execute(checkIndex('click_pay','date_ch_suid_name')):
		add_date_ch_suid_name_index = '''
			ALTER TABLE `click_pay` 
			ADD INDEX `date_ch_suid_name` (`d_date` ASC, `ch` ASC, `s_uid` ASC, `name` ASC)

		'''
		cur.execute(add_date_ch_suid_name_index)
	ret = cur.execute('desc click_pay')
	for i in cur.fetchall():
		if i.get('Field','')=='money':
			Type = i.get('Type')
			if  (Type=='decimal(12,2)')==False:
				m_sql = '''
				ALTER TABLE `click_pay` 
					CHANGE COLUMN `money` `money` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '付费金额' ;
				'''
				cur.execute(m_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'click_pay table %s' % msg

def cons_mode():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'cons_mode' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `cons_mode` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `name` varchar(45) NOT NULL DEFAULT '' COMMENT '消耗的名称',
			  `pid` int(6) NOT NULL DEFAULT '0',
			  PRIMARY KEY (`id`),
			  KEY `name` (`name`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'cons_mode table %s' % msg

def currency_cond():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'currency_cond' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `currency_cond` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `server_name` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器名称',
				  `s_uid` varchar(30) NOT NULL DEFAULT '' COMMENT '服务器ID',
				  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `start_inventory` bigint(20) NOT NULL DEFAULT '0' COMMENT '期初库存',
				  `atm_get` bigint(20) NOT NULL DEFAULT '0' COMMENT '充值所得',
				  `system_output` bigint(20) NOT NULL DEFAULT '0' COMMENT '系统产出',
				  `total_get` bigint(20) NOT NULL DEFAULT '0' COMMENT '获得总额',
				  `total_drain` bigint(20) NOT NULL DEFAULT '0' COMMENT '消耗总额',
				  `c_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '货币类型,1为钻石,2为金币',
				  `end_inventory` bigint(20) NOT NULL DEFAULT '0',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	if not cur.execute('Describe `currency_cond` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `currency_cond` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `end_inventory`
		'''
		cur.execute(add_guild_id_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'currency_cond table %s' % msg
def daily_levelbase():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'daily_levelbase' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `daily_levelbase` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '统计时间',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
				  `channel_name` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `server_name` varchar(50) NOT NULL DEFAULT '' COMMENT '服务器名称',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '日期',
				  `type_flag` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0未知,1代表所有玩家，2新增玩家，3停留玩家，4流失玩家',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'daily_levelbase table %s' % msg

def daily_levelmap():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'daily_levelmap' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `daily_levelmap` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `date` blob,
			  `all` blob,
			  `freeze` blob,
			  `left` blob,
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '统计时间',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	if not cur.execute('Describe `daily_levelmap` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `daily_levelmap` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `d_date`;
		'''
		cur.execute(add_guild_id_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'daily_levelmap table %s' % msg

def daily_online():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'daily_online' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `daily_online` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `os` tinyint(4) NOT NULL DEFAULT '0' COMMENT '平台',
			  `num` int(12) NOT NULL DEFAULT '0' COMMENT '当天最高在线',
			  `start_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '最高在线开始时间',
			  `end_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '最高在线结束时间',
			  `avg` int(12) NOT NULL DEFAULT '0' COMMENT '平均在线人数',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `daily_online` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `daily_online` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `d_date`;
		'''
		cur.execute(add_guild_id_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'daily_online table %s' % msg

def device_power_ster():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'device_power_ster' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `device_power_ster` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
			  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc` varchar(45) NOT NULL DEFAULT '' COMMENT '主渠道号',
			  `chsc` varchar(45) NOT NULL DEFAULT '' COMMENT '子渠道号',
			  `device_power` bigint(20) NOT NULL DEFAULT '0' COMMENT '玩家拥有的文章力量数',
			  `ster_stone` bigint(20) NOT NULL DEFAULT '0' COMMENT '玩家拥有的星石数',
			  `uid` bigint(20) NOT NULL COMMENT '玩家账号id',
			  `pid` bigint(20) NOT NULL COMMENT '玩家id',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `device_power_ster` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `device_power_ster` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `pid`;
		'''
		cur.execute(add_guild_id_sql)

	if not cur.execute(checkIndex('device_power_ster','d_date_uid_pid_ch_s_uid')):
		add_d_date_uid_pid_ch_s_uid = '''
				ALTER TABLE `device_power_ster` 
				ADD INDEX `d_date_uid_pid_ch_s_uid` (`d_date` ASC, `uid` ASC, `pid` ASC, `ch` ASC, `s_uid` ASC)
		'''
		cur.execute(add_d_date_uid_pid_ch_s_uid)
	conn.commit()
	conn.close()
	cur.close()
	print 'device_power_ster table %s' % msg

def equipment_retain():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'equipment_retain' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
		CREATE TABLE `equipment_retain` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
		  `server_name` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器名称',
		  `s_uid` varchar(30) NOT NULL DEFAULT '' COMMENT '服务器uid',
		  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
		  `new_equipment` int(10) NOT NULL DEFAULT '0' COMMENT '当天新启动设备',
		  `equipment_login_accont` int(10) NOT NULL DEFAULT '0' COMMENT '当天登录设备数',
		  `new_start_login_accont` int(10) NOT NULL DEFAULT '0' COMMENT '当天新启动并登录游戏的设备数',
		  `once_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '次日留存率',
		  `three_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第3日留存率',
		  `four_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第4日留存率',
		  `five_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第5日留存率',
		  `six_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第6日留存率',
		  `seven_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第7日留存率',
		  `fifteen_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第15日留存率',
		  `thirty_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第30日留存率',
		  `forty_five_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第45日留存率',
		  `sixty_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第60日留存率',
		  `seventy_five_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第75日留存率',
		  `ninety_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第90日留存率',
		  PRIMARY KEY (`id`)
		) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'equipment_retain table %s' % msg

def fetch_server():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'fetch_server' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `fetch_server` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `uuid` varchar(50) NOT NULL DEFAULT '' COMMENT '设备ID',
			  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
			  `os` tinyint(4) NOT NULL DEFAULT '0' COMMENT '设备系统',
			  `dn` varchar(100) NOT NULL DEFAULT '' COMMENT '设备名称',
			  `fetch_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '启动时间',
			  `d_date` int(8) NOT NULL DEFAULT '0',
			  `s_uid` varchar(45) NOT NULL DEFAULT '',
			  `new_flag` tinyint(4) NOT NULL DEFAULT '0',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `date_new_uuid_ch` (`d_date`,`new_flag`,`uuid`,`ch`),
			  KEY `time` (`fetch_time`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	if not cur.execute('Describe `fetch_server` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `fetch_server` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `new_flag`
		'''
		cur.execute(add_guild_id_sql)
	if not cur.execute(checkIndex('fetch_server','date_new_uuid_ch')):
		add_date_new_uuid_ch = '''
			ALTER TABLE `fetch_server` 
			ADD INDEX `date_new_uuid_ch` (`d_date` ASC, `new_flag` ASC, `uuid` ASC, `ch` ASC);
		'''
		cur.execute(add_date_new_uuid_ch)
	if not cur.execute(checkIndex('fetch_server','time')):
		add_time = '''
				ALTER TABLE `fetch_server` 
				ADD INDEX `time` (`fetch_time` ASC)
		'''
		cur.execute(add_time)

	if not cur.execute(checkIndex('fetch_server','uuid')):
		add_uuid = '''
				ALTER TABLE `fetch_server` 
				ADD INDEX `uuid` (`uuid` ASC)
		'''
		cur.execute(add_uuid)
	conn.commit()
	conn.close()
	cur.close()
	print 'fetch_server table %s' % msg

def guild_summary():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'guild_summary' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `guild_summary` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
				  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据发送的时间',
				  `chmc` varchar(45) NOT NULL DEFAULT '' COMMENT '主渠道号',
				  `chsc` varchar(45) NOT NULL DEFAULT '' COMMENT '子渠道号',
				  `guild_id` varchar(45) NOT NULL DEFAULT '' COMMENT '冒险团id',
				  `disband_flag` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0冒险团健在，1冒险团已被解散',
				  `guild_member_num` int(11) NOT NULL DEFAULT '0' COMMENT '在团玩家数',
				  `diamond_donate` int(11) NOT NULL DEFAULT '0' COMMENT '钻石捐献次数',
				  `prestige` bigint(20) NOT NULL DEFAULT '0' COMMENT '剩余总贡献',
				  `camp_wood_num` int(11) NOT NULL DEFAULT '0' COMMENT '伐木场总次数',
				  `camp_stone_num` int(11) NOT NULL DEFAULT '0' COMMENT '矿场总次数',
				  `open_boss` tinyint(4) NOT NULL DEFAULT '0' COMMENT 'boss开启次数',
				  `kill_boss` tinyint(4) NOT NULL DEFAULT '0' COMMENT '击杀boss次数',
				  `money` bigint(20) NOT NULL DEFAULT '0' COMMENT '冒险团剩余资金总量',
				  `wood` bigint(20) NOT NULL DEFAULT '0' COMMENT '冒险团剩余木材的总量',
				  `stone` bigint(20) NOT NULL DEFAULT '0' COMMENT '冒险团剩余石头的总量',
				  `marnastone` bigint(20) NOT NULL DEFAULT '0' COMMENT '冒险团剩余原石的总量',
				  `guild_lv` tinyint(4) NOT NULL DEFAULT '0' COMMENT '冒险团等级',
				  `shop_lv` tinyint(4) NOT NULL DEFAULT '0' COMMENT '冒险团商店等级',
				  `room_lv` tinyint(4) NOT NULL DEFAULT '0' COMMENT '冒险团冥想室等级',
				  `storehouse` tinyint(4) NOT NULL DEFAULT '0' COMMENT '冒险团仓库等级',
				  `prosperity_lv` tinyint(4) NOT NULL DEFAULT '0' COMMENT '冒险团繁荣度等级',
				  `time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '发送时间',
				  `unique_id` varchar(60) DEFAULT '',
				  PRIMARY KEY (`id`),
				  KEY `time` (`time`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `guild_summary` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `guild_summary` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `time`;
		'''
		cur.execute(add_guild_id_sql)
	if not cur.execute(checkIndex('guild_summary','time')):
		add_time_index = '''
			ALTER TABLE `guild_summary` 
			ADD INDEX `time` (`time` ASC)

		'''
	conn.commit()
	conn.close()
	cur.close()
	print 'guild_summary table %s' % msg

def hours_statistics():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'hours_statistics' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `hours_statistics` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
		  `hours` varchar(10) NOT NULL DEFAULT '00' COMMENT '统计所处的小时时间点',
		  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
		  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器ID',
		  `new_equip` int(11) NOT NULL DEFAULT '0' COMMENT '新增设备数',
		  `new_account` int(11) NOT NULL DEFAULT '0' COMMENT '新增账号数',
		  `actoin` int(11) NOT NULL DEFAULT '0' COMMENT '活跃人数',
		  `pay_account` int(11) NOT NULL DEFAULT '0' COMMENT '付费账号数',
		  `pay_income` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '付费金额',
		  PRIMARY KEY (`id`)
			) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'


	ret = cur.execute('desc hours_statistics')
	for i in cur.fetchall():
		if i.get('Field','')=='pay_income':
			Type = i.get('Type')
			if (Type=='decimal(12,2)')==False:
				m_sql = '''
				ALTER TABLE `hours_statistics` 
				CHANGE COLUMN `pay_income` `pay_income` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '付费金额' 
				'''
				cur.execute(m_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'hours_statistics table %s' % msg

def level_dis():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'level_dis' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `level_dis` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `b_id` int(11) NOT NULL,
				  `level` int(11) NOT NULL DEFAULT '0' COMMENT '游戏等级',
				  `num` int(11) NOT NULL DEFAULT '0' COMMENT '等级拥有的人数',
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'level_dis table %s' % msg

def litem():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'litem' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `litem` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `uid` bigint(20) NOT NULL DEFAULT '0',
				  `pid` bigint(20) NOT NULL DEFAULT '0',
				  `type` varchar(50) NOT NULL DEFAULT '',
				  `created` blob,
				  `deleted` blob,
				  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `deal_flag` bit(1) NOT NULL DEFAULT b'0' COMMENT '当天记录是否被处理过',
				  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
				  `chmc` varchar(50) NOT NULL DEFAULT '',
				  `chsc` varchar(50) NOT NULL DEFAULT '',
				  `unique_id` varchar(60) DEFAULT '',
				  PRIMARY KEY (`id`),
				  KEY `deal_flag` (`deal_flag`)
				) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `litem` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `litem` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `chsc`
		'''
		cur.execute(add_guild_id_sql)
	if not cur.execute(checkIndex('litem','deal_flag')):
		add_time_index = '''
		ALTER TABLE `litem` 
		ADD INDEX `deal_flag` ( `deal_flag` ASC)

		'''
		cur.execute(add_time_index)
	conn.commit()
	conn.close()
	cur.close()
	print 'litem table %s' % msg

def ltv_value():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'ltv_value' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `ltv_value` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `server_name` varchar(30) NOT NULL DEFAULT '' COMMENT '服务器名称',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器ID',
				  `channel_name` varchar(30) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `new_account_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天新登玩家',
				  `three_days_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '3天内第一日新增账号付费总额/第一日新增账号数',
				  `three_days_income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '3天内第一日新增账号付费总额',
				  `seven_days_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '7天内第一日新增账号付费总额/第一日新增账号数',
				  `seven_days_income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '7日收入',
				  `half_moon_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '15天内第一日新增账号付费总额/第一日新增账号数',
				  `half_moon_income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '15天内第一日新增账号付费总额',
				  `one_month_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '30天内第一日新增账号付费总额/第一日新增账号数',
				  `one_month_income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '30天内第一日新增账号付费总额',
				  `forty_five_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '第45日内第一日新增账号付费总额/第一日新增账号数',
				  `forty_five_income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '第45日内第一日新增账号付费总额',
				  `sixty_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '第60日内第一日新增账号付费总额/第一日新增账号数',
				  `sixty_income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '第60日内第一日新增账号付费总额',
				  `seventy_five_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '第75日内第一日新增账号付费总额/第一日新增账号数',
				  `seventy_five_income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '第75日内第一日新增账号付费总额',
				  `ninety_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '第90日内第一日新增账号付费总额/第一日新增账号数',
				  `ninety_income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '第90日内第一日新增账号付费总额',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `ltv_value` `four_month_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `four_month_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `ninety_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `five_month_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `five_month_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `four_month_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `six_month_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `six_month_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `five_month_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `one_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `one_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `six_month_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `two_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `two_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `one_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `four_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `four_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `two_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `five_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `five_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `four_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `six_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `six_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `five_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `eight_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `eight_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `six_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `nine_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `nine_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `eight_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `ten_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `ten_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `nine_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `eleven_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `eleven_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `ten_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `twelve_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `twelve_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `eleven_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `thirteen_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `thirteen_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `twelve_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `fourteen_day_income` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `fourteen_day_income` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `thirteen_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `two_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `two_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `fourteen_day_income`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `four_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `four_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `two_day_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `five_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `five_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `four_day_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `six_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `six_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `five_day_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `eight_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `eight_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `six_day_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `nine_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `nine_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `eight_day_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `ten_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `ten_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `nine_day_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `eleven_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `eleven_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `ten_day_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `twelve_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `twelve_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `eleven_day_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `thirteen_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `thirteen_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `twelve_day_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `fourteen_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `fourteen_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `thirteen_day_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `four_month_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `four_month_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `fourteen_day_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `five_month_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `five_month_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `four_month_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `six_month_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `six_month_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `five_month_ltv`
		'''
		cur.execute(add_sql)
	if not cur.execute('Describe `ltv_value` `one_day_ltv` '):
		add_sql = '''
			ALTER TABLE `ltv_value` 
			ADD COLUMN `one_day_ltv` decimal(10,2) NOT NULL DEFAULT '0.00' AFTER `six_month_ltv`
		'''
		cur.execute(add_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'ltv_value table %s' % msg


def magic_stone():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'magic_stone' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `magic_stone` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
			  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc` varchar(45) NOT NULL DEFAULT '' COMMENT '主渠道号',
			  `chsc` varchar(45) NOT NULL DEFAULT '' COMMENT '子渠道号',
			  `magic_stone_name` varchar(50) NOT NULL COMMENT '魔石名称',
			  `magic_stone_num` int(11) NOT NULL DEFAULT '0' COMMENT '魔石数量',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'magic_stone table %s' % msg

def mg_daily_chs():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'mg_daily_chs' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `mg_daily_chs` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `new_login_accont` int(10) NOT NULL DEFAULT '0' COMMENT '当天，新注册并登陆游戏的去重账号数',
				  `login_account` int(10) NOT NULL DEFAULT '0' COMMENT '当天，登陆游戏并且去重的账号数',
				  `pay_account_num` int(10) NOT NULL DEFAULT '0' COMMENT '付费账号数',
				  `income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '当天收入',
				  `first_pay_account` int(10) NOT NULL DEFAULT '0' COMMENT '首次付费账号数',
				  `first_pay_account_income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '首次付费收入',
				  `new_login_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '新登付费数',
				  `new_login_pay_income` decimal(10,2) DEFAULT '0.00' COMMENT '新登账号收入',
				  `one_retain_days` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '次日账号存留率',
				  `three_retain_days` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '3日存留账号率',
				  `seven_retain_days` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '7日账号存留率',
				  `average_number_online` int(12) NOT NULL DEFAULT '0' COMMENT '当天平均在线人数',
				  `highest_online` int(15) NOT NULL DEFAULT '0' COMMENT '当天最高在线人数',
				  `new_equipment` int(11) NOT NULL DEFAULT '0' COMMENT '新增去重设备数',
				  `valid_e_num` int(11) NOT NULL DEFAULT '0' COMMENT '新增加的设备中，注册了账号的设备数',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `mg_daily_chs` `atm_num` '):
			add_ch_sql = '''
				ALTER TABLE `mg_daily_chs` 
				ADD COLUMN `atm_num` int(10) NULL DEFAULT 0 AFTER `valid_e_num`
			'''
			cur.execute(add_ch_sql)

	conn.commit()
	conn.close()
	cur.close()
	print 'mg_daily_chs table %s' % msg

def mg_daily_newspaper():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'mg_daily_newspaper' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `mg_daily_newspaper` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `server_name` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器名称',
			  `s_uid` varchar(20) NOT NULL DEFAULT '' COMMENT '服务器的uid',
			  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
			  `new_login_accont` int(10) NOT NULL DEFAULT '0' COMMENT '当天，新注册并登陆游戏的去重账号数',
			  `login_account` int(10) NOT NULL DEFAULT '0' COMMENT '当天，登陆游戏并且去重的账号数',
			  `pay_account_num` int(10) NOT NULL DEFAULT '0' COMMENT '付费账号数',
			  `income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '当天收入',
			  `first_pay_account` int(10) NOT NULL DEFAULT '0' COMMENT '首次付费账号数',
			  `first_pay_account_income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '首次付费收入',
			  `new_login_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '新登付费数',
			  `new_login_pay_income` decimal(10,2) DEFAULT '0.00' COMMENT '新登账号收入',
			  `pay_ARPU` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '付费 ARPU',
			  `DAU_ARPU` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT 'DAU ARPU',
			  `one_retain_days` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '次日账号存留率',
			  `three_retain_days` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '3日存留账号率',
			  `seven_retain_days` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '7日账号存留率',
			  `average_number_online` int(12) NOT NULL DEFAULT '0' COMMENT '当天平均在线人数',
			  `highest_online` int(15) NOT NULL DEFAULT '0' COMMENT '当天最高在线人数',
			  `new_equipment` int(11) NOT NULL DEFAULT '0' COMMENT '新增去重设备数',
			  `valid_e_num` int(11) NOT NULL DEFAULT '0' COMMENT '新增加的设备中，注册了账号的设备数',
			  PRIMARY KEY (`id`),
			  KEY `dsch` (`d_date`,`channel_name`,`s_uid`)
			) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	if not cur.execute('Describe `mg_daily_newspaper` `atm_num` '):
			add_ch_sql = '''
				ALTER TABLE `mg_daily_newspaper` 
				ADD COLUMN `atm_num` int(10) NULL DEFAULT 0 AFTER `valid_e_num`
			'''
			cur.execute(add_ch_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'mg_daily_newspaper table %s' % msg


def mg_menus():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'mg_menus' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `mg_menus` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `name` varchar(45) NOT NULL,
			  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			  `pid` int(11) NOT NULL DEFAULT '0',
			  `icon_class` varchar(45) DEFAULT '',
			  `link` varchar(128) NOT NULL,
			  `target` varchar(20) DEFAULT '',
			  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			  `delete_flag` bit(1) NOT NULL DEFAULT b'0',
			  `icon` varchar(45) NOT NULL DEFAULT '',
			  `priority` int(11) NOT NULL DEFAULT '0',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'mg_menus table %s' % msg

def mg_menus_access():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'mg_menus_access' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `mg_menus_access` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `role_id` int(11) NOT NULL,
			  `menu_id` int(11) NOT NULL,
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'mg_menus_access table %s' % msg


def mg_roles():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'mg_roles' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `mg_roles` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `role_name` varchar(45) DEFAULT NULL,
				  `delete_flag` bit(1) NOT NULL DEFAULT b'0',
				  `status_flag` bit(1) NOT NULL DEFAULT b'0' COMMENT '0:正常 1:弃用',
				  `role_describe` varchar(120) DEFAULT '',
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
				  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'mg_roles table %s' % msg

def mg_user_role():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'mg_user_role' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `mg_user_role` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `role_id` int(11) NOT NULL,
			  `u_id` int(11) NOT NULL,
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'mg_user_role table %s' % msg


def num_of_phy_pur():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'num_of_phy_pur' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `num_of_phy_pur` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `server_name` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器名称',
				  `s_uid` varchar(30) NOT NULL DEFAULT '' COMMENT '服务器ID',
				  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `vip_level` varchar(45) NOT NULL DEFAULT '' COMMENT 'VIP等级',
				  `total_vip_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天登录玩家各个vip等级的总人数',
				  `once_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买1次的玩家总数',
				  `twice_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买2次的玩家总数',
				  `three_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买3次的玩家总数',
				  `four_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买4次的玩家总数',
				  `five_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买5次的玩家总数',
				  `six_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买6次的玩家总数',
				  `seven_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买7次的玩家总数',
				  `eight_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买8次的玩家总数',
				  `nine_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买9次的玩家总数',
				  `ten_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买10次的玩家总数',
				  `eleven_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买11次的玩家总数',
				  `twelve_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买12次的玩家总数',
				  `thirt_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买13次的玩家总数',
				  `fourt_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买14次的玩家总数',
				  `fift_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买15次的玩家总数',
				  `sixt_pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '当天对应vip等级购买16次的玩家总数',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'num_of_phy_pur table %s' % msg

def OD_distributoin():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'OD_distributoin' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `OD_distributoin` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `server_name` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器名称',
			  `s_uid` varchar(30) NOT NULL DEFAULT '' COMMENT '服务器ID',
			  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
			  `type` varchar(50) NOT NULL DEFAULT '' COMMENT '分布点',
			  `diff` bigint(20) NOT NULL DEFAULT '0' COMMENT '货币的产出消耗变动',
			  `p_num` bigint(20) NOT NULL DEFAULT '0' COMMENT '产出消耗的玩家数',
			  `num` bigint(20) NOT NULL DEFAULT '0' COMMENT '产出消耗次数',
			  `c_type` tinyint(4) NOT NULL COMMENT '货币类型,1为钻石,2为金币',
			  `status_flag` tinyint(4) NOT NULL COMMENT '1为产出，2为消耗',
			  PRIMARY KEY (`id`),
			  KEY `c_ts` (`c_type`,`status_flag`,`type`)
			) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'OD_distributoin table %s' % msg

def output_drain():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'output_drain' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `output_drain` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `uid` bigint(20) NOT NULL DEFAULT '0',
				  `pid` bigint(20) NOT NULL DEFAULT '0',
				  `type` varchar(50) NOT NULL DEFAULT '',
				  `have` int(12) NOT NULL DEFAULT '0',
				  `diff` int(12) NOT NULL DEFAULT '0',
				  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `item_desc` varchar(200) DEFAULT NULL,
				  `server_name` varchar(45) NOT NULL DEFAULT '',
				  `c_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '货币的类型1为钻石2为金币',
				  `status_flag` tinyint(4) NOT NULL DEFAULT '0' COMMENT '1代表系统获得货币，2代表消耗货币,3代表充值获得',
				  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '所属渠道',
				  `chmc` varchar(45) NOT NULL DEFAULT '',
				  `chsc` varchar(45) NOT NULL DEFAULT '',
				  `guild_id` bigint(20) NOT NULL DEFAULT '0',
				  `unique_id` varchar(60) DEFAULT '',
				  PRIMARY KEY (`id`,`status_flag`),
				  KEY `pid_time` (`pid`,`time`,`have`),
				  KEY `diff` (`diff`),
				  KEY `time` (`time`),
				  KEY `csdchs_uid` (`c_type`,`status_flag`,`d_date`,`s_uid`,`ch`,`type`,`time`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `output_drain` `guild_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `output_drain` 
			ADD COLUMN `guild_id` BIGINT(20) NOT NULL DEFAULT 0 AFTER `chsc` 
		'''
		cur.execute(add_guild_id_sql)
	if not cur.execute('Describe `output_drain` `unique_id` '):
		add_unique_id_sql = '''
		ALTER TABLE `output_drain` 
		ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `guild_id`
		'''
		cur.execute(add_unique_id_sql)
	## 增加一个物品id
	if not cur.execute('Describe `output_drain` `goods_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `output_drain` 
			ADD COLUMN `goods_id` BIGINT(20) NOT NULL DEFAULT 0 AFTER `unique_id`
		'''
		cur.execute(add_guild_id_sql)

	## 增加索引
	if not cur.execute(checkIndex('output_drain','c_type_goods_id_time')):
		add_index_sql = '''
			ALTER TABLE `output_drain` 
			ADD INDEX `c_type_goods_id_time` (`c_type` ASC, `goods_id` ASC, `time` ASC)

		'''
		cur.execute(add_index_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'output_drain table %s' % msg

def overview():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'overview' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `overview` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `new_equipment` bigint(20) NOT NULL DEFAULT '0' COMMENT '新增加去重设备',
				  `new_login_account` bigint(20) NOT NULL DEFAULT '0' COMMENT '新增加的去重注册账号',
				  `login_account` bigint(20) NOT NULL DEFAULT '0' COMMENT '登录游戏的去重玩家数',
				  `pay_account` bigint(20) NOT NULL DEFAULT '0' COMMENT '付费的玩家数',
				  `pay_income` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '收入总额',
				  `new_equip_login` bigint(20) NOT NULL DEFAULT '0' COMMENT '新增加的设备并且有登录游戏的设备数',
				  `start_equip` bigint(20) NOT NULL DEFAULT '0' COMMENT '启动游戏的设备数',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'overview table %s' % msg

def partner_device():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'partner_device' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `partner_device` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
				  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
				  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道',
				  `chmc` varchar(45) NOT NULL DEFAULT '' COMMENT '主渠道号',
				  `chsc` varchar(45) NOT NULL DEFAULT '' COMMENT '子渠道号',
				  `partner_name` varchar(50) NOT NULL COMMENT '伙伴名称',
				  `device_level` tinyint(4) NOT NULL DEFAULT '0' COMMENT '伙伴纹章等级',
				  `own_num` int(11) NOT NULL DEFAULT '0' COMMENT '拥有的人数',
				  PRIMARY KEY (`id`),
				  KEY `pd` (`partner_name`,`device_level`)
				) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'partner_device table %s' % msg

def pay():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'pay' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `pay` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `uuid` varchar(50) NOT NULL DEFAULT '' COMMENT '设备ID',
				  `uid` bigint(20) NOT NULL DEFAULT '0' COMMENT '账号ID',
				  `pid` bigint(20) NOT NULL DEFAULT '0' COMMENT '角色ID',
				  `account` varchar(50) NOT NULL DEFAULT '' COMMENT '账号',
				  `player` varchar(30) NOT NULL DEFAULT '' COMMENT '玩家昵称',
				  `ip` varchar(30) NOT NULL DEFAULT '' COMMENT '消费时IP',
				  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
				  `chmc` varchar(50) NOT NULL DEFAULT '' COMMENT '主渠道号',
				  `chsc` varchar(50) NOT NULL DEFAULT '' COMMENT '子渠道号',
				  `os` tinyint(4) NOT NULL DEFAULT '0' COMMENT '平台',
				  `cost_name` varchar(100) NOT NULL DEFAULT '' COMMENT '消费内容',
				  `cost_count` int(10) NOT NULL DEFAULT '0' COMMENT '消费数量',
				  `left_count` int(10) NOT NULL DEFAULT '0' COMMENT '剩余金额',
				  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '消费时间',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `unique_id` varchar(60) DEFAULT '',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'


	if not cur.execute('Describe `pay` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `pay` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `d_date`;
		'''
		cur.execute(add_guild_id_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'pay table %s' % msg

def pay_points():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'pay_points' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `pay_points` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `pay_points` varchar(30) NOT NULL COMMENT '付费点',
			  `pay_num` int(10) NOT NULL DEFAULT '0' COMMENT '购买数量',
			  `amount_of_recharge` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '充值金额',
			  `pay_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '购买时间',
			  `s_uid` varchar(45) DEFAULT NULL COMMENT '服务器uid',
			  `ch` varchar(50) DEFAULT NULL,
			  `d_date` int(8) DEFAULT '0',
			  `chmc` varchar(50) DEFAULT NULL,
			  `chsc` varchar(50) DEFAULT NULL,
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	ret = cur.execute('desc pay_points')
	for i in cur.fetchall():
		if i.get('Field','')=='amount_of_recharge':
			Type = i.get('Type')
			if (Type =='decimal(12,2)')==False:
				m_sql = '''
					ALTER TABLE `pay_points` 
					CHANGE COLUMN `amount_of_recharge` `amount_of_recharge` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '充值金额' 
				'''
				cur.execute(m_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'pay_points table %s' % msg

def physical_buy():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'physical_buy' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `physical_buy` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `pid` bigint(20) NOT NULL DEFAULT '0' COMMENT '玩家ID',
			  `vip` varchar(45) NOT NULL DEFAULT 'vip0' COMMENT '玩家购买时的VIP等级',
			  `count` int(6) NOT NULL DEFAULT '0' COMMENT '购买次数',
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '付费时间',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc` varchar(50) NOT NULL DEFAULT '' COMMENT '主渠道号',
			  `chsc` varchar(50) NOT NULL DEFAULT '' COMMENT '子渠道号',
			  `deal_flag` bit(1) NOT NULL DEFAULT b'0',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `date_pid_suid_ch` (`d_date`,`pid`,`s_uid`,`ch`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `physical_buy` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `physical_buy` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `deal_flag`
		'''
		cur.execute(add_guild_id_sql)
	if not cur.execute(checkIndex('physical_buy','date_pid_suid_ch')):
		add_date_pid_suid_ch_index = '''
		ALTER TABLE `physical_buy` 
		ADD INDEX `date_pid_suid_ch` (`d_date` ASC, `pid` ASC, `s_uid` ASC, `ch` ASC)

		'''
		cur.execute(add_date_pid_suid_ch_index)
	conn.commit()
	conn.close()
	cur.close()
	print 'physical_buy table %s' % msg


def player_create():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'player_create' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `player_create` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
				  `chmc` varchar(50) NOT NULL DEFAULT '' COMMENT '主渠道号',
				  `chsc` varchar(50) NOT NULL DEFAULT '' COMMENT '子渠道号',
				  `uid` bigint(20) NOT NULL DEFAULT '0' COMMENT '账号ID',
				  `uuid` varchar(50) NOT NULL DEFAULT '' COMMENT '设备ID',
				  `pid` bigint(20) NOT NULL DEFAULT '0' COMMENT '角色ID',
				  `os` tinyint(4) NOT NULL DEFAULT '0' COMMENT '平台id',
				  `ip` varchar(30) NOT NULL DEFAULT '' COMMENT '创建IP地址',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `unique_id` varchar(60) DEFAULT '',
				  PRIMARY KEY (`id`),
				  KEY `dchsuid` (`d_date`,`ch`,`s_uid`)
				) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'


	if not cur.execute('Describe `player_create` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `player_create` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `d_date`;
		'''
		cur.execute(add_guild_id_sql)
	if not cur.execute(checkIndex('player_create','dchsuid')):
		add_dchsuid = '''
			ALTER TABLE `player_create` 
			ADD INDEX `dchsuid` (`d_date` ASC, `ch` ASC, `s_uid` ASC)
		'''
		cur.execute(add_dchsuid)
	conn.commit()
	conn.close()
	cur.close()
	print 'player_create table %s' % msg

def player_login():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'player_login' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `player_login` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `login_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '登录时间',
				  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
				  `chmc` varchar(50) NOT NULL DEFAULT '' COMMENT '主渠道号',
				  `chsc` varchar(50) NOT NULL DEFAULT '' COMMENT '子渠道号',
				  `uid` bigint(20) NOT NULL DEFAULT '0' COMMENT '账号ID',
				  `uuid` varchar(50) NOT NULL DEFAULT '' COMMENT '设备ID',
				  `pid` bigint(20) NOT NULL DEFAULT '0' COMMENT '角色ID',
				  `ip` varchar(50) NOT NULL DEFAULT '' COMMENT '登录IP地址',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `os` tinyint(4) NOT NULL DEFAULT '0' COMMENT '平台id',
				  `vip` varchar(45) NOT NULL DEFAULT 'VIP0',
				  `level` int(5) NOT NULL DEFAULT '1',
				  `unique_id` varchar(60) DEFAULT '',
				  PRIMARY KEY (`id`),
				  KEY `schdv` (`d_date`,`s_uid`,`ch`,`vip`),
				  KEY `log_time` (`login_time`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	if not cur.execute('Describe `player_login` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `player_login` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `level`;
		'''
		cur.execute(add_guild_id_sql)
	if not cur.execute(checkIndex('player_login','log_time')):
		add_log_time = '''ALTER TABLE `player_login` 
						ADD INDEX `log_time` (`login_time` ASC)
						'''
		cur.execute(add_log_time)
	if not cur.execute(checkIndex('player_login','pid_s_uid')):
		add_pid_s_uid = '''ALTER TABLE `player_login` 
							ADD INDEX `pid_s_uid` (`pid` ASC, `s_uid` ASC);
						'''
		cur.execute(add_pid_s_uid)
	conn.commit()
	conn.close()
	cur.close()
	print 'player_login table %s' % msg

def player_logout():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'player_logout' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `player_logout` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `logout_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '登出时间',
			  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc` varchar(50) NOT NULL DEFAULT '' COMMENT '主渠道号',
			  `chsc` varchar(50) NOT NULL DEFAULT '' COMMENT '子渠道号',
			  `uid` bigint(20) NOT NULL DEFAULT '0' COMMENT '账号ID',
			  `uuid` varchar(50) NOT NULL DEFAULT '' COMMENT '设备ID',
			  `pid` bigint(20) NOT NULL DEFAULT '0' COMMENT '角色ID',
			  `ip` varchar(50) NOT NULL DEFAULT '' COMMENT '登录IP地址',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `os` tinyint(4) NOT NULL DEFAULT '0' COMMENT '平台id',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'


	if not cur.execute('Describe `player_logout` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `player_logout` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `os`;
		'''
		cur.execute(add_guild_id_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'player_logout table %s' % msg

def player_own():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'player_own' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `player_own` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
			  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc` varchar(45) NOT NULL DEFAULT '' COMMENT '主渠道号',
			  `chsc` varchar(45) NOT NULL DEFAULT '' COMMENT '子渠道号',
			  `name` varchar(45) NOT NULL DEFAULT '' COMMENT '精灵名称',
			  `level` tinyint(4) NOT NULL DEFAULT '1' COMMENT '精灵等级',
			  `uid` bigint(20) NOT NULL COMMENT '玩家账号id',
			  `pid` bigint(20) NOT NULL COMMENT '玩家id',
			  `type` tinyint(4) NOT NULL DEFAULT '1' COMMENT '区分类型',
			  PRIMARY KEY (`id`),
			  KEY `dtypechsuidnamelevel` (`d_date`,`type`,`ch`,`s_uid`,`name`,`level`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	if not cur.execute(checkIndex('player_own','d_date_uid_pid_ch_s_uid')):
		add_d_date_uid_pid_ch_s_uid = '''
				ALTER TABLE `player_own` 
				ADD INDEX `d_date_uid_pid_ch_s_uid` (`d_date` ASC, `uid` ASC, `pid` ASC, `ch` ASC, `s_uid` ASC)
		'''
		cur.execute(add_d_date_uid_pid_ch_s_uid)
	conn.commit()
	conn.close()
	cur.close()
	print 'player_own table %s' % msg

def player_regist():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'player_regist' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `player_regist` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `reg_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '账号注册时间',
			  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc` varchar(50) NOT NULL DEFAULT '' COMMENT '主渠道号',
			  `chsc` varchar(50) NOT NULL DEFAULT '' COMMENT '子渠道号',
			  `uid` bigint(20) NOT NULL DEFAULT '0' COMMENT '账号ID',
			  `uuid` varchar(50) NOT NULL DEFAULT '' COMMENT '设备ID',
			  `ip` varchar(50) NOT NULL DEFAULT '' COMMENT '当前注册玩家的ip地址',
			  `os` tinyint(4) NOT NULL DEFAULT '0' COMMENT '平台id',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `date` (`d_date`),
			  KEY `reg_time` (`reg_time`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	if not cur.execute('Describe `player_regist` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `player_regist` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `d_date`;
		'''
		cur.execute(add_guild_id_sql)
	if not cur.execute(checkIndex('player_regist','reg_time')):
		add_reg_time = '''
		ALTER TABLE `player_regist` 
		ADD INDEX `reg_time` (`reg_time` ASC)

		'''
		cur.execute(add_reg_time)
	conn.commit()
	conn.close()
	cur.close()
	print 'player_regist table %s' % msg

def player_retain():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'player_retain' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `player_retain` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `server_name` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器名称',
				  `s_uid` varchar(30) NOT NULL DEFAULT '' COMMENT '服务器uid',
				  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `new_login_accont` int(10) NOT NULL DEFAULT '0' COMMENT '当天新增账号数',
				  `once_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '次日留存率',
				  `three_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第3日留存率',
				  `four_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第4日留存率',
				  `five_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第5日留存率',
				  `six_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第6日留存率',
				  `seven_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第7日留存率',
				  `fifteen_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第15日留存率',
				  `thirty_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第30日留存率',
				  `forty_five_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第45日留存率',
				  `sixty_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第60日留存率',
				  `seventy_five_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第75日留存率',
				  `ninety_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第90日留存率',
				  `login_account` int(10) NOT NULL DEFAULT '0',
				  `regist_account` int(10) NOT NULL DEFAULT '0',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'player_retain table %s' % msg


def relic_hero():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'relic_hero' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `relic_hero` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
			  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc` varchar(45) NOT NULL DEFAULT '' COMMENT '主渠道号',
			  `chsc` varchar(45) NOT NULL DEFAULT '' COMMENT '子渠道号',
			  `name` varchar(45) NOT NULL DEFAULT '' COMMENT '挑战boss的名称',
			  `type` varchar(45) NOT NULL DEFAULT '' COMMENT '挑战boss类型',
			  `b_type` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'boss类型',
			  `uid` bigint(20) NOT NULL COMMENT '用户账号id',
			  `pid` bigint(20) NOT NULL COMMENT '玩家id',
			  `finish_time` int(11) NOT NULL DEFAULT '0' COMMENT 'boss完成时间',
			  `s_flag` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0挑战成功，1挑战失败',
			  `pid_time` varchar(45) NOT NULL COMMENT '玩家id和时间戳区别属于同一次进入关卡',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `date_suid_ch_name` (`d_date`,`s_uid`,`ch`,`name`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `relic_hero` `unique_id` '):
		add_guild_id_sql = '''
			ALTER TABLE `relic_hero` 
			ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `pid_time`
		'''
		cur.execute(add_guild_id_sql)
	if not cur.execute(checkIndex('relic_hero','date_suid_ch_name')):
		add_date_suid_ch_name_index = '''
		ALTER TABLE `relic_hero` 
		ADD INDEX `date_suid_ch_name` (`d_date` ASC, `s_uid` ASC, `ch` ASC, `name` ASC)

		'''
		cur.execute(add_date_suid_ch_name_index)
	conn.commit()
	conn.close()
	cur.close()
	print 'relic_hero table %s' % msg


def role_retain():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'role_retain' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `role_retain` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
				  `server_name` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器名称',
				  `s_uid` varchar(30) NOT NULL DEFAULT '' COMMENT '服务器uid',
				  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `new_login_accont` int(10) NOT NULL DEFAULT '0' COMMENT '当天新创建角色数',
				  `role_login_accont` int(10) NOT NULL DEFAULT '0' COMMENT '当天登录角色数',
				  `create_role_accont` int(10) NOT NULL DEFAULT '0' COMMENT '角色创建数',
				  `once_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '次日留存率',
				  `three_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第3日留存率',
				  `four_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第4日留存率',
				  `five_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第5日留存率',
				  `six_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第6日留存率',
				  `seven_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第7日留存率',
				  `fifteen_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第15日留存率',
				  `thirty_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第30日留存率',
				  `forty_five_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第45日留存率',
				  `sixty_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第60日留存率',
				  `seventy_five_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第75日留存率',
				  `ninety_retain` decimal(6,2) NOT NULL DEFAULT '0.00' COMMENT '第90日留存率',
				  PRIMARY KEY (`id`)
				) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'role_retain table %s' % msg

def servers_data():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'servers_data' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `servers_data` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `uid` varchar(45) NOT NULL DEFAULT '',
			  `name` varchar(45) NOT NULL DEFAULT '',
			  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			  PRIMARY KEY (`id`),
			  KEY `uid` (`uid`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'servers_data table %s' % msg

def spirit_level():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'spirit_level' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `spirit_level` (
				  `id` int(11) NOT NULL AUTO_INCREMENT,
				  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
				  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
				  `server_name` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器名称',
				  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
				  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
				  `name` varchar(45) NOT NULL DEFAULT '' COMMENT '精灵名称',
				  `level` tinyint(4) NOT NULL DEFAULT '1' COMMENT '精灵等级',
				  `num_owner` int(11) NOT NULL DEFAULT '0' COMMENT '持有人数',
				  PRIMARY KEY (`id`),
				  KEY `s_uid_channel_name_level` (`s_uid`,`channel_name`,`level`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'spirit_level table %s' % msg

def task_checkpoint():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'task_checkpoint' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `task_checkpoint` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
			  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc` varchar(45) NOT NULL DEFAULT '' COMMENT '主渠道号',
			  `chsc` varchar(45) NOT NULL DEFAULT '' COMMENT '子渠道号',
			  `name` varchar(45) NOT NULL DEFAULT '' COMMENT '挑战boss的名称',
			  `type` varchar(45) NOT NULL DEFAULT '' COMMENT '挑战boss类型',
			  `b_type` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'boss类型',
			  `avg_time` int(11) NOT NULL DEFAULT '0' COMMENT '挑战完成的平均时间',
			  `challenge_num` bigint(20) NOT NULL COMMENT '进行boss挑战的总次数',
			  `success` bigint(20) NOT NULL DEFAULT '0' COMMENT '成功挑战boss的次数',
			  `player_num` int(11) NOT NULL COMMENT '挑战boss的玩家数',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	if not cur.execute('Describe `task_checkpoint` `create_room_num` '):
			add_sql = '''
				ALTER TABLE `task_checkpoint` 
				ADD COLUMN `create_room_num` INT(11) NULL DEFAULT 0 AFTER `player_num`
			'''
			cur.execute(add_sql)

	if not cur.execute('Describe `task_checkpoint` `dis_room_num` '):
			add_sql = '''
				ALTER TABLE `task_checkpoint` 
				ADD COLUMN `dis_room_num` INT(11) NULL DEFAULT 0 AFTER `create_room_num`
			'''
			cur.execute(add_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'task_checkpoint table %s' % msg

def user_secontrol():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'user_secontrol' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `user_secontrol` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `u_id` int(8) NOT NULL COMMENT '用户id',
			  `secontrol` text NOT NULL,
			  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'user_secontrol table %s' % msg

def users():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'users' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `users` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `login_name` varchar(20) NOT NULL,
			  `password` varchar(36) NOT NULL,
			  `phone` int(12) DEFAULT '0',
			  `email` varchar(30) DEFAULT '0',
			  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			  `login_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			  `last_login_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			  `login_num` int(11) DEFAULT '0',
			  `delete_flag` bit(1) NOT NULL DEFAULT b'0',
			  `admin_flag` bit(1) NOT NULL DEFAULT b'0',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'users table %s' % msg

def vip_level():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'vip_level' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `vip_level` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `vip_level` varchar(20) NOT NULL DEFAULT 'VIP0' COMMENT 'vip等级',
			  PRIMARY KEY (`id`),
			  UNIQUE KEY `vip_le` (`vip_level`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'vip_level table %s' % msg

def whale_player():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'whale_player' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `whale_player` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `channel_name` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道名称',
			  `s_uid` varchar(50) NOT NULL DEFAULT '' COMMENT '服务器ID',
			  `server_name` varchar(50) NOT NULL DEFAULT '' COMMENT '服务器名称',
			  `last_pay_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '最后充值日期',
			  `player` varchar(30) NOT NULL DEFAULT '' COMMENT '玩家昵称',
			  `uid` bigint(20) NOT NULL DEFAULT '0' COMMENT '账号id',
			  `pid` bigint(20) NOT NULL DEFAULT '0' COMMENT '玩家角色id',
			  `uuid` varchar(50) NOT NULL DEFAULT '' COMMENT '玩家设备id',
			  `rechargemoney` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '充值总金额',
			  `fp_level` int(11) NOT NULL DEFAULT '1' COMMENT '玩家第一次充值时的等级',
			  `ac_level` int(11) NOT NULL DEFAULT '1' COMMENT '玩家当前活跃的最大等级',
			  `reg_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '玩家注册游戏的日期',
			  `fp_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '玩家第一次充值的日期',
			  `llogin_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '玩家最后一次活跃的日期',
			  `has_diamond` int(11) NOT NULL DEFAULT '0' COMMENT '玩家当前拥有的虚拟币数量',
			  `cons_diamond` int(11) NOT NULL DEFAULT '0' COMMENT '玩家消耗虚拟币总量',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	ret = cur.execute('desc whale_player')
	for i in cur.fetchall():
		if i.get('Field','')=='rechargemoney':
			Type = i.get('Type')
			if (Type =='decimal(12,2)')==False:
				m_sql = '''
					ALTER TABLE `whale_player` 
					CHANGE COLUMN `rechargemoney` `rechargemoney` DECIMAL(12,2) NOT NULL DEFAULT '0' COMMENT '充值总金额'
				'''
				cur.execute(m_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'whale_player table %s' % msg


def createdbguild_boss_record():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'guild_boss_record' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `guild_boss_record` (
			`id` int(11) NOT NULL AUTO_INCREMENT,
			`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			`guild_name`  varchar(45) NOT NULL DEFAULT '' COMMENT '冒险团的名字',
			`guild_id`    bigint      NOT NULL DEFAULT 0  COMMENT '冒险团ID',
			`boss_id`     bigint      NOT NULL DEFAULT 0  COMMENT 'bossId',
			`boss_starttime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'boss开启的时间',
			`boss_endtime`   datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'boss结束时间',
			`result`      tinyint     NOT NULL DEFAULT 0 COMMENT '0失败1成功',
			`s_uid`       varchar(45) NOT NULL           COMMENT '服务器id',
			`server_name` varchar(100) NOT NULL DEFAULT '' COMMENT '服务器名称',
			`unique_id`   varchar(60)  NOT NULL DEFAULT '' COMMENT '辨别的唯一id',
			PRIMARY KEY (`id`)
			) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `guild_boss_record` `guild_lv` '):
			add_guild_lv_sql = '''
				ALTER TABLE `guild_boss_record` 
				ADD COLUMN `guild_lv` INT(5) NULL DEFAULT 1 AFTER `unique_id`
			'''
			cur.execute(add_guild_lv_sql)

	if not cur.execute('Describe `guild_boss_record` `prosperity_lv` '):
			add_guild_lv_sql = '''
				ALTER TABLE `guild_boss_record` 
				ADD COLUMN `prosperity_lv` INT(5) NULL DEFAULT 1 AFTER `guild_lv`
			'''
			cur.execute(add_guild_lv_sql)
	if not cur.execute('Describe `guild_boss_record` `bosshp` '):
			add_guild_lv_sql = '''
				ALTER TABLE `guild_boss_record` 
				ADD COLUMN `bosshp` BIGINT NULL DEFAULT 0 AFTER `prosperity_lv`
			'''
			cur.execute(add_guild_lv_sql)
	if not cur.execute('Describe `guild_boss_record` `bosshp_max` '):
			add_guild_lv_sql = '''
				ALTER TABLE `guild_boss_record` 
				ADD COLUMN `bosshp_max` BIGINT NULL DEFAULT 0 AFTER `bosshp`
			'''
			cur.execute(add_guild_lv_sql)
	if not cur.execute('Describe `guild_boss_record` `kill_time` '):
			add_guild_lv_sql = '''
				ALTER TABLE `guild_boss_record` 
				ADD COLUMN `kill_time` INT(11) NULL DEFAULT 0 AFTER `bosshp_max`
			'''
			cur.execute(add_guild_lv_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'guild_boss_record table %s' % msg

def createdbguild_boss_reward():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'guild_boss_reward' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
				CREATE TABLE `guild_boss_reward` (
				`id` int(11) NOT NULL AUTO_INCREMENT,
				`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
				`p_id`        int(11) NOT NULL COMMENT 'boss记录表的id',
				`num`			int(10) NOT NULL COMMENT '对boss伤害的名次',
				`player_id`   bigint  NOT NULL COMMENT '玩家id',
				`total_damage` bigint NOT NULL COMMENT '玩家对boss的总伤害量',
				`bonus_grant`  tinyint NOT NULL COMMENT '奖励是否发放1成功0失败',
				`bonus_grant_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '奖励发放的时间',
				`bonus_id`     int(11) NOT NULL DEFAULT 0 COMMENT '掉落奖励id',
				PRIMARY KEY (`id`)
				) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'guild_boss_reward table %s' % msg


def createdbguild_operate_msg():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'guild_operate_msg' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `guild_operate_msg` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `create_time` int(11) NOT NULL DEFAULT '0' COMMENT '创建时间',
			  `guild_id` int(11) NOT NULL DEFAULT '0' COMMENT '冒险团id',
			  `guild_name` varchar(50) DEFAULT '' COMMENT '冒险团名字',
			  `operate` varchar(100) NOT NULL DEFAULT '' COMMENT '操作记录',
			  `level` int(11) NOT NULL DEFAULT '0' COMMENT '冒险团提升后等级',
			  `up_start_time` int(11) NOT NULL DEFAULT '0' COMMENT '开始升级时间',
			  `up_end_time` int(11) NOT NULL DEFAULT '0' COMMENT '升级完成时间',
			  `player_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '玩家id',
			  `disband_time` int(11) NOT NULL DEFAULT '0' COMMENT '开始升级时间',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建数据时的年月日',
			  `s_uid` int(11) NOT NULL DEFAULT '0' COMMENT '服务器id',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `guild_operate_msg` `operated_p_id` '):
			add_status_flag_sql = '''
					ALTER TABLE `guild_operate_msg` 
					ADD COLUMN `operated_p_id` BIGINT(20) NOT NULL DEFAULT 0 AFTER `unique_id`;
			'''
			cur.execute(add_status_flag_sql)
	if not cur.execute('Describe `guild_operate_msg` `guild_num` '):
			add_status_flag_sql = '''
					ALTER TABLE `guild_operate_msg` 
					ADD COLUMN `guild_num` BIGINT(20) NOT NULL DEFAULT 0 AFTER `operated_p_id`;
			'''
			cur.execute(add_status_flag_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'guild_operate_msg table %s' % msg


def guild_data_way():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'guild_data_way' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `guild_data_way` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `create_time` timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `name`        varchar(100) NOT NULL DEFAULT '' COMMENT '数据或者途径的名称',
			  `type_flag`   tinyint     NOT NULL DEFAULT 0  COMMENT '0代表数据来源1代表途径来源',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'guild_data_way table %s' % msg

def guild_production_cost_msg():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'guild_production_cost_msg' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `guild_production_cost_msg` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `guild_id` int(11) NOT NULL DEFAULT '0' COMMENT '冒险团id',
			  `guild_name` varchar(50) NOT NULL DEFAULT '' COMMENT '冒险团名字',
			  `r_type` varchar(50) NOT NULL DEFAULT '' COMMENT '数据类型',
			  `r_change` bigint(20) NOT NULL DEFAULT '0' COMMENT '改变的值',
			  `r_total` bigint(20) NOT NULL DEFAULT '0' COMMENT '剩余的总值',
			  `way` varchar(100) NOT NULL DEFAULT '' COMMENT '来源途径',
			  `operator` bigint(20) NOT NULL DEFAULT '0' COMMENT '这次改变的操作者',
			  `operate_time` int(11) NOT NULL DEFAULT '0' COMMENT '操作时的时间',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '操作时的年月日',
			  `s_uid` int(11) NOT NULL DEFAULT '0' COMMENT '服务器id',
			  `server_name` varchar(50) NOT NULL DEFAULT '' COMMENT '所在服务器',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `d_date` (`d_date`)
			) ENGINE=InnoDB AUTO_INCREMENT=470 DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'guild_production_cost_msg table %s' % msg
def origin_data():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'origin_data' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `origin_data` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `data`        longtext,
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'origin_data table %s' % msg

def skin_data():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'skin_data' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `skin_data` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `data`        longtext,
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	conn.commit()
	conn.close()
	cur.close()
	print 'skin_data table %s' % msg

def lequipment():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'lequipment' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `lequipment` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `uid` bigint(20) NOT NULL DEFAULT '0',
			  `pid` bigint(20) NOT NULL DEFAULT '0',
			  `type` varchar(20) NOT NULL DEFAULT '',
			  `created` blob,
			  `deleted` blob,
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `deal_flag` bit(1) NOT NULL DEFAULT b'0' COMMENT '当天记录是否被处理过',
			  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc` varchar(50) NOT NULL DEFAULT '',
			  `chsc` varchar(50) NOT NULL DEFAULT '',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`)
			) ENGINE=InnoDB AUTO_INCREMENT=4278 DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute(checkIndex('lequipment','deal_flag')):
		add_time_index = '''
		ALTER TABLE `lequipment` 
		ADD INDEX `deal_flag` ( `deal_flag` ASC)

		'''
		cur.execute(add_time_index)
	conn.commit()
	conn.close()
	cur.close()
	print 'lequipment table %s' % msg

def lgold():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'lgold' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `lgold` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `uid` bigint(20) NOT NULL DEFAULT '0',
			  `pid` bigint(20) NOT NULL DEFAULT '0',
			  `type` varchar(20) NOT NULL DEFAULT '',
			  `have` int(12) NOT NULL DEFAULT '0',
			  `diff` int(12) NOT NULL DEFAULT '0',
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `status_flag` tinyint(4) NOT NULL DEFAULT '0',
			  `unique_id` varchar(60) NOT NULL DEFAULT '',
			  `ch` varchar(45) NOT NULL DEFAULT '',
			  `chmc` varchar(45) NOT NULL DEFAULT '',
			  `chsc` varchar(45) NOT NULL DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `pid_time` (`pid`,`time`),
			  KEY `time` (`time`),
			  KEY `csdchs_uid` (`status_flag`,`d_date`,`s_uid`,`ch`,`type`,`time`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `lgold` `status_flag` '):
			add_status_flag_sql = '''
				ALTER TABLE `lgold` 
				ADD COLUMN `status_flag` VARCHAR(60) NULL DEFAULT '' AFTER `d_date`
			'''
			cur.execute(add_status_flag_sql)

	if not cur.execute('Describe `lgold` `unique_id` '):
			add_unique_id_sql = '''
				ALTER TABLE `lgold` 
				ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `status_flag`
			'''
			cur.execute(add_unique_id_sql)

	if not cur.execute('Describe `lgold` `ch` '):
			add_ch_sql = '''
				ALTER TABLE `lgold` 
				ADD COLUMN `ch` VARCHAR(45) NULL DEFAULT '' AFTER `unique_id`
			'''
			cur.execute(add_ch_sql)

	if not cur.execute('Describe `lgold` `chmc` '):
			add_chmc_sql = '''
				ALTER TABLE `lgold` 
				ADD COLUMN `chmc` VARCHAR(45) NULL DEFAULT '' AFTER `ch`
			'''
			cur.execute(add_chmc_sql)
	if not cur.execute('Describe `lgold` `chsc` '):
			add_chsc_sql = '''
				ALTER TABLE `lgold` 
				ADD COLUMN `chsc` VARCHAR(45) NULL DEFAULT '' AFTER `chmc`
			'''
			cur.execute(add_chsc_sql)
	if not cur.execute(checkIndex('lgold','pid_time')):
		add_pid_time_index = '''
		ALTER TABLE `lgold` 
		ADD INDEX `pid_time` (`pid` ASC, `time` ASC)

		'''
		cur.execute(add_pid_time_index)
	if not cur.execute(checkIndex('lgold','time')):
		add_time_index = '''
		ALTER TABLE `lgold` 
		ADD INDEX `time` ( `time` ASC)

		'''
		cur.execute(add_time_index)
	if not cur.execute(checkIndex('lgold','csdchs_uid')):
		add_csdchs_uid_index = '''
		ALTER TABLE `lgold` 
		ADD INDEX `csdchs_uid` (`status_flag` ASC,`d_date` ASC,`s_uid` ASC,`ch` ASC,`type` ASC,`time` ASC)

		'''
		cur.execute(add_csdchs_uid_index)
	conn.commit()
	conn.close()
	cur.close()
	print 'lgold table %s' % msg

def ldiamond():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'ldiamond' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `ldiamond` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `uid` bigint(20) NOT NULL DEFAULT '0',
			  `pid` bigint(20) NOT NULL DEFAULT '0',
			  `type` varchar(20) NOT NULL DEFAULT '',
			  `have` int(12) NOT NULL DEFAULT '0',
			  `diff` int(12) NOT NULL DEFAULT '0',
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `status_flag` tinyint(4) NOT NULL DEFAULT '0',
			  `unique_id` varchar(60) NOT NULL DEFAULT '',
			  `ch` varchar(45) NOT NULL DEFAULT '',
			  `chmc` varchar(45) NOT NULL DEFAULT '',
			  `chsc` varchar(45) NOT NULL DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `pid_time` (`pid`,`time`),
			  KEY `time` (`time`),
			  KEY `csdchs_uid` (`status_flag`,`d_date`,`s_uid`,`ch`,`type`,`time`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `ldiamond` `status_flag` '):
			add_status_flag_sql = '''
				ALTER TABLE `ldiamond` 
				ADD COLUMN `status_flag` VARCHAR(60) NULL DEFAULT '' AFTER `d_date`
			'''
			cur.execute(add_status_flag_sql)

	if not cur.execute('Describe `ldiamond` `unique_id` '):
			add_unique_id_sql = '''
				ALTER TABLE `ldiamond` 
				ADD COLUMN `unique_id` VARCHAR(60) NULL DEFAULT '' AFTER `status_flag`
			'''
			cur.execute(add_unique_id_sql)

	if not cur.execute('Describe `ldiamond` `ch` '):
			add_ch_sql = '''
				ALTER TABLE `ldiamond` 
				ADD COLUMN `ch` VARCHAR(45) NULL DEFAULT '' AFTER `unique_id`
			'''
			cur.execute(add_ch_sql)

	if not cur.execute('Describe `ldiamond` `chmc` '):
			add_chmc_sql = '''
				ALTER TABLE `ldiamond` 
				ADD COLUMN `chmc` VARCHAR(45) NULL DEFAULT '' AFTER `ch`
			'''
			cur.execute(add_chmc_sql)
	if not cur.execute('Describe `ldiamond` `chsc` '):
			add_chsc_sql = '''
				ALTER TABLE `ldiamond` 
				ADD COLUMN `chsc` VARCHAR(45) NULL DEFAULT '' AFTER `chmc`
			'''
			cur.execute(add_chsc_sql)
	if not cur.execute(checkIndex('ldiamond','pid_time')):
		add_pid_time_index = '''
		ALTER TABLE `ldiamond` 
		ADD INDEX `pid_time` (`pid` ASC, `time` ASC)

		'''
		cur.execute(add_pid_time_index)
	if not cur.execute(checkIndex('ldiamond','time')):
		add_time_index = '''
		ALTER TABLE `ldiamond` 
		ADD INDEX `time` ( `time` ASC)

		'''
		cur.execute(add_time_index)
	if not cur.execute(checkIndex('ldiamond','csdchs_uid')):
		add_csdchs_uid_index = '''
		ALTER TABLE `ldiamond` 
		ADD INDEX `csdchs_uid` (`status_flag` ASC,`d_date` ASC,`s_uid` ASC,`ch` ASC,`type` ASC,`time` ASC)

		'''
		cur.execute(add_csdchs_uid_index)
	conn.commit()
	conn.close()
	cur.close()
	print 'ldiamond table %s' % msg

def litems():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'litems' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
		CREATE TABLE `litems` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `uid` bigint(20) NOT NULL DEFAULT '0',
		  `pid` bigint(20) NOT NULL DEFAULT '0',
		  `type` varchar(50) NOT NULL DEFAULT '',
		  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
		  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
		  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
		  `item_desc` varchar(200) NOT NULL DEFAULT '0,0',
		  `status_flag` tinyint(4) NOT NULL DEFAULT '0' COMMENT '1代表系统获得货币，2代表消耗货币,3代表充值获得',
		  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '所属渠道',
		  `chmc` varchar(45) NOT NULL DEFAULT '',
		  `chsc` varchar(45) NOT NULL DEFAULT '',
		  `unique_id` varchar(60) DEFAULT '',
		  `goods_id` bigint(20) NOT NULL DEFAULT '0',
		  PRIMARY KEY (`id`),
		  KEY `pid_time` (`pid`,`time`),
		  KEY `time` (`time`),
		  KEY `csdchs_uid` (`status_flag`,`d_date`,`s_uid`,`ch`,`type`,`time`),
		  KEY `c_type_goods_id_time` (`goods_id`,`time`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'litems table %s' % msg
def lequipments():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'lequipments' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `lequipments` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `uid` bigint(20) NOT NULL DEFAULT '0',
			  `pid` bigint(20) NOT NULL DEFAULT '0',
			  `type` varchar(50) NOT NULL DEFAULT '',
			  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `item_desc` varchar(200) NOT NULL DEFAULT '0',
			  `status_flag` tinyint(4) NOT NULL DEFAULT '0' COMMENT '1代表系统获得货币，2代表消耗货币,3代表充值获得',
			  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '所属渠道',
			  `chmc` varchar(45) NOT NULL DEFAULT '',
			  `chsc` varchar(45) NOT NULL DEFAULT '',
			  `unique_id` varchar(60) DEFAULT '',
			  `goods_id` bigint(20) NOT NULL DEFAULT '0',
			  PRIMARY KEY (`id`),
			  KEY `pid_time` (`pid`,`time`),
			  KEY `time` (`time`),
			  KEY `csdchs_uid` (`status_flag`,`d_date`,`s_uid`,`ch`,`type`,`time`),
			  KEY `c_type_goods_id_time` (`goods_id`,`time`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'lequipments table %s' % msg
def lexplore():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'lexplore' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `lexplore` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `uid` bigint(20) NOT NULL DEFAULT '0',
			  `pid` bigint(20) NOT NULL DEFAULT '0',
			  `type` varchar(50) NOT NULL DEFAULT '',
			  `have` int(12) NOT NULL DEFAULT '0',
			  `diff` int(12) NOT NULL DEFAULT '0',
			  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `item_desc` varchar(200) DEFAULT NULL,
			  `status_flag` tinyint(4) NOT NULL DEFAULT '0' COMMENT '1代表系统获得货币，2代表消耗货币,3代表充值获得',
			  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '所属渠道',
			  `chmc` varchar(45) NOT NULL DEFAULT '',
			  `chsc` varchar(45) NOT NULL DEFAULT '',
			  `unique_id` varchar(60) DEFAULT '',
			  `goods_id` bigint(20) NOT NULL DEFAULT '0',
			  PRIMARY KEY (`id`),
			  KEY `pid_time` (`pid`,`time`,`have`),
			  KEY `time` (`time`),
			  KEY `csdchs_uid` (`status_flag`,`d_date`,`s_uid`,`ch`,`type`,`time`),
			  KEY `c_type_goods_id_time` (`goods_id`,`time`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'lexplore table %s' % msg

def lcontribute():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'lcontribute' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `lcontribute` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `uid` bigint(20) NOT NULL DEFAULT '0',
			  `pid` bigint(20) NOT NULL DEFAULT '0',
			  `type` varchar(50) NOT NULL DEFAULT '',
			  `have` int(12) NOT NULL DEFAULT '0',
			  `diff` int(12) NOT NULL DEFAULT '0',
			  `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `status_flag` tinyint(4) NOT NULL DEFAULT '0' COMMENT '1代表系统获得货币，2代表消耗货币,3代表充值获得',
			  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '所属渠道',
			  `chmc` varchar(45) NOT NULL DEFAULT '',
			  `chsc` varchar(45) NOT NULL DEFAULT '',
			  `guild_id` bigint(20) NOT NULL DEFAULT '0',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `pid_time` (`pid`,`time`,`have`),
			  KEY `diff` (`diff`),
			  KEY `time` (`time`),
			  KEY `csdchs_uid` (`status_flag`,`d_date`,`s_uid`,`ch`,`type`,`time`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'lcontribute table %s' % msg

def mg_daily_serverspaper():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'mg_daily_serverspaper' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `mg_daily_serverspaper` (
			  `id`                       int(11) NOT NULL AUTO_INCREMENT,
			  `s_uid`                    varchar(20) NOT NULL DEFAULT '' COMMENT '服务器的uid',
			  `create_time`              datetime NOT NULL DEFAULT NOW() COMMENT '创建时间',
			  `d_date`                   int(8) NOT NULL DEFAULT 0 COMMENT '年月日',
			  `new_login_accont`         int(10) NOT NULL DEFAULT 0 COMMENT '当天，新注册并登陆游戏的去重账号数',
			  `login_account`            int(10) NOT NULL DEFAULT 0 COMMENT '当天，登陆游戏并且去重的账号数',
			  `pay_account_num`          int(10) NOT NULL DEFAULT 0 COMMENT '付费账号数',
			  `income`                   decimal(10,2) NOT NULL DEFAULT 0.00 COMMENT '当天收入',
			  `first_pay_account`        int(10)  NOT NULL DEFAULT 0 COMMENT '首次付费账号数',
			  `first_pay_account_income` decimal(10,2) NOT NULL DEFAULT 0.00 COMMENT '首次付费收入',
			  `new_login_pay_num`        int(10) NOT NULL DEFAULT 0 COMMENT '新登付费数',
			  `new_login_pay_income`     decimal(10,2) DEFAULT 0.00 COMMENT '新登账号收入',
			  `once_retain`          	int(10) NOT NULL DEFAULT 0 COMMENT '次日账号登录数',
			  `three_retain`        	int(10) NOT NULL DEFAULT 0 COMMENT '3日账号登录数',
			  `four_retain`        		int(10) NOT NULL DEFAULT 0 COMMENT '4日账号登录数',
			  `five_retain`        		int(10) NOT NULL DEFAULT 0 COMMENT '5日账号登录数',
			  `six_retain`        		int(10) NOT NULL DEFAULT 0 COMMENT '6日账号登录数',
			  `seven_retain`        	int(10) NOT NULL DEFAULT 0 COMMENT '7日账号登录数',
			  PRIMARY KEY (`id`),
			  KEY `dsch` (`d_date`,`s_uid`)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	if not cur.execute('Describe `mg_daily_serverspaper` `fifteen_retain` '):
			add_ch_sql = '''
				ALTER TABLE `mg_daily_serverspaper` 
				ADD COLUMN `fifteen_retain` int(10) NULL DEFAULT 0 AFTER `seven_retain`
			'''
			cur.execute(add_ch_sql)
	if not cur.execute('Describe `mg_daily_serverspaper` `thirty_retain` '):
			add_ch_sql = '''
				ALTER TABLE `mg_daily_serverspaper` 
				ADD COLUMN `thirty_retain` int(10) NULL DEFAULT 0 AFTER `fifteen_retain`
			'''
			cur.execute(add_ch_sql)

	if not cur.execute('Describe `mg_daily_serverspaper` `forty_five_retain` '):
			add_ch_sql = '''
				ALTER TABLE `mg_daily_serverspaper` 
				ADD COLUMN `forty_five_retain` int(10) NULL DEFAULT 0 AFTER `thirty_retain`
			'''
			cur.execute(add_ch_sql)

	if not cur.execute('Describe `mg_daily_serverspaper` `sixty_retain` '):
			add_ch_sql = '''
				ALTER TABLE `mg_daily_serverspaper` 
				ADD COLUMN `sixty_retain` int(10) NULL DEFAULT 0 AFTER `forty_five_retain`
			'''
			cur.execute(add_ch_sql)


	if not cur.execute('Describe `mg_daily_serverspaper` `seventy_five_retain` '):
			add_ch_sql = '''
				ALTER TABLE `mg_daily_serverspaper` 
				ADD COLUMN `seventy_five_retain` int(10) NULL DEFAULT 0 AFTER `sixty_retain`
			'''
			cur.execute(add_ch_sql)

	if not cur.execute('Describe `mg_daily_serverspaper` `ninety_retain` '):
			add_ch_sql = '''
				ALTER TABLE `mg_daily_serverspaper` 
				ADD COLUMN `ninety_retain` int(10) NULL DEFAULT 0 AFTER `seventy_five_retain`
			'''
			cur.execute(add_ch_sql)

	if not cur.execute('Describe `mg_daily_serverspaper` `atm_num` '):
			add_ch_sql = '''
				ALTER TABLE `mg_daily_serverspaper` 
				ADD COLUMN `atm_num` int(10) NULL DEFAULT 0 AFTER `ninety_retain`
			'''
			cur.execute(add_ch_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'mg_daily_serverspaper table %s' % msg
## 皮肤表
def buy_skin():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'buy_skin' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `buy_skin` (
			  `id` INT NOT NULL AUTO_INCREMENT,
			  `uid`  bigint NOT NULL DEFAULT 0,
			  `pid`  bigint NOT NULL DEFAULT 0,
			  `type` tinyint NOT NULL DEFAULT 0,
			  `create_time` datetime NOT NULL DEFAULT NOW() COMMENT '创建时间', 
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOt NULL DEFAULT 0 COMMENT '年月日',
			  `ch`        varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc`      varchar(50) NOT NULL DEFAULT '' COMMENT '',
			  `chsc`      varchar(50) NOT NULL DEFAULT '' COMMENT '',
			  `cost`      int(10)  NOT NULL DEFAULT 0  COMMENT '购买皮肤消耗数量',
			  `skinid`    int(12) NOT NUll DEFAULT 0 COMMENT '皮肤id',
			  `buytime`   int(12) NOT NULL DEFAULT 0 COMMENT '购买皮肤时的时间戳',
			  `unique_id`   varchar(60)  NOT NULL DEFAULT '' COMMENT '辨别的唯一id',
			  PRIMARY KEY (`id`),
			  KEY `pid_time` (`pid`,`buytime`),
			  KEY `skinid` (`skinid`),
			  KEY `type` (`type`),
			)ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'buy_skin table %s' % msg

##
def room_info():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'room_info' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `room_info` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '创建的年月日',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '服务器id',
			  `time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
			  `ch` varchar(45) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc` varchar(45) NOT NULL DEFAULT '' COMMENT '主渠道号',
			  `chsc` varchar(45) NOT NULL DEFAULT '' COMMENT '子渠道号',
			  `name` varchar(45) NOT NULL DEFAULT '' COMMENT '挑战boss的名称',
			  `type` varchar(45) NOT NULL DEFAULT '' COMMENT '挑战boss类型',
			  `b_type` tinyint(4) NOT NULL DEFAULT '1' COMMENT 'boss类型',
			  `uid` bigint(20) NOT NULL COMMENT '用户账号id',
			  `pid` bigint(20) NOT NULL COMMENT '玩家id',
			  `finish_time` int(11) NOT NULL DEFAULT '0' COMMENT 'boss完成时间',
			  `s_flag` tinyint(4) NOT NULL DEFAULT '0' COMMENT '2创建房间，3解散房间',
			  `pid_time` varchar(45) NOT NULL COMMENT '玩家id和时间戳区别属于同一次进入关卡',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `date_suid_ch_name` (`d_date`,`s_uid`,`ch`,`name`)
			) ENGINE=InnoDB AUTO_INCREMENT=3457 DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'room_info table %s' % msg

## d_date  ch,s_uid,event_id, pid
def cstory_adventure():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'cstory_adventure' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
		CREATE TABLE `cstory_adventure` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `uid` bigint(20) NOT NULL DEFAULT '0',
		  `pid` bigint(20) NOT NULL DEFAULT '0',
		  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
		  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
		  `deal_flag` bit(1) NOT NULL DEFAULT b'0' COMMENT '当天记录是否被处理过',
		  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
		  `chmc` varchar(50) NOT NULL DEFAULT '',
		  `chsc` varchar(50) NOT NULL DEFAULT '',
		  `time` int(11) NOT NULL DEFAULT 0 COMMENT '时间发送时间',
		  `event_id` int(11) NOT NULL DEFAULT 0 COMMENT '故事关卡id',
		  `finish_time` int(6) NOT NULL DEFAULT 0 COMMENT '通关所用的时间',
		  `medal`  tinyint NOT NULL DEFAULT 0 COMMENT '通关时获得的皇冠数量',
		  `name`  varchar(50) NOT NULL DEFAULT '' COMMENT '关卡的名字',
		  `onece` tinyint NOT NULL DEFAULT 0 COMMENT '是否为首通的标识',
		  `player_lv` tinyint NOT NULL DEFAULT 1 COMMENT '通关时玩家的等级',
		  `unique_id` varchar(60) DEFAULT '',
		  PRIMARY KEY (`id`),
		  KEY `date_ch_suid_event_id_pid` (`d_date`,`ch`,`s_uid`,`event_id`,`pid`)
		) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'cstory_adventure table %s' % msg

##
def cstory_adventure_clean_up():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'cstory_adventure_clean_up' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
		CREATE TABLE `cstory_adventure_clean_up` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `uid` bigint(20) NOT NULL DEFAULT '0',
		  `pid` bigint(20) NOT NULL DEFAULT '0',
		  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
		  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
		  `deal_flag` bit(1) NOT NULL DEFAULT b'0' COMMENT '当天记录是否被处理过',
		  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
		  `chmc` varchar(50) NOT NULL DEFAULT '',
		  `chsc` varchar(50) NOT NULL DEFAULT '',
		  `time` int(11) NOT NULL DEFAULT 0 COMMENT '时间发送时间',
		  `event_id` int(11) NOT NULL DEFAULT 0 COMMENT '故事关卡id',
		  `player_lv` tinyint NOT NULL DEFAULT 1 COMMENT '通关时玩家的等级',
		  `sweepnum` tinyint NOT NULL DEFAULT 1 COMMENT '扫荡次数',
		  `unique_id` varchar(60) DEFAULT '',
		  PRIMARY KEY (`id`),
		  KEY `date_ch_suid_event_id` (`d_date`,`ch`,`s_uid`,`event_id`)
		) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'cstory_adventure_clean_up table %s' % msg

def cstory_adventure_medal():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'cstory_adventure_medal' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `cstory_adventure_medal` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
			  `create_time` datetime NOT NULL DEFAULT NOW() COMMENT '数据生成时间',
			  `event_id` int(11) NOT NULL DEFAULT 0 COMMENT '故事关卡id',
			  `one_medal`  tinyint NOT NULL DEFAULT 0 COMMENT '1个皇冠获取的人数',
			  `two_medal`  tinyint NOT NULL DEFAULT 0 COMMENT '2个皇冠获取的人数',
			  `three_medal`  tinyint NOT NULL DEFAULT 0 COMMENT '3个皇冠获取的人数',
			  `name`  varchar(50) NOT NULL DEFAULT '' COMMENT '关卡的名字',
			  `onece` tinyint NOT NULL DEFAULT 0 COMMENT '是否为首通的标识',
			  `sweepnum` int NOT NULL DEFAULT 0 COMMENT '扫荡次数',
			  PRIMARY KEY (`id`),
			  KEY `date_onece` (`d_date`,`onece`)
			) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'
	if not cur.execute('Describe `cstory_adventure_medal` `zero_medal` '):
			add_ch_sql = '''
				ALTER TABLE `cstory_adventure_medal` 
				ADD COLUMN `zero_medal` tinyint NULL DEFAULT 0 AFTER `sweepnum`
			'''
			cur.execute(add_ch_sql)
	conn.commit()
	conn.close()
	cur.close()
	print 'cstory_adventure_medal table %s' % msg

def cstory_adventure_playerlv():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'cstory_adventure_playerlv' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `cstory_adventure_playerlv` (
			  `id` int(11) NOT NULL AUTO_INCREMENT,
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOT NULL DEFAULT '0' COMMENT '年月日',
			  `ch` varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
			  `create_time` datetime NOT NULL DEFAULT NOW() COMMENT '数据生成时间',
			  `event_id` int(11) NOT NULL DEFAULT 0 COMMENT '故事关卡id',
			  `name`  varchar(50) NOT NULL DEFAULT '' COMMENT '关卡的名字',
			  `onece` tinyint NOT NULL DEFAULT 0 COMMENT '是否为首通的标识',
			  `player_lvs` text  COMMENT '所有等级的数据',
			  PRIMARY KEY (`id`),
			  KEY `date` (`d_date`)
			) ENGINE=InnoDB  DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'cstory_adventure_playerlv table %s' % msg


def player_get():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'player_get' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `player_get` (
			  `id` INT NOT NULL AUTO_INCREMENT,
			  `uid`  bigint NOT NULL DEFAULT 0,
			  `pid`  bigint NOT NULL DEFAULT 0,
			  `type` tinyint NOT NULL DEFAULT 0 COMMENT '1:等级 2:整卡 3:皮肤整卡', 
			  `time` datetime NOT NULL DEFAULT now() COMMENT '创建时间', 
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `d_date` int(8) NOt NULL DEFAULT 0 COMMENT '年月日',
			  `ch`        varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
			  `chmc`      varchar(50) NOT NULL DEFAULT '' COMMENT '',
			  `chsc`      varchar(50) NOT NULL DEFAULT '' COMMENT '',
			  `Path`      varchar(50) NOT NULL DEFAULT '' COMMENT '产出的途径',
			  `os`        tinyint NOT NULL DEFAULT 0 COMMENT '平台区分',
			  `new`       int(10) NOT NULL DEFAULT 0 COMMENT '',
			  `old`       int(10) NOT NULL DEFAULT 0 COMMENT '',
			  `unique_id` varchar(60) DEFAULT '',
			  PRIMARY KEY (`id`),
			  KEY `type_pid_time` (`type`,`pid`,`time`),
			  KEY `type_time` (`type`,`time`,`s_uid`)
			)ENGINE=InnoDB DEFAULT CHARSET=utf8
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'player_get table %s' % msg

def superstar():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'superstar' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
		CREATE TABLE `superstar` (
		  `id` INT NOT NULL AUTO_INCREMENT,
		  `uid`  bigint NOT NULL DEFAULT 0,
		  `pid`  bigint NOT NULL DEFAULT 0,
		  `optype` tinyint NOT NULL DEFAULT 0 COMMENT '1:激活 2:重置 3:确认修改', 
		  `time` int(12) NOT NULL DEFAULT 0 COMMENT '创建时间', 
		  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
		  `d_date` int(8) NOt NULL DEFAULT 0 COMMENT '年月日',
		  `ch`        varchar(50) NOT NULL DEFAULT '' COMMENT '渠道',
		  `chmc`      varchar(50) NOT NULL DEFAULT '' COMMENT '',
		  `chsc`      varchar(50) NOT NULL DEFAULT '' COMMENT '',
		  `os`        tinyint NOT NULL DEFAULT 0 COMMENT '平台区分',
		  `nodeid`       int(10) NOT NULL DEFAULT 0 COMMENT '',
		  `pname`     varchar(50) NOT NULL DEFAULT '' COMMENT '玩家名字',
		  `fid`       bigint NOT NULL DEFAULT 0  COMMENT '伙伴id',
		  `fname`     varchar(50) NOT NULL DEFAULT '' COMMENT '伙伴名字',
		  `basediamond` int(10) NOT NULL DEFAULT 0 COMMENT '基础钻石消耗',
		  `attr`        text  COMMENT '',
		  `deal_flag`   bit(1) NOT NULL DEFAULT 0 COMMENT '是否被处理过的标记',
		  PRIMARY KEY (`id`),
		  KEY `deal_flag` (`deal_flag`)
		)ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'superstar table %s' % msg

def superstars():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'superstars' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
		CREATE TABLE `superstars` (
		  `id` INT NOT NULL AUTO_INCREMENT,
		  `uid`  bigint NOT NULL DEFAULT 0,
		  `pid`  bigint NOT NULL DEFAULT 0,
		  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
		  `nodeid`    int(10) NOT NULL DEFAULT 0 COMMENT '节点id',
		  `fid`       bigint NOT NULL DEFAULT 0  COMMENT '伙伴id',
		  `fname`     varchar(50) NOT NULL DEFAULT '' COMMENT '伙伴名字',
		  `nodeseat`  tinyint  NOT NULL DEFAULT 1 COMMENT '节点槽的位置',
		  `quality`   tinyint  NOT NULL DEFAULT 1 COMMENT  '品质类型:1=>白,2=>绿,3=>蓝,4=>紫,5=>红,6=>金',
		  `attr_type` tinyint  NOT NULL DEFAULT 1 COMMENT '节点属性类型',
		  `d_date`    int(6)   NOT NULL DEFAULT  0 COMMENT '年月日',
		  `year`      int(6)   NOT NULL DEFAULT  0 COMMENT '年',
		  `month`     tinyint  NOT NULL DEFAULT  0 COMMENT '月',
		  `week`     int(6)    NOT NULL DEFAULT  0 COMMENT '周',
		  PRIMARY KEY (`id`),
		  KEY `s_uid_fid_nodeid_pid` (`s_uid`,`fid`,`nodeid`,`pid`),
  		  KEY `s_uid_d_date` (`s_uid`,`d_date`),
		  KEY `year_month` (`year`,`month`),
		  KEY `year_week` (`year`,`week`)
		)ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'


	conn.commit()
	conn.close()
	cur.close()
	print 'superstars table %s' % msg

def superstartoplayer():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'superstartoplayer' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
			CREATE TABLE `superstartoplayer` (
			  `id` INT NOT NULL AUTO_INCREMENT,
			  `uid`  bigint NOT NULL DEFAULT 0,
			  `pid`  bigint NOT NULL DEFAULT 0,
			  `pname` varchar(50) NOT NULL DEFAULT '' COMMENT '玩家的名称',
			  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
			  `ak_num` int(12) NOT NULL DEFAULT 0 COMMENT '觉醒点总数',
			  `ak_attr_num` int(12) NOT NULL DEFAULT 0 COMMENT '觉醒属性个数',
			  `king_num`   int(12) NOT NULL DEFAULT 0 COMMENT '金品质属性个数',
			  `reset_num`  int(12) NOT NULL DEFAULT 0 COMMENT '重置次数',
			  `ldiamond`   bigint  NOT NULL DEFAULT 0 COMMENT '累计消耗的钻石数',
			  `basediamond`   bigint  NOT NULL DEFAULT 0 COMMENT '累计消耗的钻石数',
			  `oneattr_reset`  int(12) NOT NULL DEFAULT 0 COMMENT '单锁定的次数',
			  `oneuserld`      bigint NOT NULL  DEFAULT 0 COMMENT '单词锁定消耗的钻石数',
			  `twoattr_reste`  int(12) NOT NULL DEFAULT 0 COMMENT '双锁定的次数',
			  `twouser_ld`     bigint  NOT NULL DEFAULT 0 COMMENT '双锁定消耗的钻石数',
			  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
			  PRIMARY KEY (`id`),
			  KEY `s_uid_pid_uid` (`s_uid`,`pid`,`uid`)

			)ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'superstartoplayer table %s' % msg


def superstardatagram():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'superstardatagram' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
		CREATE TABLE `superstardatagram` (
		  `id` INT NOT NULL AUTO_INCREMENT,
		  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
		  `st_p_num` int(12) NOT NULL DEFAULT 0 COMMENT '开启超觉醒的玩家数',
		  `ak_num` int(12) NOT NULL DEFAULT 0 COMMENT '觉醒点总数',
		  `reset_num`  int(12) NOT NULL DEFAULT 0 COMMENT '重置次数',
		  `ldiamond`   bigint  NOT NULL DEFAULT 0 COMMENT '累计消耗的钻石数',
		  `oneattr_reset`  int(12) NOT NULL DEFAULT 0 COMMENT '单锁定的次数',
		  `oneuserld`      bigint NOT NULL  DEFAULT 0 COMMENT '单词锁定消耗的钻石数',
		  `twoattr_reste`  int(12) NOT NULL DEFAULT 0 COMMENT '双锁定的次数',
		  `twouser_ld`     bigint  NOT NULL DEFAULT 0 COMMENT '双锁定消耗的钻石数',
		  `d_date`         int(8)  NOT NULL DEFAULT 0 COMMENT '年月日',
		  `year`           int(5)  NOT NULL DEFAULT 0 COMMENT '年',
		  `month`          tinyint NOT NULL DEFAULT 0 COMMENT '月',
		  `week`           int(5)  NOT NULL DEFAULT 0 COMMENT '一年中的第几周',
		  PRIMARY KEY (`id`),
		  KEY `s_uid_d_date` (`s_uid`,`d_date`),
		  KEY `year_month` (`year`,`month`),
		  KEY `year_week` (`year`,`week`)
		)ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'superstardatagram table %s' % msg

def superstarym():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'superstarym' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
		CREATE TABLE `superstarym` (
		  `id` INT NOT NULL AUTO_INCREMENT,
		  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
		  `st_p_num` int(12) NOT NULL DEFAULT 0 COMMENT '开启超觉醒的玩家数',
		  `ak_num` int(12) NOT NULL DEFAULT 0 COMMENT '觉醒点总数',
		  `reset_num`  int(12) NOT NULL DEFAULT 0 COMMENT '重置次数',
		  `ldiamond`   bigint  NOT NULL DEFAULT 0 COMMENT '累计消耗的钻石数',
		  `oneattr_reset`  int(12) NOT NULL DEFAULT 0 COMMENT '单锁定的次数',
		  `oneuserld`      bigint NOT NULL  DEFAULT 0 COMMENT '单词锁定消耗的钻石数',
		  `twoattr_reste`  int(12) NOT NULL DEFAULT 0 COMMENT '双锁定的次数',
		  `twouser_ld`     bigint  NOT NULL DEFAULT 0 COMMENT '双锁定消耗的钻石数',
		  `year`           int(5)  NOT NULL DEFAULT 0 COMMENT '年',
		  `month`          int(5) NOT NULL DEFAULT 0 COMMENT '月',
		  PRIMARY KEY (`id`),
		  KEY `s_uid_year_month` (`s_uid`,`year`,`month`)
		)ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'superstarym table %s' % msg

def superstarweek():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'superstarweek' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
		CREATE TABLE `superstarweek` (
		  `id` INT NOT NULL AUTO_INCREMENT,
		  `s_uid` varchar(45) NOT NULL DEFAULT '' COMMENT '所属服务器uid',
		  `st_p_num` int(12) NOT NULL DEFAULT 0 COMMENT '开启超觉醒的玩家数',
		  `ak_num` int(12) NOT NULL DEFAULT 0 COMMENT '觉醒点总数',
		  `reset_num`  int(12) NOT NULL DEFAULT 0 COMMENT '重置次数',
		  `ldiamond`   bigint  NOT NULL DEFAULT 0 COMMENT '累计消耗的钻石数',
		  `oneattr_reset`  int(12) NOT NULL DEFAULT 0 COMMENT '单锁定的次数',
		  `oneuserld`      bigint NOT NULL  DEFAULT 0 COMMENT '单词锁定消耗的钻石数',
		  `twoattr_reste`  int(12) NOT NULL DEFAULT 0 COMMENT '双锁定的次数',
		  `twouser_ld`     bigint  NOT NULL DEFAULT 0 COMMENT '双锁定消耗的钻石数',
		  `year`           int(5)  NOT NULL DEFAULT 0 COMMENT '年',
		  `week`           int(8)  NOT NULL DEFAULT 0 COMMENT '一年中的第几周',
		  `week_date`      varchar(20) NOT NULL DEFAULT '' COMMENT '周的起始和结束', 
		  PRIMARY KEY (`id`),
		  KEY `s_uid_year_week` (`s_uid`,`year`,`week`)
		)ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'superstarweek table %s' % msg

def superstardetail():
	conndb = connDB()
	conn,cur=conndb.mysqlcur()
	sql = '''SHOW TABLES LIKE 'superstardetail' '''
	ret =cur.execute(sql)
	msg = '已存在'
	if ret==0:
		## 表不存在创建表
		create_sql = '''
		CREATE TABLE `superstardetail` (
		  `id` INT NOT NULL AUTO_INCREMENT,
		  `uid`  bigint NOT NULL DEFAULT 0,
		  `pid`  bigint NOT NULL DEFAULT 0,
		  `fid`  bigint NOT NULL DEFAULT 0  COMMENT '伙伴id',
		  `fname` varchar(50) NOT NULL DEFAULT '' COMMENT '伙伴名字',
		  `pname` varchar(50) NOT NULL DEFAULT '' COMMENT '玩家的名称',
		  `s_uid` int(10) NOT NULL DEFAULT 0 COMMENT '所属服务器uid',
		  `op_type` tinyint NOT NULL DEFAULT 0 COMMENT '觉醒点的操作类型',
		  `nodeid`       int(10) NOT NULL DEFAULT 0 COMMENT '觉醒点的id',
		  `basediamond`   int(6)  NOT NULL DEFAULT 0 COMMENT '基础钻石消耗',
		  `lock_type`  tinyint NOT NULL DEFAULT 0 COMMENT '0未锁定1锁定一个2锁定两个',
		  `lock_user_diamond`  int(12) NOT NULL DEFAULT 0 COMMENT '锁定时额外消耗的钻石',
		  `quality`   tinyint  NOT NULL DEFAULT 1 COMMENT  '品质类型:1=>白,2=>绿,3=>蓝,4=>紫,5=>红,6=>金',
		  `attr_type` tinyint  NOT NULL DEFAULT 1 COMMENT '节点属性类型',
		  `time` int(12) NOT NULL DEFAULT 0 COMMENT '创建时间',
		  `nodeseat`  tinyint  NOT NULL DEFAULT 1 COMMENT '槽位点',
		  PRIMARY KEY (`id`),
		  KEY `s_uid_op_end_time_pid_fid` (`s_uid`,`op_type`,`time`,`pid`,`fid`),
		  KEY `end_time` (`time`)
		)ENGINE=InnoDB DEFAULT CHARSET=utf8;
		'''
		cur.execute(create_sql)
		msg ='已创建完成'

	conn.commit()
	conn.close()
	cur.close()
	print 'superstardetail table %s' % msg

def checkIndex(table_name,index_name):
	sql = '''
	show index from `%s` where Key_name =  '%s'
 
	''' % (table_name,index_name)
	return sql

# if __name__ == '__main__':
# 	## ---------
# 	## 2017-12-25
# 	##-----------
	# action_player()
	# createdbguild_boss_record()
	# createdbguild_boss_reward()
	# createdbguild_operate_msg()
	# guild_data_way()
	# guild_production_cost_msg()
	# origin_data()
	# lequipment()
	# player_login()
	# test()
def startmodifydb():
	OD_distributoin()
	action_player()
	ad_datas()
	ad_group()
	ad_level()
	atm()
	channel_server()
	channels()
	click_pay()
	connDB()
	cons_mode()
	createdbguild_boss_record()
	createdbguild_boss_reward()
	createdbguild_operate_msg()
	currency_cond()
	daily_levelbase()
	daily_levelmap()
	daily_online()
	device_power_ster()
	equipment_retain()
	fetch_server()
	guild_data_way()
	guild_production_cost_msg()
	guild_summary()
	hours_statistics()
	lequipment()
	level_dis()
	litem()
	ltv_value()
	magic_stone()
	mg_daily_chs()
	mg_daily_newspaper()
	mg_menus()
	mg_menus_access()
	mg_roles()
	mg_user_role()
	num_of_phy_pur()
	origin_data()
	output_drain()
	overview()
	partner_device()
	pay()
	pay_points()
	physical_buy()
	player_create()
	player_login()
	player_logout()
	player_own()
	player_regist()
	player_retain()
	relic_hero()
	role_retain()
	servers_data()
	spirit_level()
	task_checkpoint()
	user_secontrol()
	users()
	vip_level()
	whale_player()
	lgold()
	ldiamond()
	litems()
	lequipments()
	lexplore()
	lcontribute()
	mg_daily_serverspaper()
	buy_skin()
	skin_data()
	room_info()
	cstory_adventure()
	cstory_adventure_clean_up()
	cstory_adventure_medal()
	cstory_adventure_playerlv()
	player_get()
	superstar()
	superstars()
	# superstartoplayer()
	superstardatagram()
	superstarym()
	superstarweek()
	superstardetail()