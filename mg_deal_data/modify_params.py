#conding:utf-8
mo_params =['''
DROP TABLE IF EXISTS `guild_boss_record`;''','''
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
  PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;''']