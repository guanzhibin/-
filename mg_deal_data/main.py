#!/usr/bin/env python
#coding: utf-8
import gevent
import gevent.monkey
gevent.monkey.patch_all()
from logger import set_log
import mg_config
from deal_datas.deal_data import *
from deal_datas.analyze_data import *
from deal_datas.deal_redis import *
from deal_datas.clickpay_physicalbuy import *
if __name__ == '__main__':
    from modifydb import startmodifydb
    startmodifydb()
    try:
        config = mg_config.config
        args = mg_config.args
        greenlet_list = []
        
        deal_redis_l = []
        deal_redis_l.append(dealServersofRedis())
        deal_redis_l.append(dealRegisterOfRedis())
        deal_redis_l.append(dealLoginOfREdis())
        deal_redis_l.append(dealLogoutOfREdis())
        deal_redis_l.append(dealPlayerPayOfREdis())
        deal_redis_l.append(dealPlayerTopupsOfREdis())
        deal_redis_l.append(dealDONLOfREdis())
        deal_redis_l.append(dealDLEVELOfREdis())
        deal_redis_l.append(dealLgoldOfREdis())
        deal_redis_l.append(dealLdiamondfREdis())
        deal_redis_l.append(dealLitemfREdis())
        deal_redis_l.append(dealCreateOfRedis())
        deal_redis_l.append(click_pay_tomysql())
        deal_redis_l.append(physical_buy_tomysql())
        deal_redis_l.append(deal_fetch_server())
        deal_redis_l.append(currency_gold_diamond())
        deal_redis_l.append(relic_hero())
        deal_redis_l.append(Spirit())
        deal_redis_l.append(adgroup())
        deal_redis_l.append(guildbossrecord())
        deal_redis_l.append(GuildPperateMsg())
        deal_redis_l.append(GuildOutputCon())
        deal_redis_l.append(annex_information())
        deal_redis_l.append(deallequipmentfREdis())
        deal_redis_l.append(dealexploreREdis())
        deal_redis_l.append(dealelcontribute())
        deal_redis_l.append(deal_lequipment())
        deal_redis_l.append(buy_skin())
        deal_redis_l.append(cstory_adventure_clean_up())
        deal_redis_l.append(dealPlayerGetREdis())
        deal_redis_l.append(superstar())
        for deal_redis in deal_redis_l:
            greenlet_list.append(gevent.spawn(deal_redis.starting))

        if not args.no_pay_distribution:
            pay_distribution = pay_distribution()
            greenlet_list.append(gevent.spawn(pay_distribution.starting_deal))

        ##  处理服务器数据
        # if args.no_deal_servers is False:
        #     deal_servers = deal_servers()
        #     greenlet_list.append(gevent.spawn(deal_servers.starting_deal))

        ##  注册
        # if args.no_deal_regist is False:
        #     deal_regist = deal_regist()
        #     greenlet_list.append(gevent.spawn(deal_regist.starting))

        # ## 登录数据
        # if args.no_deal_login is False:
        #     deal_login = deal_login()
        #     greenlet_list.append(gevent.spawn(deal_login.starting))
        ##   ----------
        deal_retains= deal_retains()
        greenlet_list.append(gevent.spawn(deal_retains.starting))
        deal_ltv = deal_ltv()
        greenlet_list.append(gevent.spawn(deal_ltv.starting))
        physical_buy = physical_buy()
        greenlet_list.append(gevent.spawn(physical_buy.starting_deal))
        click_pay = click_pay()
        greenlet_list.append(gevent.spawn(click_pay.starting_deal))
        deal_litem = deal_litem() 
        greenlet_list.append(gevent.spawn(deal_litem.starting))
        # 活跃用户
        deal_ac_player = deal_ac_player()
        greenlet_list.append(gevent.spawn(deal_ac_player.starting))
        ## 分渠道
        deal_equipment = deal_equipment()
        greenlet_list.append(gevent.spawn(deal_equipment.starting))

        ## 每小时实时数据
        hours_statistics = hours_statistics()
        greenlet_list.append(gevent.spawn(hours_statistics.starting))

        ## 货币进毁 存
        currencyGDS = currencyGDS()
        greenlet_list.append(gevent.spawn(currencyGDS.starting))

        # 产出消耗分布
        ODDistributoin = ODDistributoin()
        greenlet_list.append(gevent.spawn(ODDistributoin.starting))

        deal_task_check_point = deal_task_check_point()
        greenlet_list.append(gevent.spawn(deal_task_check_point.starting))

        spirit_level = spirit_level()
        greenlet_list.append(gevent.spawn(spirit_level.starting))


        ad_groups = ad_groups()
        greenlet_list.append(gevent.spawn(ad_groups.starting))

        cstory_adventure_medal = cstory_adventure_medal()
        greenlet_list.append(gevent.spawn(cstory_adventure_medal.starting))

        
        superstars = superstars()
        greenlet_list.append(gevent.spawn(superstars.starting))
        ## -------
        gevent.joinall(greenlet_list)
    except Exception as e:
        mg_config.mg_log.error(str(e))
