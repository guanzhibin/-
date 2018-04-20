#!/usr/bin/env  
#coding:utf8
import pymysql
from DBUtils.PooledDB import PooledDB  
class mysqlDB(object):
    def __init__(self,m_host,m_port,m_user,m_pwd,m_db):                              #连接数据库  
        self.m_host = m_host
        self.m_port = m_port
        self.m_user = m_user
        self.m_pwd = m_pwd
        self.m_db  = m_db
        db_message = dict(host=self.m_host,user=self.m_user,passwd=self.m_pwd,db=self.m_db,port=self.m_port,charset="utf8")
        args = (15,15,30,100,True,0,None)
        self.pool = PooledDB(pymysql,*args,**db_message) 
    def base_connect(self):
        # conn=pymysql.connect(host=self.m_host,user=self.m_user,port=self.m_port,passwd=self.m_pwd,db=self.m_db,charset = 'utf8')
        conn = self.pool.connection()
        cur=conn.cursor(cursor=pymysql.cursors.DictCursor)
        return conn,cur
    def mselect(self,sql,value):
        conn,cur = self.base_connect()
        cur.execute(sql,value)
        conn.commit()
        conn.close()
        cur.close()
        return cur.fetchall() ##处理对应字段
    def mupdate(self,sql,value):
        conn,cur = self.base_connect()
        data = cur.execute(sql,value)
        conn.commit()
        conn.close()
        cur.close()
        return data
    def madd(self,sql,value):
        conn,cur = self.base_connect()
        last_id = 0
        try:
            ret = cur.execute(sql % value)
            if ret==1:
                last_id = int(cur.lastrowid)
        except Exception as e:
            raise
        conn.commit()
        conn.close()
        cur.close()
        ## 成功返回1
        return last_id
    def malladd(self,sql,values):
        conn,cur = self.base_connect()
        try:
            cur.executemany(sql,values)  
        except Exception as err:  
            print(err,'插入出错')  
        finally:  
            conn.commit()
            cur.close()  
            conn.close()
